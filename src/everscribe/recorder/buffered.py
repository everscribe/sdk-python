"""Buffered recorder: wraps another recorder with asynchronous, batched
writes on a background daemon thread.

The Python analog of the Go SDK's channel + ``select`` loop. Events are held
in an in-memory buffer and flushed to the inner recorder when the pending
batch reaches ``flush_size`` or ``flush_interval`` elapses, whichever comes
first. ``record`` never blocks on the network (it only enqueues), so it is
safe to call from async handlers - the flush I/O happens off the event loop
on the worker thread.
"""

from __future__ import annotations

import copy
import logging
import threading
import time
from collections import deque
from dataclasses import dataclass
from enum import Enum
from queue import Empty, Queue
from typing import Callable, Deque, List, Optional

from ..event import Event, prepare_event
from ..event.types import Logger
from .base import Recorder
from .errors import BufferFullError, DrainTimeoutError

DEFAULT_BUFFER_SIZE = 1000
DEFAULT_FLUSH_SIZE = 100
DEFAULT_FLUSH_INTERVAL = 5.0
DEFAULT_FLUSH_TIMEOUT = 30.0
DEFAULT_DRAIN_TIMEOUT = 30.0


class OverflowPolicy(str, Enum):
    """Controls :meth:`BufferedRecorder.record` when the buffer is full."""

    #: Discard the incoming event and increment the dropped counter (default).
    #: Keeps the request path fast at the cost of losing events under
    #: sustained pressure. Warns on the first drop and every 1000th after.
    DROP_NEWEST = "drop-newest"
    #: Block the caller until space is available (or the recorder closes).
    #: Preserves every event but adds unbounded latency.
    BLOCK = "block"
    #: Raise :class:`BufferFullError` immediately.
    ERROR = "error"


@dataclass
class BufferedStats:
    """Runtime counters for observability."""

    dropped: int  # total events dropped due to overflow
    flushed: int  # total events successfully flushed to the inner recorder
    flush_errs: int  # total flush calls that failed
    pending: int  # events currently in the buffer
    buffer_size: int  # buffer capacity


@dataclass
class _FlushRequest:
    ack: "Queue[Optional[BaseException]]"


class BufferedRecorder:
    """Wraps ``inner`` with asynchronous batched writes. Spawns a background
    daemon thread that runs until :meth:`close`. Safe for concurrent
    :meth:`record` calls. Implements :class:`Recorder`."""

    def __init__(
        self,
        inner: Recorder,
        *,
        buffer_size: int = DEFAULT_BUFFER_SIZE,
        flush_size: int = DEFAULT_FLUSH_SIZE,
        flush_interval: float = DEFAULT_FLUSH_INTERVAL,
        flush_timeout: float = DEFAULT_FLUSH_TIMEOUT,
        overflow: OverflowPolicy = OverflowPolicy.DROP_NEWEST,
        drain_timeout: float = DEFAULT_DRAIN_TIMEOUT,
        logger: "Logger | None" = None,
    ) -> None:
        self._inner = inner
        self._buffer_size = buffer_size
        self._flush_size = flush_size
        self._flush_interval = flush_interval
        self._flush_timeout = flush_timeout
        self._overflow = OverflowPolicy(overflow)
        self._drain_timeout = drain_timeout
        self._logger: Logger = logger or logging.getLogger("everscribe.recorder")

        self._cond = threading.Condition()
        self._buf: Deque[Event] = deque()
        self._flush_requests: List[_FlushRequest] = []
        self._closed = False
        self._dropped = 0
        self._flushed = 0
        self._flush_errs = 0

        # Detect the optional batch capability once.
        rb = getattr(inner, "record_batch", None)
        self._record_batch: Optional[Callable[..., None]] = rb if callable(rb) else None

        self._next_flush = time.monotonic() + flush_interval
        self._thread = threading.Thread(
            target=self._run, name="everscribe-recorder", daemon=True
        )
        self._thread.start()

    # -- public API --------------------------------------------------------

    def record(self, e: Event) -> None:
        """Enqueue an event for asynchronous delivery. Empty-``action`` events
        are no-ops. Behavior when the buffer is full follows the configured
        :class:`OverflowPolicy`. Calls after :meth:`close` are silently
        dropped.

        ``prepare_event`` runs here, on the calling thread, so the in-scope
        status capture (and request context) is applied before the event is
        handed to the worker thread."""
        if e is None or e.action == "":
            return
        with self._cond:
            if self._closed:
                return
            prepare_event(e)
            if len(self._buf) >= self._buffer_size:
                if self._overflow is OverflowPolicy.ERROR:
                    raise BufferFullError()
                if self._overflow is OverflowPolicy.BLOCK:
                    while len(self._buf) >= self._buffer_size and not self._closed:
                        self._cond.wait()
                    if self._closed:
                        return
                else:  # DROP_NEWEST
                    self._dropped += 1
                    n = self._dropped
                    if n == 1 or n % 1000 == 0:
                        self._logger.warning(
                            "everscribe: recorder buffer full, event dropped "
                            "(action=%s dropped_total=%d buffer_size=%d)",
                            e.action,
                            n,
                            self._buffer_size,
                        )
                    return
            self._buf.append(copy.copy(e))
            # notify_all (not notify): under BLOCK policy the worker and one or
            # more blocked producers wait on the same condition, and only the
            # worker must act on new events / flush requests.
            self._cond.notify_all()

    def flush(self, timeout: "float | None" = None) -> None:
        """Force an immediate flush of all events buffered at call time and
        block until they are persisted. Raises the inner recorder's error if
        the flush failed, or ``TimeoutError`` if ``timeout`` elapses first.
        A no-op after :meth:`close`."""
        ack: "Queue[Optional[BaseException]]" = Queue(maxsize=1)
        with self._cond:
            if self._closed:
                return
            self._flush_requests.append(_FlushRequest(ack=ack))
            self._cond.notify_all()
        try:
            err = ack.get(timeout=timeout)
        except Empty as exc:
            raise TimeoutError("recorder: flush timed out") from exc
        if err is not None:
            raise err

    def close(self) -> None:
        """Drain pending events and stop the worker thread. Idempotent. Raises
        :class:`DrainTimeoutError` if the drain exceeds ``drain_timeout`` with
        events still pending."""
        with self._cond:
            if self._closed:
                return
            self._closed = True
            self._cond.notify_all()
        self._thread.join(timeout=self._drain_timeout)
        if self._thread.is_alive():
            raise DrainTimeoutError()

    def stats(self) -> BufferedStats:
        """Return cumulative counters for observability."""
        with self._cond:
            return BufferedStats(
                dropped=self._dropped,
                flushed=self._flushed,
                flush_errs=self._flush_errs,
                pending=len(self._buf),
                buffer_size=self._buffer_size,
            )

    # -- worker ------------------------------------------------------------

    def _run(self) -> None:
        while True:
            with self._cond:
                while True:
                    if self._closed or self._flush_requests:
                        break
                    if len(self._buf) >= self._flush_size:
                        break
                    remaining = self._next_flush - time.monotonic()
                    if remaining <= 0:
                        break
                    self._cond.wait(timeout=remaining)
                batch = list(self._buf)
                self._buf.clear()
                requests = self._flush_requests
                self._flush_requests = []
                closed = self._closed
                self._next_flush = time.monotonic() + self._flush_interval
                # Wake any producers blocked on a full buffer (BLOCK policy).
                self._cond.notify_all()

            # Flush outside the lock so record() stays responsive during I/O.
            err = self._flush_batch(batch)
            for req in requests:
                req.ack.put(err)
            if closed:
                return

    def _flush_batch(self, batch: "List[Event]") -> "Optional[BaseException]":
        n = len(batch)
        if n == 0:
            return None
        err: Optional[BaseException] = None
        try:
            if self._record_batch is not None:
                self._record_batch(batch, timeout=self._flush_timeout)
            else:
                # Serial fallback for inner recorders without record_batch;
                # the Recorder protocol has no timeout param, so we don't pass
                # one here (HTTPRecorder always has record_batch above).
                for e in batch:
                    try:
                        self._inner.record(e)
                    except Exception as ex:  # one failure must not abort the batch
                        err = ex
        except Exception as ex:
            err = ex
        if err is not None:
            with self._cond:
                self._flush_errs += 1
                total = self._flush_errs
            self._logger.error(
                "everscribe: recorder flush failed (error=%s batch_size=%d "
                "flush_errs_total=%d)",
                err,
                n,
                total,
            )
            return err
        with self._cond:
            self._flushed += n
        return None


__all__ = [
    "BufferedRecorder",
    "BufferedStats",
    "OverflowPolicy",
    "DEFAULT_BUFFER_SIZE",
    "DEFAULT_FLUSH_SIZE",
    "DEFAULT_FLUSH_INTERVAL",
    "DEFAULT_FLUSH_TIMEOUT",
    "DEFAULT_DRAIN_TIMEOUT",
]
