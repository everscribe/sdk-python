"""Buffered recorder: wraps another recorder with asynchronous, batched
writes on a background daemon thread.

Events are held in an in-memory buffer and flushed to the inner recorder when the pending
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
from collections.abc import Callable
from dataclasses import dataclass
from enum import Enum
from queue import Empty, Queue

from ..event import Event, prepare_event
from ..event.types import Logger
from .base import Recorder
from .drop import DroppedByReason, DropReason, OnDrop
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

    dropped: int  # events dropped on overflow; equals dropped_by_reason.buffer_full
    flushed: int  # total events successfully flushed to the inner recorder
    flush_errs: int  # failed delivery attempts
    pending: int  # events currently in the buffer
    buffer_size: int  # buffer capacity
    retries: int  # delivery retries performed
    dropped_by_reason: DroppedByReason  # drops per reason


@dataclass
class _FlushRequest:
    ack: Queue[BaseException | None]


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
        logger: Logger | None = None,
        on_drop: OnDrop | None = None,
    ) -> None:
        """``on_drop`` is called with (events, reason, err) when events are
        dropped. It must not block, must not call ``record``, ``flush``, or
        ``close`` on this recorder, and may be called concurrently from
        request threads and the worker."""
        self._inner = inner
        self._buffer_size = buffer_size
        self._flush_size = flush_size
        self._flush_interval = flush_interval
        self._flush_timeout = flush_timeout
        self._overflow = OverflowPolicy(overflow)
        self._drain_timeout = drain_timeout
        self._logger: Logger = logger or logging.getLogger("everscribe.recorder")

        self._cond = threading.Condition()
        self._buf: deque[Event] = deque()
        self._flush_requests: list[_FlushRequest] = []
        self._closed = False
        self._on_drop = on_drop
        self._drops: dict[DropReason, int] = dict.fromkeys(DropReason, 0)
        self._flushed = 0
        self._flush_errs = 0
        self._retries = 0

        # Detect the optional batch capability once.
        rb = getattr(inner, "record_batch", None)
        self._record_batch: Callable[..., None] | None = rb if callable(rb) else None

        self._next_flush = time.monotonic() + flush_interval
        self._thread = threading.Thread(target=self._run, name="everscribe-recorder", daemon=True)
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
        # Decide under the lock, report the drop after releasing it.
        drop: DropReason | None = None
        with self._cond:
            if self._closed:
                drop = DropReason.CLOSED
            else:
                prepare_event(e)
                if len(self._buf) >= self._buffer_size:
                    if self._overflow is OverflowPolicy.ERROR:
                        raise BufferFullError()
                    if self._overflow is OverflowPolicy.BLOCK:
                        while len(self._buf) >= self._buffer_size and not self._closed:
                            self._cond.wait()
                        if self._closed:
                            drop = DropReason.CLOSED
                    else:  # DROP_NEWEST
                        drop = DropReason.BUFFER_FULL
                if drop is None:
                    self._buf.append(copy.copy(e))
                    # notify_all (not notify): under BLOCK policy the worker and
                    # blocked producers share the condition; only the worker acts.
                    self._cond.notify_all()
        if drop is None:
            return
        n = self._report_drop([e], drop)
        if drop is DropReason.BUFFER_FULL and (n == 1 or n % 1000 == 0):
            self._logger.warning(
                "everscribe: recorder buffer full, event dropped "
                "(action=%s dropped_total=%d buffer_size=%d)",
                e.action,
                n,
                self._buffer_size,
            )

    def _report_drop(
        self,
        events: list[Event],
        reason: DropReason,
        err: BaseException | None = None,
    ) -> int:
        """Count dropped events, then call ``on_drop`` outside the lock.
        Returns the post-increment buffer_full total. Never call while
        holding ``_cond``."""
        with self._cond:
            self._drops[reason] += len(events)
            total = self._drops[DropReason.BUFFER_FULL]
        cb = self._on_drop
        if cb is not None:
            try:
                cb(list(events), reason, err)
            except Exception as ex:  # noqa: BLE001 -- a bad callback must not break delivery
                self._logger.error(
                    "everscribe: on_drop callback failed (error=%s reason=%s)", ex, reason.value
                )
        return total

    def flush(self, timeout: float | None = None) -> None:
        """Force an immediate flush of all events buffered at call time and
        block until they are persisted. Raises the inner recorder's error if
        the flush failed, or ``TimeoutError`` if ``timeout`` elapses first.
        A no-op after :meth:`close`."""
        ack: Queue[BaseException | None] = Queue(maxsize=1)
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
                dropped=self._drops[DropReason.BUFFER_FULL],
                flushed=self._flushed,
                flush_errs=self._flush_errs,
                pending=len(self._buf),
                buffer_size=self._buffer_size,
                retries=self._retries,
                dropped_by_reason=DroppedByReason(
                    buffer_full=self._drops[DropReason.BUFFER_FULL],
                    rejected=self._drops[DropReason.REJECTED],
                    retries_exhausted=self._drops[DropReason.RETRIES_EXHAUSTED],
                    shutdown=self._drops[DropReason.SHUTDOWN],
                    closed=self._drops[DropReason.CLOSED],
                ),
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

    def _flush_batch(self, batch: list[Event]) -> BaseException | None:
        n = len(batch)
        if n == 0:
            return None
        err: BaseException | None = None
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
                    except Exception as ex:  # noqa: BLE001 -- one failure must not abort the batch
                        err = ex
        except Exception as ex:  # noqa: BLE001 -- audit flush must never propagate
            err = ex
        if err is not None:
            with self._cond:
                self._flush_errs += 1
                total = self._flush_errs
            self._logger.error(
                "everscribe: recorder flush failed (error=%s batch_size=%d flush_errs_total=%d)",
                err,
                n,
                total,
            )
            return err
        with self._cond:
            self._flushed += n
        return None


__all__ = [
    "DEFAULT_BUFFER_SIZE",
    "DEFAULT_DRAIN_TIMEOUT",
    "DEFAULT_FLUSH_INTERVAL",
    "DEFAULT_FLUSH_SIZE",
    "DEFAULT_FLUSH_TIMEOUT",
    "BufferedRecorder",
    "BufferedStats",
    "OverflowPolicy",
]
