"""Buffered recorder: wraps another recorder with asynchronous, batched
writes on a background daemon thread.

Events are held in an in-memory buffer and sent to the inner recorder in
batches of up to ``flush_size``, one in flight, when the buffer reaches
``min(flush_size, buffer_size)`` or ``flush_interval`` elapses, whichever
comes first. ``record`` never blocks on the network (it only enqueues), so it is
safe to call from async handlers - the flush I/O happens off the event loop
on the worker thread.
"""

from __future__ import annotations

import copy
import inspect
import logging
import threading
import time
from collections import deque
from collections.abc import Callable
from dataclasses import dataclass
from enum import Enum

from ..event import Event, prepare_event
from ..event.types import Logger
from .base import Recorder
from .deliver import deliver
from .drop import DroppedByReason, DropReason, OnDrop
from .errors import BufferFullError, DrainTimeoutError

DEFAULT_BUFFER_SIZE = 1000
DEFAULT_FLUSH_SIZE = 100
DEFAULT_FLUSH_INTERVAL = 5.0
DEFAULT_FLUSH_TIMEOUT = 30.0
DEFAULT_DRAIN_TIMEOUT = 30.0
DEFAULT_INITIAL_BACKOFF = 0.5
DEFAULT_MAX_BACKOFF = 30.0
DEFAULT_MAX_RETRY_DURATION = 300.0

# Durations are clamped here so clock arithmetic and waits never overflow.
_MAX_DURATION = 30 * 365 * 24 * 3600.0
# Largest batch size; larger or infinite values clamp to it.
_MAX_SIZE = 1 << 31
# How long close waits for a cancelled worker before giving up on it.
_CLOSE_GRACE = 1.0


def _duration(v: float | None, default: float) -> float:
    """Seconds; None, <= 0, or NaN use ``default``, huge values are clamped."""
    if v is None or not v > 0:
        return default
    return float(min(v, _MAX_DURATION))


def _size(v: int | None, default: int) -> int:
    """A count; None, <= 0, or NaN use ``default``, others clamp to [1, cap]."""
    if v is None or not v > 0:
        return default
    return max(1, int(min(v, _MAX_SIZE)))


def _wait_limit(v: float) -> float:
    """A duration for thread.join, which raises past ``TIMEOUT_MAX``."""
    return float(min(v, threading.TIMEOUT_MAX))


def _timeout(v: float) -> float:
    """Clamp a huge flush timeout to what sockets and waits accept; other
    values pass through unchanged."""
    if v is None or not v > 0:
        return v
    return float(min(v, _MAX_DURATION, threading.TIMEOUT_MAX))


def _accepts_timeout(fn: Callable[..., None]) -> bool:
    """Whether ``fn`` takes a ``timeout`` keyword (or ``**kwargs``)."""
    try:
        params = inspect.signature(fn).parameters.values()
    except (TypeError, ValueError):
        return False
    return any(
        p.kind is p.VAR_KEYWORD or (p.name == "timeout" and p.kind is not p.POSITIONAL_ONLY)
        for p in params
    )


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


class BufferedRecorder:
    """Wraps ``inner`` with asynchronous batched writes. Spawns a background
    daemon thread that runs until :meth:`close`. Safe for concurrent
    :meth:`record` calls. Implements :class:`Recorder`.

    Always call :meth:`close`: the worker is a daemon thread, so events still
    buffered when the process exits are lost unreported."""

    def __init__(
        self,
        inner: Recorder,
        *,
        buffer_size: int = DEFAULT_BUFFER_SIZE,
        flush_size: int | None = DEFAULT_FLUSH_SIZE,
        flush_interval: float | None = DEFAULT_FLUSH_INTERVAL,
        flush_timeout: float = DEFAULT_FLUSH_TIMEOUT,
        overflow: OverflowPolicy = OverflowPolicy.DROP_NEWEST,
        drain_timeout: float = DEFAULT_DRAIN_TIMEOUT,
        logger: Logger | None = None,
        on_drop: OnDrop | None = None,
        initial_backoff: float | None = None,
        max_backoff: float | None = None,
        max_retry_duration: float | None = None,
    ) -> None:
        """``on_drop`` is called with (events, reason, err) when events are
        dropped. It must not block, must not call ``record``, ``flush``, or
        ``close`` on this recorder, and may be called concurrently from
        request threads and the worker.

        A failed batch is retried with jittered backoff starting at
        ``initial_backoff`` seconds (default 0.5), capped at ``max_backoff``
        (default 30), for up to ``max_retry_duration`` (default 300). None or
        <= 0 uses the default, as it does for ``flush_size`` and
        ``flush_interval``."""
        self._inner = inner
        self._buffer_size = buffer_size
        self._flush_size = _size(flush_size, DEFAULT_FLUSH_SIZE)
        self._flush_interval = _duration(flush_interval, DEFAULT_FLUSH_INTERVAL)
        self._send_at = min(self._flush_size, buffer_size)
        # NaN means unset, like None: the inner uses its own timeout.
        self._flush_timeout = None if flush_timeout != flush_timeout else _timeout(flush_timeout)
        self._overflow = OverflowPolicy(overflow)
        self._drain_timeout = _wait_limit(_duration(drain_timeout, DEFAULT_DRAIN_TIMEOUT))
        self._initial_backoff = _duration(initial_backoff, DEFAULT_INITIAL_BACKOFF)
        self._max_backoff = _duration(max_backoff, DEFAULT_MAX_BACKOFF)
        self._max_retry_duration = _duration(max_retry_duration, DEFAULT_MAX_RETRY_DURATION)
        self._logger: Logger = logger or logging.getLogger("everscribe.recorder")

        self._cond = threading.Condition()
        self._buf: deque[Event] = deque()
        # Exact flush counting: events appended, events taken by the worker,
        # events resolved, and the highest count a flush is waiting on.
        self._enqueued = 0
        self._taken = 0
        self._done = 0
        self._flush_target = 0
        self._ticked = False
        self._closed = False
        self._stopped = False
        # Set by close to stop retries once the drain times out.
        self._cancel = threading.Event()
        # The first close call's outcome, shared with later calls.
        self._closing = False
        self._close_done = False
        self._close_timed_out = False
        self._on_drop = on_drop
        self._drops: dict[DropReason, int] = dict.fromkeys(DropReason, 0)
        self._flushed = 0
        self._flush_errs = 0
        self._retries = 0

        # Detect the optional batch capability once.
        rb = getattr(inner, "record_batch", None)
        self._record_batch: Callable[..., None] | None = rb if callable(rb) else None
        self._batch_timeout = rb is not None and _accepts_timeout(rb)

        self._thread = threading.Thread(target=self._run, name="everscribe-recorder", daemon=True)
        self._thread.start()

    # -- public API --------------------------------------------------------

    def record(self, e: Event) -> None:
        """Enqueue an event for asynchronous delivery. Empty-``action`` events
        are no-ops. Behavior when the buffer is full follows the configured
        :class:`OverflowPolicy`. Calls after :meth:`close` report the event
        to ``on_drop`` as ``CLOSED``.

        ``prepare_event`` runs here, on the calling thread, so the in-scope
        status capture (and request context) is applied before the event is
        handed to the worker thread."""
        if e is None or e.action == "":
            return
        # Decide under the lock, report the drop after releasing it.
        drop: DropReason | None = None
        # Mark the event first so end_event cannot report it a second time.
        prepare_event(e)
        with self._cond:
            if self._closed:
                drop = DropReason.CLOSED
            else:
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
                    self._enqueued += 1
                    if len(self._buf) >= self._send_at:
                        # notify_all: BLOCK producers share the condition.
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
        """Send every event queued at call time and block until each is
        delivered or reported to ``on_drop``. Raises ``TimeoutError`` if
        ``timeout`` elapses first; the send itself carries on. Events
        recorded after the call don't hold it up. A no-op after
        :meth:`close`."""
        deadline = None if timeout is None else time.monotonic() + _timeout(timeout)
        with self._cond:
            if self._closed:
                return
            # Counted now; FIFO order makes done >= target exact.
            target = self._enqueued
            if target > self._flush_target:
                self._flush_target = target
                self._cond.notify_all()
            while self._done < target and not self._stopped:
                if deadline is None:
                    self._cond.wait()
                    continue
                left = deadline - time.monotonic()
                if not left > 0:  # also NaN
                    raise TimeoutError("recorder: flush timed out")
                self._cond.wait(min(left, threading.TIMEOUT_MAX))

    def close(self) -> None:
        """Drain pending events, with retries, and stop the worker thread.
        Raises :class:`DrainTimeoutError` if ``drain_timeout`` elapses first;
        what is still queued is then reported as ``SHUTDOWN``. Later and
        concurrent calls wait for the first and share its outcome."""
        with self._cond:
            if not self._closed:
                self._closed = True
                # Wakes BLOCK producers (they report CLOSED) and the worker.
                self._cond.notify_all()
            if threading.current_thread() is self._thread:
                # Called from on_drop on the worker: it can't wait for itself.
                return
            if self._closing:
                while not self._close_done:
                    self._cond.wait()
                timed_out = self._close_timed_out
            else:
                self._closing = True
                timed_out = None
        if timed_out is None:
            timed_out = self._close_first()
        if timed_out:
            raise DrainTimeoutError()

    def _close_first(self) -> bool:
        """Wait out the drain; returns whether it timed out."""
        self._thread.join(self._drain_timeout)
        if not self._thread.is_alive():
            with self._cond:
                # Normally empty; not if the worker died early.
                left = self._take_all()
                self._close_done = True
                self._cond.notify_all()
            self._report_shutdown(left)
            return False
        # Stop retries and backoff; a running send can't be interrupted.
        self._cancel.set()
        self._thread.join(_CLOSE_GRACE)
        left = []
        with self._cond:
            if self._thread.is_alive():
                # The worker reports its in-flight batch when the send returns.
                left = self._take_all()
                self._stopped = True
            # Publish before reporting, so an on_drop calling close returns.
            self._close_done = True
            self._close_timed_out = True
            self._cond.notify_all()
        self._report_shutdown(left)
        return True

    def _take_all(self) -> list[Event]:
        """Empty the buffer and count it resolved. Call holding ``_cond``."""
        left = list(self._buf)
        self._buf.clear()
        self._taken += len(left)
        self._done += len(left)
        self._cond.notify_all()
        return left

    def _report_shutdown(self, events: list[Event]) -> None:
        if not events:
            return
        self._report_drop(events, DropReason.SHUTDOWN)
        self._logger.error(
            "everscribe: recorder dropped events (reason=%s count=%d error=%s)",
            DropReason.SHUTDOWN.value,
            len(events),
            None,
        )

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
        try:
            self._loop()
        finally:
            # On any exit, even an exception from on_drop, release flush and
            # close and report what is left.
            with self._cond:
                self._stopped = True
                left = self._take_all()
            self._report_shutdown(left)

    def _loop(self) -> None:
        next_tick = time.monotonic() + self._flush_interval
        while True:
            with self._cond:
                while True:
                    now = time.monotonic()
                    if now >= next_tick:
                        # Also catches a tick that passed during a long send.
                        self._ticked = True
                        next_tick = now + self._flush_interval
                    n = len(self._buf)
                    if n == 0:
                        self._ticked = False
                        if self._closed:
                            self._stopped = True
                            self._cond.notify_all()
                            return
                    elif (
                        self._closed
                        or self._ticked
                        or self._taken < self._flush_target
                        or n >= self._send_at
                    ):
                        break
                    self._cond.wait(min(next_tick - now, threading.TIMEOUT_MAX))
                self._ticked = False
                k = min(n, self._flush_size)
                batch = [self._buf.popleft() for _ in range(k)]
                self._taken += k
                # Wake producers blocked on a full buffer (BLOCK policy).
                self._cond.notify_all()

            # Send outside the lock so record() stays responsive during I/O.
            try:
                rest = deliver(self, batch, self._cancel)
            except Exception as ex:  # noqa: BLE001 -- deliver reports its own failures; bug guard
                self._logger.error("everscribe: recorder delivery failed (error=%s)", ex)
                self._report_drop(batch, DropReason.REJECTED, ex)
                rest = []
            self._report_shutdown(rest)
            with self._cond:
                self._done += k
                self._cond.notify_all()


__all__ = [
    "DEFAULT_BUFFER_SIZE",
    "DEFAULT_DRAIN_TIMEOUT",
    "DEFAULT_FLUSH_INTERVAL",
    "DEFAULT_FLUSH_SIZE",
    "DEFAULT_FLUSH_TIMEOUT",
    "DEFAULT_INITIAL_BACKOFF",
    "DEFAULT_MAX_BACKOFF",
    "DEFAULT_MAX_RETRY_DURATION",
    "BufferedRecorder",
    "BufferedStats",
    "OverflowPolicy",
]
