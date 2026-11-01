"""Deliver one batch: the retry loop and splitting of rejected batches."""

from __future__ import annotations

import random
import threading
import time
from collections.abc import Callable
from typing import TYPE_CHECKING

from ..event import Event
from .drop import DropReason
from .retry import is_retryable, is_splittable, retry_wait

if TYPE_CHECKING:
    from .buffered import BufferedRecorder

# Split sends allowed per original batch.
SPLIT_BUDGET = 32


def full_jitter(d: float) -> float:
    """A uniform random wait in [0, d]."""
    return random.uniform(0.0, d)


def deliver(
    rec: BufferedRecorder,
    batch: list[Event],
    cancel: threading.Event,
    *,
    clock: Callable[[], float] = time.monotonic,
    sleep: Callable[[float], object] | None = None,
    jitter: Callable[[float], float] | None = None,
) -> list[Event]:
    """Send ``batch``, retrying and splitting, until every event is
    delivered or reported. Returns ``[]`` then. Once ``cancel`` is set no new
    attempt starts, and the unresolved events come back in order, unreported."""
    if not batch:
        return []
    if cancel.is_set():
        return list(batch)
    d = _Delivery(rec, cancel, clock, sleep or cancel.wait, jitter or full_jitter)
    return d.send(list(batch))


class _Delivery:
    """State shared by one batch and the halves split from it."""

    def __init__(
        self,
        rec: BufferedRecorder,
        cancel: threading.Event,
        clock: Callable[[], float],
        sleep: Callable[[float], object],
        jitter: Callable[[float], float],
    ) -> None:
        self.rec = rec
        self.cancel = cancel
        self.clock = clock
        self.sleep = sleep
        self.jitter = jitter
        self.deadline = clock() + rec._max_retry_duration
        self.budget = SPLIT_BUDGET
        self.split_err: BaseException | None = None

    def send(self, p: list[Event]) -> list[Event]:
        """Resolve part ``p`` with the retry loop."""
        rec = self.rec
        # A part first reached after the deadline gets exactly one attempt.
        final = self.clock() >= self.deadline
        n = 1
        while True:
            retry, err = self.attempt(p)
            if err is None:
                return []
            if self.cancel.is_set():
                return retry
            with rec._cond:
                rec._flush_errs += 1
            splits = len(retry) > 1 and is_splittable(err)
            log = _debug(rec) if splits else rec._logger.warning
            log(
                "everscribe: recorder delivery attempt failed (error=%s batch_size=%d attempt=%d)",
                err,
                len(p),
                n,
            )
            if not retry:
                return []
            if splits:
                self.split_err = err
                return self.split(retry)
            if not is_retryable(err):
                self.drop(retry, DropReason.REJECTED, err)
                return []
            if final:
                self.drop(retry, DropReason.RETRIES_EXHAUSTED, err)
                return []
            p = retry
            wait = retry_wait(n, err, rec._initial_backoff, rec._max_backoff, self.jitter)
            left = self.deadline - self.clock()
            if wait >= left:
                wait, final = max(left, 0.0), True
            if wait > 0:
                self.sleep(wait)
            if self.cancel.is_set():
                return p
            with rec._cond:
                rec._retries += 1
            n += 1

    def split(self, p: list[Event]) -> list[Event]:
        """Halve ``p``; resolve the first half fully before the second."""
        mid = len(p) // 2
        halves = (p[:mid], p[mid:])
        for i, h in enumerate(halves):
            if self.budget == 0:
                self.drop(h, DropReason.REJECTED, self.split_err)
                continue
            self.budget -= 1
            rest = self.send(h)
            if rest:
                return rest + halves[1] if i == 0 else rest
        return []

    def attempt(self, p: list[Event]) -> tuple[list[Event], BaseException | None]:
        """Send ``p`` once. Returns the unresolved events and the error
        behind them; a None error means nothing is left to do."""
        rec = self.rec
        rb = rec._record_batch
        if rb is not None:
            try:
                if rec._batch_timeout:
                    rb(p, timeout=rec._flush_timeout)
                else:
                    rb(p)
            except Exception as ex:  # noqa: BLE001 -- classified by the caller
                return p, ex
            with rec._cond:
                rec._flushed += len(p)
            return [], None

        # Serial fallback: keep only retryable failures, reject the rest alone.
        failed: list[Event] = []
        last: BaseException | None = None
        retry_err: BaseException | None = None
        for i, e in enumerate(p):
            if self.cancel.is_set():
                return failed + p[i:], last or InterruptedError("cancelled")
            try:
                rec._inner.record(e)
            except Exception as ex:  # noqa: BLE001 -- classified below
                if self.cancel.is_set():
                    return failed + p[i:], ex
                if is_retryable(ex):
                    failed.append(e)
                    retry_err = ex
                else:
                    self.drop([e], DropReason.REJECTED, ex)
                last = ex
                continue
            with rec._cond:
                rec._flushed += 1
        if retry_err is not None:
            return failed, retry_err
        return [], last

    def drop(self, events: list[Event], reason: DropReason, err: BaseException | None) -> None:
        self.rec._report_drop(events, reason, err)
        self.rec._logger.error(
            "everscribe: recorder dropped events (reason=%s count=%d error=%s)",
            reason.value,
            len(events),
            err,
        )


def _debug(rec: BufferedRecorder) -> Callable[..., object]:
    # The Logger protocol has no debug; fall back to silence.
    dbg = getattr(rec._logger, "debug", None)
    return dbg if callable(dbg) else (lambda *_a, **_k: None)


__all__ = ["SPLIT_BUDGET", "deliver", "full_jitter"]
