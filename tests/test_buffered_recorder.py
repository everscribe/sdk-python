"""Tests for BufferedRecorder: flush triggers, overflow policies, drain on
close, batch-vs-serial, error propagation, and stats."""

from __future__ import annotations

import logging
import threading
import time
from collections.abc import Callable, Sequence

import pytest

from everscribe.event import Event, end_event
from everscribe.recorder import BufferedRecorder, DropReason, OverflowPolicy, buffered
from everscribe.recorder.errors import BufferFullError, DrainTimeoutError, HTTPError, NetworkError


def wait_until(pred: Callable[[], bool], timeout: float = 2.0) -> bool:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if pred():
            return True
        time.sleep(0.005)
    return pred()


class FakeInner:
    """Inner recorder with batch capability; records everything it sees."""

    def __init__(self) -> None:
        self.lock = threading.Lock()
        self.batches: list[list[Event]] = []
        self.singles: list[Event] = []
        self.fail: BaseException | None = None
        self.block: threading.Event | None = None

    def record_batch(self, events: Sequence[Event], *, timeout: float | None = None) -> None:
        if self.block is not None:
            self.block.wait(timeout=5)
        if self.fail is not None:
            raise self.fail
        with self.lock:
            self.batches.append(list(events))

    def record(self, e: Event, *, timeout: float | None = None) -> None:
        with self.lock:
            self.singles.append(e)

    def all_events(self) -> list[Event]:
        with self.lock:
            out = list(self.singles)
            for b in self.batches:
                out.extend(b)
            return out


class GatedInner(FakeInner):
    """Each record_batch call takes one permit from ``gate`` before it
    finishes; ``fails`` leading calls raise ``err``."""

    def __init__(self, permits: int = 0) -> None:
        super().__init__()
        self.gate = threading.Semaphore(permits)
        self.calls = 0
        self.fails = 0
        self.err: BaseException = NetworkError("down")

    def record_batch(self, events: Sequence[Event], *, timeout: float | None = None) -> None:
        with self.lock:
            self.calls += 1
            fail = self.fails > 0
            if fail:
                self.fails -= 1
        if fail:
            raise self.err
        assert self.gate.acquire(timeout=5)
        with self.lock:
            self.batches.append(list(events))

    def sizes(self) -> list[int]:
        with self.lock:
            return [len(b) for b in self.batches]


class SerialInner:
    """Inner recorder WITHOUT record_batch, to exercise the serial fallback."""

    def __init__(self) -> None:
        self.lock = threading.Lock()
        self.singles: list[Event] = []

    def record(self, e: Event, *, timeout: float | None = None) -> None:
        with self.lock:
            self.singles.append(e)


# Large size/interval so the worker only flushes on explicit flush/close;
# used by overflow tests that need events to stay buffered.
_QUIET = {"flush_size": 10_000, "flush_interval": 3600.0}


def hold_one_in_flight(r: BufferedRecorder, inner: GatedInner) -> None:
    """Park one batch in the gated inner so the buffer itself can fill."""
    r.record(Event("inflight"))
    assert wait_until(lambda: inner.calls == 1)


def test_flush_triggers_on_size() -> None:
    inner = FakeInner()
    r = BufferedRecorder(inner, flush_size=2, flush_interval=3600.0)
    try:
        r.record(Event("a"))
        r.record(Event("b"))
        assert wait_until(lambda: len(inner.all_events()) == 2)
        assert {e.action for e in inner.all_events()} == {"a", "b"}
    finally:
        r.close()


def test_flush_triggers_on_interval() -> None:
    inner = FakeInner()
    r = BufferedRecorder(inner, flush_size=10_000, flush_interval=0.05)
    try:
        r.record(Event("a"))
        assert wait_until(lambda: len(inner.all_events()) == 1)
    finally:
        r.close()


def test_manual_flush() -> None:
    inner = FakeInner()
    r = BufferedRecorder(inner, **_QUIET)
    try:
        r.record(Event("a"))
        r.record(Event("b"))
        r.flush(timeout=2)
        assert len(inner.all_events()) == 2
    finally:
        r.close()


def test_empty_action_not_enqueued() -> None:
    inner = FakeInner()
    r = BufferedRecorder(inner, **_QUIET)
    try:
        r.record(Event())
        r.flush(timeout=2)
        assert inner.all_events() == []
        assert r.stats().dropped == 0
    finally:
        r.close()


def test_prepare_event_runs_on_calling_thread() -> None:
    inner = FakeInner()
    r = BufferedRecorder(inner, **_QUIET)
    try:
        e = Event("a")
        e.id = ""
        r.record(e)
        r.flush(timeout=2)
        assert inner.all_events()[0].id  # id filled before enqueue
    finally:
        r.close()


def test_close_drains_pending() -> None:
    inner = FakeInner()
    r = BufferedRecorder(inner, **_QUIET)
    r.record(Event("a"))
    r.record(Event("b"))
    r.close()  # should drain
    assert {e.action for e in inner.all_events()} == {"a", "b"}


def test_close_is_idempotent() -> None:
    inner = FakeInner()
    r = BufferedRecorder(inner, **_QUIET)
    r.close()
    r.close()  # no raise


def test_record_after_close_reports_closed_once() -> None:
    inner = FakeInner()
    seen: list[DropReason] = []
    r = BufferedRecorder(inner, on_drop=lambda _e, reason, _x: seen.append(reason), **_QUIET)
    r.close()
    r.record(Event("a"))
    assert inner.all_events() == []
    assert seen == [DropReason.CLOSED]


def test_flush_after_close_is_noop() -> None:
    inner = FakeInner()
    r = BufferedRecorder(inner, **_QUIET)
    r.close()
    r.flush(timeout=1)  # returns without raising


def test_overflow_drop_newest() -> None:
    inner = GatedInner()
    r = BufferedRecorder(inner, buffer_size=1, overflow=OverflowPolicy.DROP_NEWEST, **_QUIET)
    try:
        hold_one_in_flight(r, inner)
        r.record(Event("a"))  # fills buffer
        r.record(Event("b"))  # dropped
        r.record(Event("c"))  # dropped
        assert r.stats().dropped == 2
        assert r.stats().pending == 1
    finally:
        inner.gate.release(10)
        r.close()


def test_overflow_error() -> None:
    inner = GatedInner()
    r = BufferedRecorder(inner, buffer_size=1, overflow=OverflowPolicy.ERROR, **_QUIET)
    try:
        hold_one_in_flight(r, inner)
        r.record(Event("a"))
        with pytest.raises(BufferFullError):
            r.record(Event("b"))
    finally:
        inner.gate.release(10)
        r.close()


def test_overflow_string_alias_accepted() -> None:
    inner = GatedInner()
    r = BufferedRecorder(inner, buffer_size=1, overflow="error", **_QUIET)  # type: ignore[arg-type]
    try:
        hold_one_in_flight(r, inner)
        r.record(Event("a"))
        with pytest.raises(BufferFullError):
            r.record(Event("b"))
    finally:
        inner.gate.release(10)
        r.close()


def test_overflow_block_unblocks_on_space() -> None:
    inner = GatedInner()
    r = BufferedRecorder(inner, buffer_size=1, overflow=OverflowPolicy.BLOCK, **_QUIET)
    try:
        hold_one_in_flight(r, inner)
        r.record(Event("a"))  # fills buffer
        done = threading.Event()

        def producer() -> None:
            r.record(Event("b"))  # blocks until space
            done.set()

        t = threading.Thread(target=producer)
        t.start()
        # still blocked (buffer full, one batch in flight)
        assert not done.wait(timeout=0.2)
        inner.gate.release(10)  # the worker takes "a" -> frees space
        assert done.wait(timeout=2)
        t.join()
        r.flush(timeout=2)
        assert _actions(inner) == ["inflight", "a", "b"]
    finally:
        inner.gate.release(10)
        r.close()


def test_serial_fallback_used_without_record_batch() -> None:
    inner = SerialInner()
    r = BufferedRecorder(inner, **_QUIET)
    try:
        r.record(Event("a"))
        r.record(Event("b"))
        r.flush(timeout=2)
        assert {e.action for e in inner.singles} == {"a", "b"}
    finally:
        r.close()


def test_batch_path_used_with_record_batch() -> None:
    inner = FakeInner()
    r = BufferedRecorder(inner, **_QUIET)
    try:
        r.record(Event("a"))
        r.record(Event("b"))
        r.flush(timeout=2)
        assert len(inner.batches) == 1
        assert inner.singles == []
    finally:
        r.close()


def test_stats_flushed_counter() -> None:
    inner = FakeInner()
    r = BufferedRecorder(inner, **_QUIET)
    try:
        r.record(Event("a"))
        r.record(Event("b"))
        r.flush(timeout=2)
        assert r.stats().flushed == 2
    finally:
        r.close()


def test_flush_error_is_reported_not_raised() -> None:
    inner = FakeInner()
    inner.fail = HTTPError(422, "boom")
    drops: list[tuple[list[str], DropReason]] = []
    r = BufferedRecorder(
        inner,
        on_drop=lambda evs, reason, _e: drops.append(([e.action for e in evs], reason)),
        **_QUIET,
    )
    try:
        r.record(Event("a"))
        r.flush(timeout=2)  # returns; the event was reported
        assert drops == [(["a"], DropReason.REJECTED)]
        assert r.stats().flush_errs == 1
    finally:
        r.close()


def test_close_drain_timeout_raises() -> None:
    inner = FakeInner()
    inner.block = threading.Event()  # record_batch will hang
    r = BufferedRecorder(inner, drain_timeout=0.1, **_QUIET)
    r.record(Event("a"))
    start = time.monotonic()
    try:
        with pytest.raises(DrainTimeoutError):
            r.close()
        assert time.monotonic() - start < 0.1 + 1.5
    finally:
        inner.block.set()  # let the worker finish so the thread can exit


def _actions(inner: FakeInner) -> list[str]:
    return [e.action for e in inner.all_events()]


def test_order_kept_during_retry() -> None:
    inner = GatedInner(permits=100)
    inner.fails = 2
    r = BufferedRecorder(inner, flush_size=2, initial_backoff=0.01, flush_interval=3600.0)
    try:
        names = [f"e{i}" for i in range(7)]
        for n in names:
            r.record(Event(n))
        r.flush(timeout=5)
        assert _actions(inner) == names
        assert inner.sizes() == [2, 2, 2, 1]
        assert r.stats().retries == 2
    finally:
        r.close()


def test_flush_sends_batches_of_flush_size() -> None:
    inner = GatedInner(permits=100)
    r = BufferedRecorder(inner, flush_size=100, flush_interval=3600.0)
    try:
        for i in range(250):
            r.record(Event(f"e{i}"))
        r.flush(timeout=5)
        assert inner.sizes() == [100, 100, 50]
        assert _actions(inner) == [f"e{i}" for i in range(250)]
    finally:
        r.close()


def test_full_batch_now_rest_on_interval() -> None:
    inner = GatedInner(permits=100)
    r = BufferedRecorder(inner, flush_size=100, flush_interval=1.5)
    try:
        start = time.monotonic()
        for i in range(150):
            r.record(Event(f"e{i}"))
        assert wait_until(lambda: inner.sizes() == [100], timeout=1.0)
        assert r.stats().pending == 50  # the rest waits for the interval
        assert wait_until(lambda: inner.sizes() == [100, 50], timeout=3.0)
        assert time.monotonic() - start < 1.5 + 1.0
    finally:
        r.close()


def _flush_in_thread(r: BufferedRecorder) -> threading.Event:
    done = threading.Event()

    def run() -> None:
        r.flush(timeout=5)
        done.set()

    threading.Thread(target=run, daemon=True).start()
    return done


def test_overlapping_flushes_wait_for_their_own_events() -> None:
    inner = GatedInner()
    r = BufferedRecorder(inner, **_QUIET)
    try:
        r.record(Event("a"))
        r.record(Event("b"))
        first = _flush_in_thread(r)
        assert wait_until(lambda: inner.calls == 1)  # [a, b] in flight
        r.record(Event("c"))  # recorded after the first flush
        second = _flush_in_thread(r)
        assert not first.wait(0.1)
        inner.gate.release()
        assert first.wait(2)
        assert _actions(inner) == ["a", "b"]
        assert not second.wait(0.1)
        inner.gate.release()
        assert second.wait(2)
        assert _actions(inner) == ["a", "b", "c"]
    finally:
        inner.gate.release()
        r.close()


def test_buffer_full_during_outage_reports_each_drop_once() -> None:
    inner = GatedInner(permits=100)
    inner.fails = 1_000_000
    drops: list[tuple[list[str], DropReason]] = []
    r = BufferedRecorder(
        inner,
        buffer_size=2,
        flush_size=2,
        flush_interval=3600.0,
        initial_backoff=0.01,
        max_backoff=0.01,
        on_drop=lambda evs, reason, _e: drops.append(([e.action for e in evs], reason)),
    )
    try:
        r.record(Event("a"))
        r.record(Event("b"))
        assert wait_until(lambda: inner.calls >= 2)  # [a, b] retrying
        for n in ("c", "d", "e", "f", "g"):
            r.record(Event(n))
        assert drops == [
            (["e"], DropReason.BUFFER_FULL),
            (["f"], DropReason.BUFFER_FULL),
            (["g"], DropReason.BUFFER_FULL),
        ]
        with inner.lock:
            inner.fails = 0
        r.flush(timeout=5)
        assert _actions(inner) == ["a", "b", "c", "d"]
        assert r.stats().dropped == 3
        assert len(drops) == 3
    finally:
        r.close()


@pytest.mark.parametrize("size", [0, -5, None])
def test_unset_flush_size_uses_100(size: int | None) -> None:
    inner = GatedInner(permits=100)
    r = BufferedRecorder(inner, flush_size=size, flush_interval=3600.0)  # type: ignore[arg-type]
    try:
        for i in range(250):
            r.record(Event(f"e{i}"))
        r.flush(timeout=5)
        assert inner.sizes() == [100, 100, 50]
    finally:
        r.close()


@pytest.mark.parametrize("interval", [0, -1.0, float("nan"), None])
def test_unset_flush_interval_uses_default(interval: float | None) -> None:
    inner = GatedInner(permits=100)
    r = BufferedRecorder(inner, flush_size=100, flush_interval=interval)  # type: ignore[arg-type]
    try:
        r.record(Event("a"))
        time.sleep(0.1)
        assert inner.sizes() == []  # no busy sends on a zero interval
        r.flush(timeout=2)
        assert inner.sizes() == [1]
    finally:
        r.close()


def test_small_buffer_large_flush_size_block_delivers_without_interval() -> None:
    inner = GatedInner(permits=1000)
    r = BufferedRecorder(
        inner,
        buffer_size=10,
        flush_size=100,
        flush_interval=3600.0,
        overflow=OverflowPolicy.BLOCK,
    )
    done = threading.Event()

    def produce() -> None:
        for i in range(40):
            r.record(Event(f"e{i}"))
            time.sleep(0.001)
        done.set()

    try:
        threading.Thread(target=produce, daemon=True).start()
        assert done.wait(3)
        assert wait_until(lambda: len(inner.all_events()) == 40)
        assert _actions(inner) == [f"e{i}" for i in range(40)]
    finally:
        r.close()


def test_interval_during_long_send_sends_right_after() -> None:
    inner = GatedInner()
    r = BufferedRecorder(inner, flush_size=10_000, flush_interval=1.0)
    try:
        r.record(Event("a"))
        assert wait_until(lambda: inner.calls == 1)  # [a] in flight
        r.record(Event("b"))
        time.sleep(1.2)  # the next interval elapses mid-send
        inner.gate.release(2)
        assert wait_until(lambda: _actions(inner) == ["a", "b"], timeout=0.5)
    finally:
        inner.gate.release(10)
        r.close()


def test_flush_timeout_during_slow_send_still_delivers() -> None:
    inner = GatedInner()
    r = BufferedRecorder(inner, **_QUIET)
    try:
        r.record(Event("a"))
        with pytest.raises(TimeoutError):
            r.flush(timeout=0.05)
        inner.gate.release()
        assert wait_until(lambda: _actions(inner) == ["a"])
        assert r.stats().dropped_by_reason.shutdown == 0
    finally:
        inner.gate.release(10)
        r.close()


# -- close: drain, shutdown and closed reports --------------------------------


class HungInner(FakeInner):
    """record_batch blocks until ``release`` is set, then succeeds or raises
    ``after``."""

    def __init__(self) -> None:
        super().__init__()
        self.release = threading.Event()
        self.entered = threading.Event()
        self.after: BaseException | None = None

    def record_batch(self, events: Sequence[Event], *, timeout: float | None = None) -> None:
        self.entered.set()
        assert self.release.wait(30)
        if self.after is not None:
            raise self.after
        with self.lock:
            self.batches.append(list(events))


Drops = list[tuple[list[str], DropReason]]


def _collect(drops: Drops) -> Callable[[list[Event], DropReason, BaseException | None], None]:
    lock = threading.Lock()

    def cb(evs: list[Event], reason: DropReason, _x: BaseException | None) -> None:
        with lock:
            drops.append(([e.action for e in evs], reason))

    return cb


@pytest.mark.parametrize("after", [None, NetworkError("down")])
def test_close_hung_inner_reports_queued_then_in_flight_once(
    after: BaseException | None,
) -> None:
    inner = HungInner()
    inner.after = after
    drops: Drops = []
    r = BufferedRecorder(
        inner, flush_size=2, flush_interval=3600.0, drain_timeout=0.2, on_drop=_collect(drops)
    )
    try:
        r.record(Event("a"))
        r.record(Event("b"))  # [a, b] goes in flight and hangs
        assert inner.entered.wait(2)
        r.record(Event("c"))
        start = time.monotonic()
        with pytest.raises(DrainTimeoutError):
            r.close()
        assert time.monotonic() - start < 0.2 + 1.5
        assert drops == [(["c"], DropReason.SHUTDOWN)]  # queued only, before close returned
    finally:
        inner.release.set()
    if after is None:
        assert wait_until(lambda: _actions(inner) == ["a", "b"])
        time.sleep(0.05)
        assert drops == [(["c"], DropReason.SHUTDOWN)]
    else:
        assert wait_until(lambda: len(drops) == 2)
        assert drops == [(["c"], DropReason.SHUTDOWN), (["a", "b"], DropReason.SHUTDOWN)]
        assert _actions(inner) == []
    assert wait_until(lambda: not r._thread.is_alive())
    assert r.stats().pending == 0


def test_record_concurrent_with_close_every_event_resolved_once() -> None:
    for _ in range(5):
        inner = FakeInner()
        drops: Drops = []
        r = BufferedRecorder(inner, buffer_size=50, flush_size=10, on_drop=_collect(drops))
        go = threading.Event()

        def produce(t: int, rec: BufferedRecorder = r, start: threading.Event = go) -> None:
            start.wait()
            for i in range(300):
                rec.record(Event(f"t{t}-{i}"))

        threads = [threading.Thread(target=produce, args=(t,)) for t in range(8)]
        for th in threads:
            th.start()
        go.set()
        time.sleep(0.002)
        r.close()
        for th in threads:
            th.join(5)
            assert not th.is_alive()
        seen = _actions(inner) + [a for evs, _ in drops for a in evs]
        want = [f"t{t}-{i}" for t in range(8) for i in range(300)]
        assert sorted(seen) == sorted(want)
        assert {reason for _, reason in drops} <= {DropReason.BUFFER_FULL, DropReason.CLOSED}


def _close_in_thread(r: BufferedRecorder) -> tuple[threading.Thread, list[object]]:
    out: list[object] = []

    def run() -> None:
        try:
            r.close()
            out.append(None)
        except DrainTimeoutError as ex:
            out.append(ex)

    t = threading.Thread(target=run, daemon=True)
    t.start()
    return t, out


def test_concurrent_close_waits_for_first_drain() -> None:
    inner = GatedInner()
    r = BufferedRecorder(inner, **_QUIET)
    r.record(Event("a"))
    r.record(Event("b"))
    first, out1 = _close_in_thread(r)
    assert wait_until(lambda: inner.calls == 1)  # drain in flight
    second, out2 = _close_in_thread(r)
    second.join(0.2)
    assert second.is_alive()  # waits for the first
    inner.gate.release()
    first.join(2)
    second.join(2)
    assert not first.is_alive() and not second.is_alive()
    assert out1 == out2 == [None]
    assert _actions(inner) == ["a", "b"]


def test_concurrent_close_shares_drain_timeout() -> None:
    inner = HungInner()
    r = BufferedRecorder(inner, drain_timeout=0.2, **_QUIET)
    try:
        r.record(Event("a"))
        first, out1 = _close_in_thread(r)
        assert inner.entered.wait(2)
        second, out2 = _close_in_thread(r)
        first.join(3)
        second.join(3)
        assert not first.is_alive() and not second.is_alive()
        assert isinstance(out1[0], DrainTimeoutError)
        assert isinstance(out2[0], DrainTimeoutError)
        with pytest.raises(DrainTimeoutError):
            r.close()  # later calls see the same outcome
    finally:
        inner.release.set()


def test_on_drop_calling_close_from_worker_does_not_deadlock() -> None:
    inner = GatedInner(permits=100)
    inner.fails = 1
    inner.err = HTTPError(400, "bad")
    r: BufferedRecorder

    def cb(_e: list[Event], _r: DropReason, _x: BaseException | None) -> None:
        r.close()

    r = BufferedRecorder(inner, on_drop=cb, **_QUIET)
    r.record(Event("bad"))
    r.flush(timeout=2)  # the reject calls close on the worker thread
    t, out = _close_in_thread(r)
    t.join(2)
    assert not t.is_alive()
    assert out == [None]


def test_on_drop_calling_close_during_fallback_does_not_deadlock() -> None:
    inner = HungInner()
    r: BufferedRecorder
    inner_errs: list[object] = []

    def cb(_e: list[Event], _r: DropReason, _x: BaseException | None) -> None:
        try:
            r.close()
        except DrainTimeoutError as ex:
            inner_errs.append(ex)

    r = BufferedRecorder(inner, flush_size=1, flush_interval=3600.0, drain_timeout=0.1, on_drop=cb)
    try:
        r.record(Event("a"))
        assert inner.entered.wait(2)
        r.record(Event("b"))
        t, out = _close_in_thread(r)
        t.join(3)
        assert not t.is_alive()
        assert isinstance(out[0], DrainTimeoutError)
        assert len(inner_errs) == 1  # the nested close saw the stored outcome
    finally:
        inner.release.set()


def test_pending_flush_returns_when_close_gives_up() -> None:
    inner = HungInner()
    r = BufferedRecorder(inner, drain_timeout=0.1, **_QUIET)
    try:
        r.record(Event("a"))
        r.record(Event("b"))
        flushed = threading.Event()

        def run() -> None:
            r.flush()
            flushed.set()

        threading.Thread(target=run, daemon=True).start()
        assert inner.entered.wait(2)
        with pytest.raises(DrainTimeoutError):
            r.close()
        assert flushed.wait(0.5)
    finally:
        inner.release.set()


class _Bail(BaseException):
    pass


@pytest.mark.filterwarnings("ignore::pytest.PytestUnhandledThreadExceptionWarning")
def test_base_exception_from_on_drop_on_worker_does_not_hang() -> None:
    inner = GatedInner(permits=100)
    inner.fails = 1
    inner.err = HTTPError(400, "bad")
    drops: Drops = []
    raised = []

    def cb(evs: list[Event], reason: DropReason, _x: BaseException | None) -> None:
        drops.append(([e.action for e in evs], reason))
        if not raised:
            raised.append(1)
            raise _Bail

    r = BufferedRecorder(inner, flush_size=1, flush_interval=3600.0, on_drop=cb)
    r.record(Event("bad"))
    r.record(Event("next"))
    flushed = threading.Event()

    def run() -> None:
        r.flush()
        flushed.set()

    threading.Thread(target=run, daemon=True).start()
    assert flushed.wait(2)
    t, out = _close_in_thread(r)
    t.join(2)
    assert not t.is_alive()
    assert out == [None]
    assert drops == [(["bad"], DropReason.REJECTED), (["next"], DropReason.SHUTDOWN)]


@pytest.mark.filterwarnings("ignore::pytest.PytestUnhandledThreadExceptionWarning")
@pytest.mark.parametrize("overflow", list(OverflowPolicy))
def test_record_after_worker_died_reports_closed(overflow: OverflowPolicy) -> None:
    inner = GatedInner(permits=100)
    inner.fails = 1
    inner.err = HTTPError(400, "bad")
    drops: Drops = []

    def cb(evs: list[Event], reason: DropReason, _x: BaseException | None) -> None:
        drops.append(([e.action for e in evs], reason))
        if reason is DropReason.REJECTED:
            raise _Bail

    r = BufferedRecorder(
        inner, buffer_size=1, flush_size=1, flush_interval=3600.0, overflow=overflow, on_drop=cb
    )
    r.record(Event("bad"))  # the worker takes it and dies
    assert wait_until(lambda: not r._thread.is_alive())
    done = threading.Event()

    def produce() -> None:
        r.record(Event("late"))
        done.set()

    threading.Thread(target=produce, daemon=True).start()
    assert done.wait(2)  # reported at once, under every policy
    assert drops == [(["bad"], DropReason.REJECTED), (["late"], DropReason.CLOSED)]
    r.flush(timeout=1)
    r.close()


@pytest.mark.filterwarnings("ignore::pytest.PytestUnhandledThreadExceptionWarning")
def test_block_producer_parked_when_worker_dies_reports_closed() -> None:
    inner = HungInner()
    inner.after = HTTPError(400, "bad")
    drops: Drops = []

    def cb(evs: list[Event], reason: DropReason, _x: BaseException | None) -> None:
        drops.append(([e.action for e in evs], reason))
        if reason is DropReason.REJECTED:
            raise _Bail

    r = BufferedRecorder(
        inner,
        buffer_size=1,
        flush_size=1,
        flush_interval=3600.0,
        overflow=OverflowPolicy.BLOCK,
        on_drop=cb,
    )
    try:
        r.record(Event("bad"))
        assert inner.entered.wait(2)  # [bad] in flight
        r.record(Event("x"))  # fills the buffer
        done = threading.Event()

        def produce() -> None:
            r.record(Event("late"))
            done.set()

        threading.Thread(target=produce, daemon=True).start()
        # The producer is the only waiter while the worker is in the send.
        assert wait_until(lambda: len(r._cond._waiters) == 1)  # type: ignore[attr-defined]
        inner.release.set()  # 400, on_drop raises, the worker dies
        assert done.wait(2)
        assert wait_until(lambda: len(drops) == 3)
        assert sorted(drops) == sorted(
            [
                (["bad"], DropReason.REJECTED),
                (["x"], DropReason.SHUTDOWN),
                (["late"], DropReason.CLOSED),
            ]
        )
        assert r.stats().pending == 0
        r.close()
    finally:
        inner.release.set()


def test_interrupted_close_can_be_retried(monkeypatch: pytest.MonkeyPatch) -> None:
    inner = GatedInner()
    r = BufferedRecorder(inner, **_QUIET)
    r.record(Event("a"))
    real_join = r._thread.join
    calls = []

    def join(timeout: float | None = None) -> None:
        if not calls:
            calls.append(1)
            raise KeyboardInterrupt
        real_join(timeout)

    monkeypatch.setattr(r._thread, "join", join)
    with pytest.raises(KeyboardInterrupt):
        r.close()
    t, out = _close_in_thread(r)
    assert wait_until(lambda: inner.calls == 1)
    inner.gate.release()
    t.join(3)
    assert not t.is_alive()
    assert out == [None]
    assert _actions(inner) == ["a"]


def test_huge_drain_timeout_is_clamped(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(threading, "TIMEOUT_MAX", 0.3)
    inner = HungInner()
    r = BufferedRecorder(inner, drain_timeout=1e300, **_QUIET)
    try:
        r.record(Event("a"))
        start = time.monotonic()
        t, out = _close_in_thread(r)
        t.join(0.3 + 1.5)
        assert not t.is_alive()
        assert isinstance(out[0], DrainTimeoutError)
        assert time.monotonic() - start < 0.3 + 1.5
    finally:
        inner.release.set()


@pytest.mark.parametrize(("size", "want"), [(float("inf"), [5]), (2.5, [2, 2, 1]), (0.5, [1] * 5)])
def test_float_flush_size_is_clamped(size: float, want: list[int]) -> None:
    inner = GatedInner(permits=100)
    r = BufferedRecorder(inner, flush_size=size, flush_interval=3600.0)  # type: ignore[arg-type]
    try:
        for i in range(5):
            r.record(Event(f"e{i}"))
        r.flush(timeout=2)
        assert inner.sizes() == want
    finally:
        r.close()


def test_nan_flush_timeout_passes_none() -> None:
    seen: list[float | None] = []

    class Inner(FakeInner):
        def record_batch(self, events: Sequence[Event], *, timeout: float | None = None) -> None:
            seen.append(timeout)

    r = BufferedRecorder(Inner(), flush_timeout=float("nan"), **_QUIET)
    try:
        r.record(Event("a"))
        r.flush(timeout=2)
        assert seen == [None]
    finally:
        r.close()


# --- auto-record reporting -------------------------------------------------


Reports = list[tuple[list[str], DropReason, BaseException | None]]


def _collect_err(
    reports: Reports,
) -> Callable[[list[Event], DropReason, BaseException | None], None]:
    lock = threading.Lock()

    def cb(evs: list[Event], reason: DropReason, err: BaseException | None) -> None:
        with lock:
            reports.append(([e.action for e in evs], reason, err))

    return cb


_LOG = logging.getLogger("everscribe.test.autorecord")


def test_auto_record_overflow_error_reports_buffer_full(caplog: pytest.LogCaptureFixture) -> None:
    inner = GatedInner()
    reports: Reports = []
    r = BufferedRecorder(
        inner, buffer_size=1, overflow=OverflowPolicy.ERROR, on_drop=_collect_err(reports), **_QUIET
    )
    try:
        hold_one_in_flight(r, inner)
        r.record(Event("a"))
        with caplog.at_level(logging.ERROR, logger=_LOG.name):
            end_event(Event("auto"), r, _LOG)
        assert len(reports) == 1
        actions, reason, err = reports[0]
        assert actions == ["auto"]
        assert reason is DropReason.BUFFER_FULL
        assert isinstance(err, BufferFullError)
        assert any("everscribe: auto-record failed" in m for m in caplog.messages)
        assert r.stats().dropped == 1
    finally:
        inner.gate.release(10)
        r.close()


def test_direct_record_overflow_error_raises_without_report() -> None:
    inner = GatedInner()
    reports: Reports = []
    r = BufferedRecorder(
        inner, buffer_size=1, overflow=OverflowPolicy.ERROR, on_drop=_collect_err(reports), **_QUIET
    )
    try:
        hold_one_in_flight(r, inner)
        r.record(Event("a"))
        with pytest.raises(BufferFullError):
            r.record(Event("b"))
        assert reports == []
        assert r.stats().dropped == 0
    finally:
        inner.gate.release(10)
        r.close()


def test_auto_record_block_times_out_and_reports(
    monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    monkeypatch.setattr(buffered, "AUTO_RECORD_TIMEOUT", 0.2)
    inner = GatedInner()
    reports: Reports = []
    r = BufferedRecorder(
        inner, buffer_size=1, overflow=OverflowPolicy.BLOCK, on_drop=_collect_err(reports), **_QUIET
    )
    try:
        hold_one_in_flight(r, inner)
        r.record(Event("a"))  # fills the buffer
        done = threading.Event()

        def produce() -> None:
            end_event(Event("auto"), r, _LOG)
            done.set()

        with caplog.at_level(logging.ERROR, logger=_LOG.name):
            threading.Thread(target=produce, daemon=True).start()
            assert done.wait(3)
        assert len(reports) == 1
        actions, reason, err = reports[0]
        assert actions == ["auto"]
        assert reason is DropReason.BUFFER_FULL
        assert isinstance(err, TimeoutError)
        assert any("everscribe: auto-record failed" in m for m in caplog.messages)
        assert len(r._cond._waiters) == 0  # type: ignore[attr-defined]
        inner.gate.release(10)  # space frees; the dropped event must not appear
        r.flush(timeout=2)
        assert _actions(inner) == ["inflight", "a"]
        assert len(reports) == 1
    finally:
        inner.gate.release(10)
        r.close()


def test_auto_record_block_delivers_when_space_frees(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(buffered, "AUTO_RECORD_TIMEOUT", 5.0)
    inner = GatedInner()
    reports: Reports = []
    r = BufferedRecorder(
        inner, buffer_size=1, overflow=OverflowPolicy.BLOCK, on_drop=_collect_err(reports), **_QUIET
    )
    try:
        hold_one_in_flight(r, inner)
        r.record(Event("a"))  # fills the buffer
        done = threading.Event()

        def produce() -> None:
            end_event(Event("auto"), r, _LOG)
            done.set()

        threading.Thread(target=produce, daemon=True).start()
        assert wait_until(lambda: len(r._cond._waiters) == 1)  # type: ignore[attr-defined]
        inner.gate.release(10)
        assert done.wait(3)
        r.flush(timeout=2)
        assert _actions(inner) == ["inflight", "a", "auto"]
        assert reports == []
    finally:
        inner.gate.release(10)
        r.close()
