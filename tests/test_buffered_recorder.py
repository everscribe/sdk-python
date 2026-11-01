"""Tests for BufferedRecorder: flush triggers, overflow policies, drain on
close, batch-vs-serial, error propagation, and stats."""

from __future__ import annotations

import threading
import time
from collections.abc import Callable, Sequence

import pytest

from everscribe.event import Event
from everscribe.recorder import BufferedRecorder, DropReason, OverflowPolicy
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


def test_record_after_close_is_noop() -> None:
    inner = FakeInner()
    r = BufferedRecorder(inner, **_QUIET)
    r.close()
    r.record(Event("a"))
    assert inner.all_events() == []


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
    with pytest.raises(DrainTimeoutError):
        r.close()
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
    r = BufferedRecorder(inner, flush_size=100, flush_interval=0.4)
    try:
        for i in range(150):
            r.record(Event(f"e{i}"))
        assert wait_until(lambda: inner.sizes() == [100], timeout=0.3)
        time.sleep(0.05)
        assert inner.sizes() == [100]
        assert wait_until(lambda: inner.sizes() == [100, 50])
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
    r = BufferedRecorder(inner, flush_size=10_000, flush_interval=0.3)
    try:
        r.record(Event("a"))
        assert wait_until(lambda: inner.calls == 1)  # [a] in flight
        r.record(Event("b"))
        time.sleep(0.5)  # the next interval elapses mid-send
        inner.gate.release(2)
        assert wait_until(lambda: _actions(inner) == ["a", "b"], timeout=0.15)
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
