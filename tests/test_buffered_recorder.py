"""Tests for BufferedRecorder: flush triggers, overflow policies, drain on
close, batch-vs-serial, error propagation, and stats."""

from __future__ import annotations

import threading
import time
from typing import Callable, List, Optional, Sequence

import pytest

from everscribe.event import Event
from everscribe.recorder import BufferedRecorder, OverflowPolicy
from everscribe.recorder.errors import BufferFullError, DrainTimeoutError, HTTPError


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
        self.batches: List[List[Event]] = []
        self.singles: List[Event] = []
        self.fail: Optional[BaseException] = None
        self.block: Optional[threading.Event] = None

    def record_batch(
        self, events: Sequence[Event], *, timeout: "float | None" = None
    ) -> None:
        if self.block is not None:
            self.block.wait(timeout=5)
        if self.fail is not None:
            raise self.fail
        with self.lock:
            self.batches.append(list(events))

    def record(self, e: Event, *, timeout: "float | None" = None) -> None:
        with self.lock:
            self.singles.append(e)

    def all_events(self) -> List[Event]:
        with self.lock:
            out = list(self.singles)
            for b in self.batches:
                out.extend(b)
            return out


class SerialInner:
    """Inner recorder WITHOUT record_batch, to exercise the serial fallback."""

    def __init__(self) -> None:
        self.lock = threading.Lock()
        self.singles: List[Event] = []

    def record(self, e: Event, *, timeout: "float | None" = None) -> None:
        with self.lock:
            self.singles.append(e)


# Large size/interval so the worker only flushes on explicit flush/close;
# used by overflow tests that need events to stay buffered.
_QUIET = dict(flush_size=10_000, flush_interval=3600.0)


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
    inner = FakeInner()
    r = BufferedRecorder(
        inner, buffer_size=1, overflow=OverflowPolicy.DROP_NEWEST, **_QUIET
    )
    try:
        r.record(Event("a"))  # fills buffer
        r.record(Event("b"))  # dropped
        r.record(Event("c"))  # dropped
        assert r.stats().dropped == 2
        assert r.stats().pending == 1
    finally:
        r.close()


def test_overflow_error() -> None:
    inner = FakeInner()
    r = BufferedRecorder(inner, buffer_size=1, overflow=OverflowPolicy.ERROR, **_QUIET)
    try:
        r.record(Event("a"))
        with pytest.raises(BufferFullError):
            r.record(Event("b"))
    finally:
        r.close()


def test_overflow_string_alias_accepted() -> None:
    inner = FakeInner()
    r = BufferedRecorder(inner, buffer_size=1, overflow="error", **_QUIET)  # type: ignore[arg-type]
    try:
        r.record(Event("a"))
        with pytest.raises(BufferFullError):
            r.record(Event("b"))
    finally:
        r.close()


def test_overflow_block_unblocks_on_space() -> None:
    inner = FakeInner()
    r = BufferedRecorder(inner, buffer_size=1, overflow=OverflowPolicy.BLOCK, **_QUIET)
    try:
        r.record(Event("a"))  # fills buffer
        done = threading.Event()

        def producer() -> None:
            r.record(Event("b"))  # blocks until space
            done.set()

        t = threading.Thread(target=producer)
        t.start()
        # still blocked (buffer full, worker not flushing on its own)
        assert not done.wait(timeout=0.2)
        r.flush(timeout=2)  # drains buffer -> frees space
        assert done.wait(timeout=2)
        t.join()
    finally:
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


def test_flush_error_propagates_and_counts() -> None:
    inner = FakeInner()
    inner.fail = HTTPError(500, "boom")
    r = BufferedRecorder(inner, **_QUIET)
    try:
        r.record(Event("a"))
        with pytest.raises(HTTPError):
            r.flush(timeout=2)
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
