"""Tests for DropReason, on_drop, and per-reason drop counters."""

from __future__ import annotations

import logging
import threading
import time
from collections.abc import Mapping, Sequence

import pytest

import everscribe
from everscribe.event import Event
from everscribe.recorder import (
    BufferedRecorder,
    DroppedByReason,
    DropReason,
    OnDrop,
    OverflowPolicy,
)
from everscribe.recorder import new as new_recorder
from everscribe.recorder.http import Transport

_QUIET = {"flush_size": 10_000, "flush_interval": 3600.0}


class Inner:
    def __init__(self, gated: bool = False) -> None:
        self.lock = threading.Lock()
        self.events: list[Event] = []
        self.calls = 0
        # When gated, each batch waits for a permit so one stays in flight.
        self.gate = threading.Semaphore(0) if gated else None

    def record_batch(self, events: Sequence[Event], *, timeout: float | None = None) -> None:
        with self.lock:
            self.calls += 1
        if self.gate is not None:
            assert self.gate.acquire(timeout=5)
        with self.lock:
            self.events.extend(events)

    def hold(self, r: BufferedRecorder) -> None:
        """Park one batch in flight so the buffer itself can fill."""
        r.record(Event("inflight"))
        deadline = time.monotonic() + 2
        while self.calls < 1 and time.monotonic() < deadline:
            time.sleep(0.005)
        assert self.calls == 1

    def open(self) -> None:
        if self.gate is not None:
            self.gate.release(1000)

    def record(self, e: Event, *, timeout: float | None = None) -> None:
        with self.lock:
            self.events.append(e)


def test_reason_values() -> None:
    assert [r.value for r in DropReason] == [
        "buffer_full",
        "rejected",
        "retries_exhausted",
        "shutdown",
        "closed",
    ]


def test_on_drop_receives_new_list_and_reason() -> None:
    calls: list[tuple[list[Event], DropReason, BaseException | None]] = []
    inner = Inner(gated=True)
    r = BufferedRecorder(
        inner,
        buffer_size=1,
        overflow=OverflowPolicy.DROP_NEWEST,
        on_drop=lambda evs, reason, err: calls.append((evs, reason, err)),
        **_QUIET,
    )
    try:
        inner.hold(r)
        r.record(Event("a"))
        r.record(Event("b"))
    finally:
        inner.open()
        r.close()
    assert len(calls) == 1
    evs, reason, err = calls[0]
    assert reason is DropReason.BUFFER_FULL
    assert err is None
    assert [e.action for e in evs] == ["b"]


def test_report_drop_passes_a_new_list() -> None:
    seen: list[list[Event]] = []
    r = BufferedRecorder(Inner(), on_drop=lambda evs, _r, _e: seen.append(evs), **_QUIET)
    try:
        mine = [Event("x")]
        r._report_drop(mine, DropReason.REJECTED, RuntimeError("boom"))
        assert seen[0] == mine
        assert seen[0] is not mine
    finally:
        r.close()


def test_raising_callback_is_logged_and_delivery_continues(
    caplog: pytest.LogCaptureFixture,
) -> None:
    def boom(_e: list[Event], _r: DropReason, _x: BaseException | None) -> None:
        raise RuntimeError("callback bug")

    inner = Inner(gated=True)
    r = BufferedRecorder(
        inner,
        buffer_size=1,
        on_drop=boom,
        logger=logging.getLogger("test.drop"),
        **_QUIET,
    )
    with caplog.at_level(logging.WARNING, logger="test.drop"):
        inner.hold(r)
        r.record(Event("a"))
        r.record(Event("b"))  # dropped, callback raises
        inner.open()
        r.flush()
        r.record(Event("c"))
        r.close()
    assert [e.action for e in inner.events] == ["inflight", "a", "c"]
    assert any("on_drop" in rec.getMessage() for rec in caplog.records)


def test_base_exception_from_callback_propagates() -> None:
    def bail(_e: list[Event], _r: DropReason, _x: BaseException | None) -> None:
        raise KeyboardInterrupt

    r = BufferedRecorder(Inner(), on_drop=bail, **_QUIET)
    try:
        with pytest.raises(KeyboardInterrupt):
            r._report_drop([Event("a")], DropReason.CLOSED)
    finally:
        r.close()


def test_every_reason_counts_without_callback() -> None:
    r = BufferedRecorder(Inner(), **_QUIET)
    try:
        for reason in DropReason:
            r._report_drop([Event("a"), Event("b")], reason)
        s = r.stats()
    finally:
        r.close()
    assert s.dropped_by_reason == DroppedByReason(2, 2, 2, 2, 2)
    assert s.dropped == s.dropped_by_reason.buffer_full == 2
    assert s.retries == 0


def test_report_drop_returns_buffer_full_total() -> None:
    r = BufferedRecorder(Inner(), **_QUIET)
    try:
        assert r._report_drop([Event("a")], DropReason.BUFFER_FULL) == 1
        assert r._report_drop([Event("a")], DropReason.REJECTED) == 1
        assert r._report_drop([Event("a")], DropReason.BUFFER_FULL) == 2
    finally:
        r.close()


def test_drop_newest_warns_first_and_every_1000th(caplog: pytest.LogCaptureFixture) -> None:
    reasons: list[DropReason] = []
    inner = Inner(gated=True)
    r = BufferedRecorder(
        inner,
        buffer_size=1,
        on_drop=lambda _e, reason, _x: reasons.append(reason),
        logger=logging.getLogger("test.drop2"),
        **_QUIET,
    )
    with caplog.at_level(logging.WARNING, logger="test.drop2"):
        inner.hold(r)
        r.record(Event("fill"))
        for _ in range(2000):
            r.record(Event("x"))
    s = r.stats()
    inner.open()
    r.close()
    assert s.dropped == 2000
    assert set(reasons) == {DropReason.BUFFER_FULL}
    warns = [rec for rec in caplog.records if "buffer full" in rec.getMessage()]
    assert len(warns) == 3  # 1st, 1000th, 2000th
    assert "dropped_total=1 " in warns[0].getMessage()


def _gated_transport(gate: threading.Semaphore, sent: threading.Event) -> Transport:
    def transport(
        url: str, body: bytes, headers: Mapping[str, str], timeout: float
    ) -> tuple[int, bytes]:
        sent.set()
        assert gate.acquire(timeout=5)
        return 200, b"{}"

    return transport


def test_on_drop_routed_through_factory_and_client() -> None:
    builders = (
        lambda t, cb: new_recorder("p", "k", transport=t, buffer_size=1, on_drop=cb, **_QUIET),
        lambda t, cb: everscribe.new("p", "k").new_recorder(
            transport=t, buffer_size=1, on_drop=cb, **_QUIET
        ),
    )
    for build in builders:
        # A fresh gate per run, so no permits leak into the next one.
        gate = threading.Semaphore(0)
        sent = threading.Event()
        seen: list[DropReason] = []
        r = build(_gated_transport(gate, sent), seen_append(seen))
        try:
            r.record(Event("inflight"))
            assert sent.wait(2)
            r.record(Event("a"))
            r.record(Event("b"))
        finally:
            gate.release(2)  # "inflight" and "a"
            r.close()
        assert seen == [DropReason.BUFFER_FULL]


def seen_append(seen: list[DropReason]) -> OnDrop:
    return lambda _e, reason, _x: seen.append(reason)


def test_manual_record_after_close_then_end_event_reports_once() -> None:
    from everscribe.event import end_event
    from everscribe.event.context import request_scope

    seen: list[DropReason] = []
    r = BufferedRecorder(Inner(), on_drop=seen_append(seen), **_QUIET)
    r.close()
    with request_scope(Event("req")) as e:
        r.record(e)
        end_event(e, r)
    assert seen == [DropReason.CLOSED]
    assert r.stats().dropped_by_reason.closed == 1


def test_block_producer_woken_by_close_reports_closed_once() -> None:
    seen: list[DropReason] = []
    inner = Inner(gated=True)
    r = BufferedRecorder(
        inner,
        buffer_size=1,
        overflow=OverflowPolicy.BLOCK,
        on_drop=seen_append(seen),
        **_QUIET,
    )
    inner.hold(r)
    r.record(Event("fill"))
    t = threading.Thread(target=r.record, args=(Event("blocked"),))
    t.start()
    time.sleep(0.1)
    closer = threading.Thread(target=r.close)
    closer.start()
    t.join(timeout=2)
    assert not t.is_alive()
    inner.open()
    closer.join(timeout=2)
    assert not closer.is_alive()
    assert seen == [DropReason.CLOSED]
    assert r.stats().dropped_by_reason.closed == 1
