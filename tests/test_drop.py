"""Tests for DropReason, on_drop, and per-reason drop counters."""

from __future__ import annotations

import logging
import threading
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

_QUIET = {"flush_size": 10_000, "flush_interval": 3600.0}


class Inner:
    def __init__(self) -> None:
        self.lock = threading.Lock()
        self.events: list[Event] = []

    def record_batch(self, events: Sequence[Event], *, timeout: float | None = None) -> None:
        with self.lock:
            self.events.extend(events)

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
    r = BufferedRecorder(
        Inner(),
        buffer_size=1,
        overflow=OverflowPolicy.DROP_NEWEST,
        on_drop=lambda evs, reason, err: calls.append((evs, reason, err)),
        **_QUIET,
    )
    try:
        r.record(Event("a"))
        r.record(Event("b"))
    finally:
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

    inner = Inner()
    r = BufferedRecorder(
        inner,
        buffer_size=1,
        on_drop=boom,
        logger=logging.getLogger("test.drop"),
        **_QUIET,
    )
    with caplog.at_level(logging.WARNING, logger="test.drop"):
        r.record(Event("a"))
        r.record(Event("b"))  # dropped, callback raises
        r.flush()
        r.record(Event("c"))
        r.close()
    assert [e.action for e in inner.events] == ["a", "c"]
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
    r = BufferedRecorder(
        Inner(),
        buffer_size=1,
        on_drop=lambda _e, reason, _x: reasons.append(reason),
        logger=logging.getLogger("test.drop2"),
        **_QUIET,
    )
    with caplog.at_level(logging.WARNING, logger="test.drop2"):
        r.record(Event("fill"))
        for _ in range(2000):
            r.record(Event("x"))
    s = r.stats()
    r.close()
    assert s.dropped == 2000
    assert set(reasons) == {DropReason.BUFFER_FULL}
    warns = [rec for rec in caplog.records if "buffer full" in rec.getMessage()]
    assert len(warns) == 3  # 1st, 1000th, 2000th
    assert "dropped_total=1 " in warns[0].getMessage()


def test_on_drop_routed_through_factory_and_client() -> None:
    def transport(
        url: str, body: bytes, headers: Mapping[str, str], timeout: float
    ) -> tuple[int, bytes]:
        return 200, b"{}"

    builders = (
        lambda cb: new_recorder("p", "k", transport=transport, buffer_size=1, on_drop=cb, **_QUIET),
        lambda cb: everscribe.new("p", "k").new_recorder(
            transport=transport, buffer_size=1, on_drop=cb, **_QUIET
        ),
    )
    for build in builders:
        seen: list[DropReason] = []
        r = build(seen_append(seen))
        try:
            r.record(Event("a"))
            r.record(Event("b"))
        finally:
            r.close()
        assert seen == [DropReason.BUFFER_FULL]


def seen_append(seen: list[DropReason]) -> OnDrop:
    return lambda _e, reason, _x: seen.append(reason)
