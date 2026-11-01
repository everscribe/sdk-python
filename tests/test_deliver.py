"""Tests for deliver: the retry loop and batch splitting."""

from __future__ import annotations

import itertools
import json
import threading
import time
from collections.abc import Callable, Mapping, Sequence
from typing import Any

import pytest

from everscribe.event import Event, prepare_event
from everscribe.recorder import BufferedRecorder, DropReason, HTTPRecorder
from everscribe.recorder.deliver import deliver
from everscribe.recorder.errors import EncodeError, HTTPError, NetworkError

_QUIET: dict[str, Any] = {"flush_size": 10_000, "flush_interval": 3600.0}

# script(call_index, events) returns an exception to raise, or None.
Script = Callable[[int, list[Event]], BaseException | None]


class Clock:
    def __init__(self) -> None:
        self.now = 0.0

    def __call__(self) -> float:
        return self.now

    def sleep(self, s: float) -> None:
        self.now += s


class Inner:
    """Scripted batch recorder logging each call's ids and start time."""

    def __init__(self, clock: Clock, script: Script, cost: Callable[[int], float] | None = None):
        self.clock = clock
        self.script = script
        self.cost = cost
        self.calls: list[tuple[list[str], float]] = []
        self.timeouts: list[float | None] = []
        self.delivered: list[str] = []

    def record_batch(self, events: Sequence[Event], *, timeout: float | None = None) -> None:
        i = len(self.calls)
        self.calls.append(([e.id for e in events], self.clock()))
        self.timeouts.append(timeout)
        if self.cost is not None:
            self.clock.now += self.cost(i)
        err = self.script(i, list(events))
        if err is not None:
            raise err
        self.delivered.extend(e.id for e in events)

    def record(self, e: Event) -> None:
        raise AssertionError("batch path expected")


class Drops:
    def __init__(self) -> None:
        self.calls: list[tuple[list[str], DropReason, BaseException | None]] = []

    def __call__(self, events: list[Event], reason: DropReason, err: BaseException | None) -> None:
        self.calls.append(([e.id for e in events], reason, err))

    def ids(self, reason: DropReason) -> list[str]:
        return [i for ids, r, _ in self.calls if r is reason for i in ids]


def events(n: int, prefix: str = "e") -> list[Event]:
    out = []
    for i in range(n):
        e = Event(f"{prefix}.{i}")
        e.id = f"{prefix}{i}"
        prepare_event(e)
        out.append(e)
    return out


def ident(d: float) -> float:
    return d


def make(inner: Any, drops: Drops | None = None, **kw: Any) -> BufferedRecorder:
    return BufferedRecorder(inner, on_drop=drops, **_QUIET, **kw)


def run(rec: BufferedRecorder, batch: list[Event], clock: Clock, **kw: Any) -> list[Event]:
    cancel = kw.pop("cancel", threading.Event())
    sleep = kw.pop("sleep", clock.sleep)
    return deliver(rec, batch, cancel, clock=clock, sleep=sleep, jitter=ident)


def always(err: Callable[[], BaseException]) -> Script:
    return lambda _i, _e: err()


def first_n(n: int, err: Callable[[], BaseException]) -> Script:
    return lambda i, _e: err() if i < n else None


@pytest.fixture
def clock() -> Clock:
    return Clock()


def test_503_then_ok(clock: Clock) -> None:
    inner = Inner(clock, first_n(1, lambda: HTTPError(503, "")))
    drops = Drops()
    rec = make(inner, drops, flush_timeout=7.0)
    try:
        b = events(3)
        assert run(rec, b, clock) == []
        assert inner.delivered == ["e0", "e1", "e2"]
        assert inner.timeouts == [7.0, 7.0]
        s = rec.stats()
        assert (s.retries, s.flush_errs, s.flushed) == (1, 1, 3)
        assert drops.calls == []
    finally:
        rec.close()


def test_network_error_then_ok(clock: Clock) -> None:
    inner = Inner(clock, first_n(1, lambda: NetworkError("reset")))
    rec = make(inner)
    try:
        assert run(rec, events(2), clock) == []
        assert len(inner.calls) == 2
        assert inner.delivered == ["e0", "e1"]
    finally:
        rec.close()


def test_plain_type_error_not_retried(clock: Clock) -> None:
    inner = Inner(clock, always(lambda: TypeError("bug")))
    drops = Drops()
    rec = make(inner, drops)
    try:
        assert run(rec, events(3), clock) == []
        assert len(inner.calls) == 1
        assert drops.ids(DropReason.REJECTED) == ["e0", "e1", "e2"]
        assert isinstance(drops.calls[0][2], TypeError)
        assert rec.stats().retries == 0
    finally:
        rec.close()


def test_retry_timing(clock: Clock) -> None:
    inner = Inner(clock, always(lambda: HTTPError(503, "")))
    drops = Drops()
    rec = make(inner, drops, initial_backoff=0.5, max_backoff=2.0, max_retry_duration=10.0)
    try:
        assert run(rec, events(2), clock) == []
        assert [t for _, t in inner.calls] == [0, 0.5, 1.5, 3.5, 5.5, 7.5, 9.5, 10]
        assert len(drops.calls) == 1
        ids, reason, err = drops.calls[0]
        assert (ids, reason) == (["e0", "e1"], DropReason.RETRIES_EXHAUSTED)
        assert isinstance(err, HTTPError) and err.status_code == 503
        assert rec.stats().retries == 7
        assert rec.stats().flush_errs == 8
    finally:
        rec.close()


def test_short_max_retry_duration(clock: Clock) -> None:
    inner = Inner(clock, always(lambda: HTTPError(503, "")))
    drops = Drops()
    rec = make(inner, drops, initial_backoff=0.5, max_retry_duration=0.1)
    try:
        run(rec, events(1), clock)
        assert [t for _, t in inner.calls] == [0, pytest.approx(0.1)]
        assert drops.ids(DropReason.RETRIES_EXHAUSTED) == ["e0"]
    finally:
        rec.close()


def test_permanent_500_then_next_batch_delivers(clock: Clock) -> None:
    inner = Inner(clock, lambda _i, ev: HTTPError(500, "") if ev[0].id.startswith("a") else None)
    drops = Drops()
    rec = make(inner, drops, max_retry_duration=5.0)
    try:
        assert run(rec, events(2, "a"), clock) == []
        assert drops.ids(DropReason.RETRIES_EXHAUSTED) == ["a0", "a1"]
        n = len(inner.calls)
        assert run(rec, events(2, "b"), clock) == []
        assert len(inner.calls) == n + 1
        assert inner.delivered == ["b0", "b1"]
    finally:
        rec.close()


def bad_ids(bad: set[str], status: int = 400) -> Script:
    return lambda _i, ev: HTTPError(status, "") if any(e.id in bad for e in ev) else None


def test_one_bad_among_100(clock: Clock) -> None:
    inner = Inner(clock, bad_ids({"e37"}))
    drops = Drops()
    rec = make(inner, drops)
    try:
        b = events(100)
        assert run(rec, b, clock) == []
        assert inner.delivered == [e.id for e in b if e.id != "e37"]
        assert drops.ids(DropReason.REJECTED) == ["e37"]
        assert len(drops.calls) == 1
        assert len(inner.calls) <= 33
        assert rec.stats().retries == 0
    finally:
        rec.close()


@pytest.mark.parametrize(("status", "n", "limit"), [(400, 100, 25), (413, 10, 5)])
def test_oversized_splits_until_it_fits(clock: Clock, status: int, n: int, limit: int) -> None:
    inner = Inner(clock, lambda _i, ev: HTTPError(status, "") if len(ev) > limit else None)
    drops = Drops()
    rec = make(inner, drops)
    try:
        b = events(n)
        assert run(rec, b, clock) == []
        assert inner.delivered == [e.id for e in b]
        assert drops.calls == []
    finally:
        rec.close()


def test_every_batch_rejected_uses_exactly_33_calls(clock: Clock) -> None:
    inner = Inner(clock, always(lambda: HTTPError(400, "bad")))
    drops = Drops()
    rec = make(inner, drops)
    try:
        b = events(100)
        assert run(rec, b, clock) == []
        assert len(inner.calls) == 33
        assert drops.ids(DropReason.REJECTED) == [e.id for e in b]
        assert all(isinstance(err, HTTPError) for _, _, err in drops.calls)
    finally:
        rec.close()


def test_split_half_retry_does_not_spend_budget(clock: Clock) -> None:
    # Call 1 is the first split half; one 503 there costs one extra call only.
    def script(i: int, _ev: list[Event]) -> BaseException | None:
        return HTTPError(503, "") if i == 1 else HTTPError(400, "")

    inner = Inner(clock, script)
    drops = Drops()
    rec = make(inner, drops)
    try:
        b = events(100)
        assert run(rec, b, clock) == []
        assert len(inner.calls) == 34
        assert inner.calls[1][0] == inner.calls[2][0]
        assert drops.ids(DropReason.REJECTED) == [e.id for e in b]
        assert rec.stats().retries == 1
    finally:
        rec.close()


def _split_then_503(i: int, _ev: list[Event]) -> BaseException | None:
    return HTTPError(400, "") if i == 0 else HTTPError(503, "")


def test_split_half_inherits_deadline(clock: Clock) -> None:
    # The rejected original takes 8s, so the first half has 2s left.
    inner = Inner(clock, _split_then_503, cost=lambda i: 8.0 if i == 0 else 0.0)
    drops = Drops()
    rec = make(inner, drops, initial_backoff=0.5, max_backoff=2.0, max_retry_duration=10.0)
    try:
        assert run(rec, events(2), clock) == []
        first = [t for ids, t in inner.calls if ids == ["e0"]]
        assert first == [8, 8.5, 9.5, 10]
        assert drops.ids(DropReason.RETRIES_EXHAUSTED) == ["e0", "e1"]
    finally:
        rec.close()


def test_part_reached_after_deadline_gets_one_call(clock: Clock) -> None:
    inner = Inner(clock, _split_then_503, cost=lambda i: 8.0 if i == 0 else 0.0)
    drops = Drops()
    rec = make(inner, drops, initial_backoff=0.5, max_backoff=2.0, max_retry_duration=10.0)
    try:
        run(rec, events(2), clock)
        second = [t for ids, t in inner.calls if ids == ["e1"]]
        assert second == [10]
        assert drops.calls[-1][:2] == (["e1"], DropReason.RETRIES_EXHAUSTED)
    finally:
        rec.close()


def test_401_drops_whole_batch_in_one_call(clock: Clock) -> None:
    inner = Inner(clock, always(lambda: HTTPError(401, "")))
    drops = Drops()
    rec = make(inner, drops)
    try:
        assert run(rec, events(5), clock) == []
        assert len(inner.calls) == 1
        assert drops.ids(DropReason.REJECTED) == ["e0", "e1", "e2", "e3", "e4"]
    finally:
        rec.close()


def test_real_http_recorder_isolates_unencodable_event(clock: Clock) -> None:
    bodies: list[bytes] = []

    def transport(
        url: str, body: bytes, headers: Mapping[str, str], timeout: float
    ) -> tuple[int, bytes]:
        bodies.append(body)
        return 202, b""

    drops = Drops()
    rec = make(HTTPRecorder("p", "k", transport=transport), drops)
    try:
        b = events(10)
        b[6].metadata["bad"] = object()
        assert run(rec, b, clock) == []
        sent = [w["id"] for body in bodies for w in json.loads(body)["events"]]
        assert sent == [e.id for e in b if e.id != "e6"]
        assert drops.ids(DropReason.REJECTED) == ["e6"]
        assert isinstance(drops.calls[0][2], EncodeError)
    finally:
        rec.close()


class SerialInner:
    def __init__(self, fail: Callable[[str, int], BaseException | None]) -> None:
        self.fail = fail
        self.calls: list[str] = []
        self.delivered: list[str] = []

    def record(self, e: Event) -> None:
        self.calls.append(e.id)
        err = self.fail(e.id, self.calls.count(e.id))
        if err is not None:
            raise err
        self.delivered.append(e.id)


def test_serial_fallback_resends_only_failures(clock: Clock) -> None:
    def fail(id: str, nth: int) -> BaseException | None:
        if id == "e1" and nth == 1:
            return HTTPError(503, "")
        if id == "e3":
            return HTTPError(400, "")
        return None

    inner = SerialInner(fail)
    drops = Drops()
    rec = make(inner, drops)
    try:
        assert run(rec, events(5), clock) == []
        assert inner.calls == ["e0", "e1", "e2", "e3", "e4", "e1"]
        assert inner.delivered == ["e0", "e2", "e4", "e1"]
        assert drops.ids(DropReason.REJECTED) == ["e3"]
        s = rec.stats()
        assert (s.flushed, s.retries) == (4, 1)
    finally:
        rec.close()


def test_ids_identical_across_attempts(clock: Clock) -> None:
    inner = Inner(clock, first_n(2, lambda: NetworkError("x")))
    rec = make(inner)
    try:
        run(rec, events(4), clock)
        assert len(inner.calls) == 3
        assert inner.calls[0][0] == inner.calls[1][0] == inner.calls[2][0]
    finally:
        rec.close()


def test_cancel_mid_backoff_returns_remainder(clock: Clock) -> None:
    # Original rejected and split; the first half's backoff is cancelled.
    inner = Inner(clock, _split_then_503)
    drops = Drops()
    rec = make(inner, drops)
    cancel = threading.Event()

    def sleep(s: float) -> None:
        cancel.set()

    try:
        b = events(4)
        out = run(rec, b, clock, cancel=cancel, sleep=sleep)
        assert [e.id for e in out] == ["e0", "e1", "e2", "e3"]
        assert len(inner.calls) == 2
        assert drops.calls == []
        assert rec.stats().dropped_by_reason.rejected == 0
    finally:
        rec.close()


def test_default_sleep_ends_on_cancel() -> None:
    inner = Inner(Clock(), always(lambda: HTTPError(503, "")))
    drops = Drops()
    rec = make(inner, drops, initial_backoff=60.0, max_backoff=60.0)
    cancel = threading.Event()
    threading.Timer(0.05, cancel.set).start()
    try:
        start = time.monotonic()
        out = deliver(rec, events(2), cancel, jitter=ident)
        assert time.monotonic() - start < 5
        assert [e.id for e in out] == ["e0", "e1"]
        assert drops.calls == []
    finally:
        rec.close()


def test_cancel_set_on_entry_makes_no_call(clock: Clock) -> None:
    inner = Inner(clock, always(lambda: HTTPError(503, "")))
    rec = make(inner)
    cancel = threading.Event()
    cancel.set()
    try:
        b = events(3)
        out = run(rec, b, clock, cancel=cancel)
        assert out == b
        assert inner.calls == []
    finally:
        rec.close()


class OldSignature:
    def __init__(self) -> None:
        self.batches: list[list[str]] = []

    def record_batch(self, events: Sequence[Event]) -> None:
        self.batches.append([e.id for e in events])

    def record(self, e: Event) -> None:
        raise AssertionError("batch path expected")


def test_old_record_batch_signature_still_delivers(clock: Clock) -> None:
    inner = OldSignature()
    drops = Drops()
    rec = make(inner, drops)
    try:
        assert run(rec, events(3), clock) == []
        assert inner.batches == [["e0", "e1", "e2"]]
        assert drops.calls == []
    finally:
        rec.close()


class KwargsBatch:
    def __init__(self) -> None:
        self.kwargs: list[dict[str, Any]] = []

    def record_batch(self, events: Sequence[Event], **kw: Any) -> None:
        self.kwargs.append(kw)

    def record(self, e: Event) -> None:
        raise AssertionError("batch path expected")


def test_kwargs_record_batch_gets_timeout(clock: Clock) -> None:
    inner = KwargsBatch()
    rec = make(inner, flush_timeout=3.0)
    try:
        run(rec, events(1), clock)
        assert inner.kwargs == [{"timeout": 3.0}]
    finally:
        rec.close()


@pytest.mark.parametrize("huge", [float("inf"), 1e308, 10**400])
def test_huge_durations_do_not_raise(clock: Clock, huge: float) -> None:
    inner = Inner(clock, first_n(1, lambda: HTTPError(503, "")))
    rec = make(inner, initial_backoff=huge, max_backoff=huge, max_retry_duration=huge)
    try:
        assert run(rec, events(1), clock) == []
        assert len(inner.calls) == 2
    finally:
        rec.close()


@pytest.mark.parametrize("bad", [None, 0, -1.0, float("nan")])
def test_invalid_durations_use_defaults(clock: Clock, bad: float | None) -> None:
    inner = Inner(clock, always(lambda: HTTPError(503, "")))
    drops = Drops()
    rec = make(inner, drops, initial_backoff=bad, max_backoff=bad, max_retry_duration=bad)
    try:
        run(rec, events(1), clock)
        times = [t for _, t in inner.calls]
        assert times[:3] == [0, 0.5, 1.5]
        assert times[-1] == 300.0
        assert max(b - a for a, b in itertools.pairwise(times)) == 30.0
    finally:
        rec.close()


def test_options_routed_through_factory_and_client() -> None:
    import everscribe
    from everscribe.recorder import new

    opts: dict[str, Any] = {"initial_backoff": 1.0, "max_backoff": 2.0, "max_retry_duration": 3.0}
    for rec in (new("p", "k", **opts), everscribe.new("p", "k").new_recorder(**opts)):
        try:
            got = (rec._initial_backoff, rec._max_backoff, rec._max_retry_duration)
            assert got == (1.0, 2.0, 3.0)
        finally:
            rec.close()


class StrictEvent(threading.Event):
    """Raises like Windows when a wait exceeds threading.TIMEOUT_MAX."""

    def __init__(self) -> None:
        super().__init__()
        self.waits: list[float] = []

    def wait(self, timeout: float | None = None) -> bool:
        if timeout is not None and timeout > threading.TIMEOUT_MAX:
            raise OverflowError("timeout value is too large")
        self.waits.append(timeout or 0.0)
        return super().wait(timeout)


def test_default_sleep_clamps_to_timeout_max(monkeypatch: pytest.MonkeyPatch) -> None:
    from everscribe.recorder.deliver import cancel_sleep

    monkeypatch.setattr(threading, "TIMEOUT_MAX", 0.01)
    cancel = StrictEvent()
    start = time.monotonic()
    cancel_sleep(cancel)(0.05)
    assert time.monotonic() - start >= 0.05
    assert len(cancel.waits) >= 2
    assert all(w <= 0.01 for w in cancel.waits)


def test_default_sleep_clamped_still_ends_on_cancel(monkeypatch: pytest.MonkeyPatch) -> None:
    from everscribe.recorder.deliver import cancel_sleep

    monkeypatch.setattr(threading, "TIMEOUT_MAX", 0.01)
    cancel = StrictEvent()
    threading.Timer(0.05, cancel.set).start()
    start = time.monotonic()
    cancel_sleep(cancel)(1e9)
    assert time.monotonic() - start < 5


@pytest.mark.parametrize(
    ("given", "want"),
    [
        (7.0, 7.0),
        (0.0, 0.0),
        (-1.0, -1.0),
        (float("inf"), "max"),
        (1e308, "max"),
    ],
)
def test_flush_timeout_clamped(clock: Clock, given: float, want: float | str) -> None:
    from everscribe.recorder.buffered import _MAX_DURATION

    inner = Inner(clock, lambda _i, _e: None)
    rec = make(inner, flush_timeout=given)
    try:
        run(rec, events(1), clock)
        cap = min(_MAX_DURATION, threading.TIMEOUT_MAX)
        assert inner.timeouts == [cap if want == "max" else want]
    finally:
        rec.close()


@pytest.mark.parametrize("raises", [False, True], ids=["ok", "raises"])
def test_cancel_during_serial_fallback(clock: Clock, raises: bool) -> None:
    cancel = threading.Event()

    def fail(id: str, _nth: int) -> BaseException | None:
        if id == "e1":
            return HTTPError(503, "")
        if id == "e2":
            cancel.set()
            return HTTPError(400, "") if raises else None
        return None

    inner = SerialInner(fail)
    drops = Drops()
    rec = make(inner, drops)
    try:
        out = run(rec, events(5), clock, cancel=cancel)
        tail = ["e2", "e3", "e4"] if raises else ["e3", "e4"]
        assert [e.id for e in out] == ["e1", *tail]
        assert inner.calls == ["e0", "e1", "e2"]
        assert drops.calls == []
        assert rec.stats().retries == 0
    finally:
        rec.close()
