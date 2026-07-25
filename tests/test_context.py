"""Tests for request-scoped event context and outcome capture -> result
mapping."""

from __future__ import annotations

import asyncio
from dataclasses import dataclass

from everscribe.event import (
    Actor,
    Event,
    Origin,
    Result,
    current_event,
    end_event,
    event_scope,
    new_from_context,
    prepare_event,
    request_scope,
    result_from_http_status,
    run_with_event,
)


@dataclass
class FakeCapture:
    """Test double for OutcomeCapture built on the old status-int shape, so
    existing HTTP-status-mapping tests read unchanged; translates through
    result_from_http_status the same way a real HTTP adapter's capture
    would."""

    status: int

    @property
    def outcome(self) -> Result | None:
        if self.status == 0:
            return None
        return result_from_http_status(self.status)


def _template() -> Event:
    e = Event()
    e.actor = Actor(type="user", id="u1")
    e.origin = Origin(ip="1.2.3.4", user_agent="curl")
    e.tenant_id = "t1"
    return e


def test_new_from_context_outside_scope_returns_minimal_event() -> None:
    e = new_from_context()
    assert e.action == ""
    assert e.actor == Actor()
    assert e.id  # still gets an id


def test_new_from_context_clones_template_fields() -> None:
    with event_scope(_template()):
        e = new_from_context()
    assert e.actor == Actor(type="user", id="u1")
    assert e.origin == Origin(ip="1.2.3.4", user_agent="curl")
    assert e.tenant_id == "t1"


def test_new_from_context_returns_independent_events() -> None:
    with event_scope(_template()):
        a = new_from_context()
        b = new_from_context()
        assert a is not b
        assert a.id != b.id
        a.actor.id = "mutated"
        a.with_field("k", 1)
        # b is unaffected by mutations to a
        assert b.actor.id == "u1"
        assert b.metadata == {}


def test_metadata_not_shared_from_template() -> None:
    tmpl = _template()
    tmpl.with_field("shared", "no")
    with event_scope(tmpl):
        e = new_from_context()
    assert e.metadata == {}


def test_scope_is_reset_on_exit() -> None:
    with event_scope(_template()):
        pass
    assert new_from_context().actor == Actor()  # back to minimal


def test_run_with_event_callback_form() -> None:
    result = run_with_event(_template(), None, lambda: new_from_context().actor.id)
    assert result == "u1"


def test_nested_scopes_restore_outer() -> None:
    outer = _template()
    inner = Event()
    inner.actor = Actor(type="admin", id="a1")
    with event_scope(outer):
        assert new_from_context().actor.id == "u1"
        with event_scope(inner):
            assert new_from_context().actor.id == "a1"
        assert new_from_context().actor.id == "u1"


def test_context_propagates_across_await() -> None:
    async def handler() -> str:
        await asyncio.sleep(0)
        return new_from_context().actor.id

    async def main() -> str:
        with event_scope(_template()):
            return await handler()

    assert asyncio.run(main()) == "u1"


# --- prepare_event ---------------------------------------------------------


def test_prepare_event_fills_missing_id() -> None:
    e = Event()
    e.id = ""
    prepare_event(e)
    assert e.id


def test_prepare_event_no_capture_leaves_result_empty() -> None:
    e = Event("x")
    prepare_event(e)
    assert e.result.status == ""


def test_prepare_event_auto_result_from_capture_ok() -> None:
    e = Event("x")
    with event_scope(_template(), FakeCapture(status=200)):
        prepare_event(e)
    assert e.result.status == "ok"
    assert e.result.code == 200


def test_prepare_event_status_3xx_is_ok() -> None:
    e = Event("x")
    with event_scope(_template(), FakeCapture(status=302)):
        prepare_event(e)
    assert e.result.status == "ok"
    assert e.result.code == 302


def test_prepare_event_status_401_403_denied() -> None:
    for code in (401, 403):
        e = Event("x")
        with event_scope(_template(), FakeCapture(status=code)):
            prepare_event(e)
        assert e.result.status == "denied"
        assert e.result.code == code


def test_prepare_event_status_500_error() -> None:
    e = Event("x")
    with event_scope(_template(), FakeCapture(status=500)):
        prepare_event(e)
    assert e.result.status == "error"
    assert e.result.code == 500


def test_prepare_event_no_outcome_yet_leaves_result_untouched() -> None:
    # Invariant 5: prepare_event is never final. A capture reporting no
    # outcome yet (status 0, i.e. outcome is None) means "nothing written
    # yet", not "nothing ever will be" - prepare_event must not bake the
    # "no response written" sentinel into an event recorded mid-handler.
    # Only end_event (the final path) may do that; see
    # test_end_event_no_outcome_applies_no_response_written_sentinel below.
    e = Event("x")
    with event_scope(_template(), FakeCapture(status=0)):
        prepare_event(e)
    assert e.result.status == ""
    assert e.result.message is None


def test_prepare_event_explicit_result_wins_over_capture() -> None:
    e = Event("x")
    e.result = e.result.__class__(status="denied", code=418)
    with event_scope(_template(), FakeCapture(status=200)):
        prepare_event(e)
    assert e.result.status == "denied"
    assert e.result.code == 418


# --- request_scope / end_event (record lifecycle) --------------------------


class FakeRecorder:
    def __init__(self) -> None:
        self.events: list[Event] = []

    def record(self, e: Event) -> None:
        self.events.append(e)


def test_end_event_no_outcome_applies_no_response_written_sentinel() -> None:
    # Invariant 5, the final half: end_event runs after the handler has
    # genuinely finished, so a capture that never reports an outcome (status
    # 0 the whole time) means the sentinel belongs here.
    rec = FakeRecorder()
    with request_scope(_template(), FakeCapture(status=0)) as e:
        e.action = "x"
        end_event(e, rec)
    assert len(rec.events) == 1
    assert rec.events[0].result.status == "error"
    assert rec.events[0].result.message == "no response written"
    assert rec.events[0].result.code == 0


def test_current_event_outside_scope_returns_detached_event() -> None:
    assert current_event().action == ""
    assert current_event().actor == Actor()


def test_current_event_is_the_request_scope_yielded_event() -> None:
    # Invariant 2: current_event() is framework-neutral - it reads the
    # ContextVar directly, not any transport-specific accessor - so it
    # returns the exact same event object request_scope installed.
    with request_scope(_template()) as current:
        assert current_event() is current
        current_event().action = "seen-via-current-event"
        assert current.action == "seen-via-current-event"


def test_request_scope_stamps_idempotency_key_on_current_not_template() -> None:
    # Invariant 4: the key is stamped on the request-scoped current event at
    # begin, unconditionally, and the template it was cloned from must stay
    # unstamped - both submission paths (manual record + end_event's
    # auto-record) need the same key so the server's duplicate-absorbing
    # constraint dedupes a retried submission instead of colliding on the
    # events primary key.
    tmpl = _template()
    assert tmpl.idempotency_key == ""
    with request_scope(tmpl) as current:
        assert current.idempotency_key != ""
        assert current.idempotency_key == current.id
        # The template itself is never stamped.
        assert tmpl.idempotency_key == ""


def test_new_from_context_clone_has_no_idempotency_key() -> None:
    # Invariant 6: new_from_context clones stay keyless and get fresh ids,
    # even though the request-scoped current event (from request_scope) is
    # stamped. A clone that inherited the key would let the server dedupe
    # unrelated events emitted by a multi-event handler against each other.
    with request_scope(_template()) as current:
        assert current.idempotency_key != ""
        clone = new_from_context()
        assert clone.idempotency_key == ""
        assert clone.id != current.id


def test_dedupe_manual_record_then_end_event_is_noop() -> None:
    # Invariant 3: dedupe is state, not inference from "action is empty". A
    # handler that sets action AND records the current event itself must not
    # also have it auto-recorded by end_event. prepare_event (which every
    # bundled Recorder calls internally before persisting) marks the
    # request-scoped event recorded as a side effect; end_event checks that
    # same flag.
    rec = FakeRecorder()
    with request_scope(_template()) as current:
        current.action = "manual.action"
        prepare_event(current)  # what a Recorder.record() does internally
        rec.record(current)  # the handler's own manual submission
        end_event(current, rec)  # the adapter's auto-record backstop
    assert len(rec.events) == 1


def test_dedupe_end_event_called_twice_records_once() -> None:
    # A second end_event call (e.g. a defensive double-invocation from an
    # adapter) must not resubmit either - same recorded flag, same claim.
    rec = FakeRecorder()
    with request_scope(_template()) as current:
        current.action = "x"
        end_event(current, rec)
        end_event(current, rec)
    assert len(rec.events) == 1


def test_dedupe_is_per_request_not_global() -> None:
    # The recorded flag lives in request-scoped state, so two requests
    # (two request_scope blocks) do not dedupe against each other.
    rec = FakeRecorder()
    with request_scope(_template()) as first:
        first.action = "x"
        end_event(first, rec)
    with request_scope(_template()) as second:
        second.action = "x"
        end_event(second, rec)
    assert len(rec.events) == 2
