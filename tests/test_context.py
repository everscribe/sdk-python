"""Tests for request-scoped event context and status->result mapping."""

from __future__ import annotations

import asyncio
from dataclasses import dataclass

from everscribe.event import (
    Actor,
    Event,
    Origin,
    event_scope,
    from_context,
    prepare_event,
    run_with_event,
)


@dataclass
class FakeCapture:
    status: int


def _template() -> Event:
    e = Event()
    e.actor = Actor(type="user", id="u1")
    e.origin = Origin(ip="1.2.3.4", user_agent="curl")
    e.tenant_id = "t1"
    return e


def test_from_context_outside_scope_returns_minimal_event() -> None:
    e = from_context()
    assert e.action == ""
    assert e.actor == Actor()
    assert e.id  # still gets an id


def test_from_context_clones_template_fields() -> None:
    with event_scope(_template()):
        e = from_context()
    assert e.actor == Actor(type="user", id="u1")
    assert e.origin == Origin(ip="1.2.3.4", user_agent="curl")
    assert e.tenant_id == "t1"


def test_from_context_returns_independent_events() -> None:
    with event_scope(_template()):
        a = from_context()
        b = from_context()
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
        e = from_context()
    assert e.metadata == {}


def test_scope_is_reset_on_exit() -> None:
    with event_scope(_template()):
        pass
    assert from_context().actor == Actor()  # back to minimal


def test_run_with_event_callback_form() -> None:
    result = run_with_event(_template(), None, lambda: from_context().actor.id)
    assert result == "u1"


def test_nested_scopes_restore_outer() -> None:
    outer = _template()
    inner = Event()
    inner.actor = Actor(type="admin", id="a1")
    with event_scope(outer):
        assert from_context().actor.id == "u1"
        with event_scope(inner):
            assert from_context().actor.id == "a1"
        assert from_context().actor.id == "u1"


def test_context_propagates_across_await() -> None:
    async def handler() -> str:
        await asyncio.sleep(0)
        return from_context().actor.id

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


def test_prepare_event_status_zero_no_response() -> None:
    e = Event("x")
    with event_scope(_template(), FakeCapture(status=0)):
        prepare_event(e)
    assert e.result.status == "error"
    assert e.result.message == "no response written"
    assert e.result.code == 0


def test_prepare_event_explicit_result_wins_over_capture() -> None:
    e = Event("x")
    e.result = e.result.__class__(status="denied", code=418)
    with event_scope(_template(), FakeCapture(status=200)):
        prepare_event(e)
    assert e.result.status == "denied"
    assert e.result.code == 418
