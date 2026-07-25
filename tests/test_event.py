"""Tests for the event domain: construction, builders, redaction, and the
on-the-wire serialization format (snake_case with empty fields omitted)."""

from __future__ import annotations

import json
import uuid
from datetime import datetime, timezone

import pytest

from everscribe.event import (
    Actor,
    Change,
    Event,
    Origin,
    Result,
    Target,
    apply_redaction,
    event_to_wire,
    result_to_wire,
    with_redacted_fields,
)


def test_new_event_populates_id_occurred_at_action() -> None:
    e = Event("user.login")
    assert e.action == "user.login"
    # id is a valid uuid4
    parsed = uuid.UUID(e.id)
    assert parsed.version == 4
    assert e.occurred_at.tzinfo is not None
    # default nested structs are empty
    assert e.actor == Actor()
    assert e.target == Target()
    assert e.origin == Origin()
    assert e.result == Result()
    assert e.metadata == {}
    assert e.change is None


def test_empty_event() -> None:
    e = Event()
    assert e.action == ""


def test_each_event_has_independent_metadata() -> None:
    a = Event("a").with_field("k", 1)
    b = Event("b")
    assert a.metadata == {"k": 1}
    assert b.metadata == {}


def test_with_field_chaining() -> None:
    e = Event("x").with_field("a", 1).with_field("b", "two")
    assert e.metadata == {"a": 1, "b": "two"}


def test_with_fields_alternating_pairs() -> None:
    e = Event("x").with_fields("reason", "spam", "severity", "high")
    assert e.metadata == {"reason": "spam", "severity": "high"}


def test_with_fields_drops_trailing_odd_value() -> None:
    e = Event("x").with_fields("a", 1, "b")
    assert e.metadata == {"a": 1}


def test_with_fields_skips_non_string_keys() -> None:
    e = Event("x").with_fields(1, "one", "b", 2)
    assert e.metadata == {"b": 2}


def test_with_fields_empty_is_noop() -> None:
    e = Event("x").with_fields()
    assert e.metadata == {}


# --- wire format -----------------------------------------------------------


def _fixed(e: Event) -> Event:
    """Pin id/occurred_at so wire output is deterministic."""
    e.id = "11111111-1111-4111-8111-111111111111"
    e.occurred_at = datetime(2026, 7, 1, 12, 0, 0, tzinfo=timezone.utc)
    return e


def test_wire_minimal_event() -> None:
    e = _fixed(Event("user.login"))
    assert event_to_wire(e) == {
        "id": "11111111-1111-4111-8111-111111111111",
        "occurred_at": "2026-07-01T12:00:00Z",
        "action": "user.login",
        "actor": {"type": ""},
    }


def test_wire_occurred_at_z_suffix_and_microseconds() -> None:
    e = _fixed(Event("x"))
    e.occurred_at = datetime(2026, 7, 1, 12, 0, 0, 123456, tzinfo=timezone.utc)
    assert event_to_wire(e)["occurred_at"] == "2026-07-01T12:00:00.123456Z"


def test_wire_naive_datetime_assumed_utc() -> None:
    e = _fixed(Event("x"))
    e.occurred_at = datetime(2026, 7, 1, 12, 0, 0)  # noqa: DTZ001 -- naive input is the case under test
    assert event_to_wire(e)["occurred_at"] == "2026-07-01T12:00:00Z"


def test_wire_non_utc_datetime_converted() -> None:
    from datetime import timedelta

    e = _fixed(Event("x"))
    tz = timezone(timedelta(hours=2))
    e.occurred_at = datetime(2026, 7, 1, 14, 0, 0, tzinfo=tz)
    assert event_to_wire(e)["occurred_at"] == "2026-07-01T12:00:00Z"


def test_wire_full_event_snake_case_and_omitempty() -> None:
    e = _fixed(Event("api_key.revoke"))
    e.tenant_id = "tenant-42"
    e.actor = Actor(type="user", id="u1", display_name="Ada", email="ada@x.io")
    e.target = Target(type="api_key", id="k1")
    e.metadata = {"reason": "leaked"}
    e.origin = Origin(ip="1.2.3.4", user_agent="curl/8", request_id="req-9")
    e.result = Result(status="ok", code=200)
    e.idempotency_key = "evt-1"
    wire = event_to_wire(e)
    assert wire == {
        "id": "11111111-1111-4111-8111-111111111111",
        "occurred_at": "2026-07-01T12:00:00Z",
        "action": "api_key.revoke",
        "actor": {
            "type": "user",
            "id": "u1",
            "display_name": "Ada",
            "email": "ada@x.io",
        },
        "tenant_id": "tenant-42",
        "target": {"type": "api_key", "id": "k1"},
        "metadata": {"reason": "leaked"},
        "origin": {"ip": "1.2.3.4", "user_agent": "curl/8", "request_id": "req-9"},
        "result": {"status": "ok", "code": 200},
        "idempotency_key": "evt-1",
    }
    # round-trips through json cleanly
    assert json.loads(json.dumps(wire)) == wire


def test_wire_omits_empty_metadata_target_origin_result() -> None:
    e = _fixed(Event("x"))
    wire = event_to_wire(e)
    assert "metadata" not in wire
    assert "target" not in wire
    assert "origin" not in wire
    assert "result" not in wire
    assert "change" not in wire
    assert "tenant_id" not in wire
    assert "idempotency_key" not in wire


def test_wire_partial_nested_objects() -> None:
    e = _fixed(Event("x"))
    e.target = Target(id="only-id")
    e.origin = Origin(ip="9.9.9.9")
    wire = event_to_wire(e)
    assert wire["target"] == {"id": "only-id"}
    assert wire["origin"] == {"ip": "9.9.9.9"}


def test_wire_result_code_zero_omitted() -> None:
    assert result_to_wire(Result(status="ok", code=0)) == {"status": "ok"}


def test_result_message_exception_rendered_as_string() -> None:
    r = Result(status="error", code=500, message=ValueError("boom"))
    assert result_to_wire(r) == {"status": "error", "code": 500, "message": "boom"}


def test_result_empty_string_message_omitted() -> None:
    assert result_to_wire(Result(status="ok", message="")) == {"status": "ok"}


def test_result_all_empty_returns_none() -> None:
    assert result_to_wire(Result()) is None


def test_result_non_string_message_preserved() -> None:
    assert result_to_wire(Result(message={"detail": "x"})) == {
        "message": {"detail": "x"}
    }


# --- diff / raw_diff / redaction ------------------------------------------


def test_diff_sets_change_before_after() -> None:
    e = Event("user.update").diff({"name": "old"}, {"name": "new"})
    assert e.change == Change(before={"name": "old"}, after={"name": "new"})
    wire = event_to_wire(_fixed(e))
    assert wire["change"] == {"before": {"name": "old"}, "after": {"name": "new"}}


def test_diff_normalizes_via_json() -> None:
    # tuples become lists after the JSON round-trip
    e = Event("x").diff({"tags": ("a", "b")}, {"tags": ("a",)})
    assert e.change is not None
    assert e.change.before == {"tags": ["a", "b"]}


def test_diff_redacts_fields() -> None:
    e = Event("user.update").diff(
        {"email": "a@x.io", "password": "hunter2"},
        {"email": "b@x.io", "password": "hunter3"},
        with_redacted_fields("/password"),
    )
    assert e.change is not None
    assert e.change.before == {"email": "a@x.io", "password": "[REDACTED]"}
    assert e.change.after == {"email": "b@x.io", "password": "[REDACTED]"}


def test_diff_non_serializable_leaves_change_unset() -> None:
    e = Event("x").diff({"ok": 1}, object())
    assert e.change is None


def test_diff_of_none_records_explicit_null() -> None:
    e = Event("x").diff(None, {"a": 1})
    wire = event_to_wire(_fixed(e))
    assert wire["change"] == {"before": None, "after": {"a": 1}}


def test_raw_diff_all_none_is_noop() -> None:
    e = Event("x").raw_diff()
    assert e.change is None


def test_raw_diff_sets_patch_only() -> None:
    patch = [{"op": "replace", "path": "/a", "value": 2}]
    e = Event("x").raw_diff(patch=patch)
    wire = event_to_wire(_fixed(e))
    assert wire["change"] == {"patch": patch}


def test_raw_diff_before_after_patch() -> None:
    e = Event("x").raw_diff(before={"a": 1}, after={"a": 2}, patch=[])
    assert e.change is not None
    assert e.change.before == {"a": 1}
    assert e.change.after == {"a": 2}
    # empty-list patch is not None so it is included
    assert event_to_wire(_fixed(e))["change"]["patch"] == []


# --- apply_redaction directly ---------------------------------------------


def test_apply_redaction_nested_and_array_index() -> None:
    doc = {"user": {"password": "p"}, "keys": ["a", "b"]}
    out = apply_redaction(doc, ["/user/password", "/keys/0"])
    assert out == {"user": {"password": "[REDACTED]"}, "keys": ["[REDACTED]", "b"]}


def test_apply_redaction_missing_path_skipped() -> None:
    out = apply_redaction({"a": 1}, ["/nope", "/a/deep"])
    assert out == {"a": 1}


def test_apply_redaction_out_of_range_index_skipped() -> None:
    out = apply_redaction({"k": ["a"]}, ["/k/5"])
    assert out == {"k": ["a"]}


def test_apply_redaction_empty_pointer_redacts_whole_doc() -> None:
    assert apply_redaction({"a": 1}, [""]) == "[REDACTED]"


def test_apply_redaction_escaped_tokens() -> None:
    # ~1 -> "/", ~0 -> "~"
    doc = {"a/b": {"c~d": "secret"}}
    out = apply_redaction(doc, ["/a~1b/c~0d"])
    assert out == {"a/b": {"c~d": "[REDACTED]"}}


def test_apply_redaction_no_paths_just_normalizes() -> None:
    assert apply_redaction({"t": ("x",)}) == {"t": ["x"]}


def test_apply_redaction_non_serializable_raises() -> None:
    with pytest.raises((TypeError, ValueError)):
        apply_redaction(object())
