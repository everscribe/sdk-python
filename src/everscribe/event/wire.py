"""Serializes events to their on-the-wire JSON shape.

Field names are snake_case and empty fields are omitted, producing the
canonical audit-log wire format.
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

from .types import UNSET, Actor, Change, Origin, Result, Target

# Imported for typing only; avoids a runtime import cycle with event.py.
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from .event import Event


def _iso(dt: datetime) -> str:
    """Format a datetime as RFC 3339 / ISO 8601 with a ``Z`` UTC suffix.
    Naive datetimes are assumed to be UTC."""
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    else:
        dt = dt.astimezone(timezone.utc)
    s = dt.isoformat()
    if s.endswith("+00:00"):
        s = s[:-6] + "Z"
    return s


def event_to_wire(e: "Event") -> dict[str, Any]:
    """Serialize an :class:`Event` to its wire shape (a plain dict ready for
    ``json.dumps``)."""
    wire: dict[str, Any] = {
        "id": e.id,
        "occurred_at": _iso(e.occurred_at),
        "action": e.action,
        "actor": _actor_to_wire(e.actor),
    }
    if e.tenant_id:
        wire["tenant_id"] = e.tenant_id
    target = _target_to_wire(e.target)
    if target:
        wire["target"] = target
    if e.metadata:
        wire["metadata"] = e.metadata
    origin = _origin_to_wire(e.origin)
    if origin:
        wire["origin"] = origin
    result = result_to_wire(e.result)
    if result:
        wire["result"] = result
    if e.change is not None:
        change = _change_to_wire(e.change)
        if change:
            wire["change"] = change
    if e.idempotency_key:
        wire["idempotency_key"] = e.idempotency_key
    return wire


def _actor_to_wire(a: Actor) -> dict[str, Any]:
    # `type` is always present; other fields are omitted when empty.
    w: dict[str, Any] = {"type": a.type}
    if a.id:
        w["id"] = a.id
    if a.display_name:
        w["display_name"] = a.display_name
    if a.email:
        w["email"] = a.email
    return w


def _target_to_wire(t: Target) -> "dict[str, Any] | None":
    w: dict[str, Any] = {}
    if t.type:
        w["type"] = t.type
    if t.id:
        w["id"] = t.id
    return w or None


def _origin_to_wire(o: Origin) -> "dict[str, Any] | None":
    w: dict[str, Any] = {}
    if o.ip:
        w["ip"] = o.ip
    if o.user_agent:
        w["user_agent"] = o.user_agent
    if o.request_id:
        w["request_id"] = o.request_id
    return w or None


def result_to_wire(r: Result) -> "dict[str, Any] | None":
    """Serializes a ``Result``: ``Exception`` -> ``.message``, empty-string
    message omitted, all-empty Result returns ``None`` (omitted by the
    caller)."""
    w: dict[str, Any] = {}
    if r.status:
        w["status"] = r.status
    if r.code:
        w["code"] = r.code
    message = r.message
    if isinstance(message, BaseException):
        message = str(message)
    if isinstance(message, str) and message == "":
        message = None
    if message is not None:
        w["message"] = message
    return w or None


def _change_to_wire(c: Change) -> "dict[str, Any] | None":
    w: dict[str, Any] = {}
    if c.before is not UNSET:
        w["before"] = c.before
    if c.after is not UNSET:
        w["after"] = c.after
    if c.patch is not UNSET:
        w["patch"] = c.patch
    return w or None


__all__ = ["event_to_wire", "result_to_wire"]
