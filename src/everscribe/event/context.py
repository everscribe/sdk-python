"""Request-scoped event context, backed by :mod:`contextvars`.

A framework adapter installs a template Event (and an optional
:class:`StatusCapture`) for the duration of a request; handlers pull a fresh,
independent Event out of it with :func:`from_context`.

``contextvars`` propagate across ``await`` boundaries and into tasks, so this
works unchanged under both sync (WSGI) and async (ASGI) servers.
"""

from __future__ import annotations

from contextlib import contextmanager
from contextvars import ContextVar
from dataclasses import dataclass, replace
from typing import Callable, Iterator, TypeVar

from .event import Event, _new_id, _now_utc
from .types import Result, StatusCapture

T = TypeVar("T")


@dataclass
class _EventContext:
    template: Event
    capture: "StatusCapture | None" = None


_event_context: ContextVar["_EventContext | None"] = ContextVar(
    "everscribe_event_context", default=None
)


def from_context() -> Event:
    """Return a fresh Event pre-populated from the request-scoped template
    installed by :func:`run_with_event` / :func:`event_scope` (typically from
    a middleware adapter).

    Each call returns an independent Event - mutations don't leak across
    events derived from the same context. When called outside any scope (e.g.
    from a background job), returns a minimal ``Event()``.
    """
    ctx = _event_context.get()
    if ctx is None:
        return Event()
    return _clone_template(ctx.template)


@contextmanager
def event_scope(
    template: Event, capture: "StatusCapture | None" = None
) -> Iterator[None]:
    """Context manager that installs ``template`` and ``capture`` for the
    duration of the block. Async adapters use it around the downstream call::

        with event_scope(template, capture):
            await self.app(scope, receive, send)

    (contextvars set before an ``await`` remain visible inside the awaited
    coroutine, so a plain ``with`` is correct here.)
    """
    token = _event_context.set(_EventContext(template=template, capture=capture))
    try:
        yield
    finally:
        _event_context.reset(token)


def run_with_event(
    template: Event, capture: "StatusCapture | None", fn: Callable[[], T]
) -> T:
    """Run ``fn`` with ``template`` and ``capture`` installed in scope, and
    return its result. Async adapters generally prefer
    :func:`event_scope`."""
    with event_scope(template, capture):
        return fn()


def prepare_event(e: Event) -> None:
    """Fill defaults on ``e``: ``id`` if empty and ``result`` auto-populated
    from the in-scope :class:`StatusCapture` when ``result.status`` is unset.
    Recorder implementations call this on each event before persisting so
    handlers can rely on auto-populated fields."""
    if not e.id:
        e.id = _new_id()
    if e.occurred_at is None:  # defensive; the default factory always sets it
        e.occurred_at = _now_utc()
    if not e.result.status:
        ctx = _event_context.get()
        if ctx is not None and ctx.capture is not None:
            e.result = _result_from_capture(ctx.capture)


def _clone_template(tmpl: Event) -> Event:
    clone = Event(action=tmpl.action)  # fresh id + occurred_at
    clone.actor = replace(tmpl.actor)
    clone.tenant_id = tmpl.tenant_id
    clone.target = replace(tmpl.target)
    clone.origin = replace(tmpl.origin)
    clone.result = replace(tmpl.result)
    clone.idempotency_key = tmpl.idempotency_key
    if tmpl.change is not None:
        clone.change = replace(tmpl.change)
    # metadata is deliberately NOT cloned - each event owns its own map.
    return clone


def _result_from_capture(capture: StatusCapture) -> Result:
    """Derive a :class:`Result` from a captured HTTP status. Status 0 (no
    response written) maps to an error - typically an early return or crash
    before any response."""
    status = capture.status
    if status == 0:
        return Result(status="error", message="no response written")
    r = Result(code=status)
    if 200 <= status < 400:
        r.status = "ok"
    elif status in (401, 403):
        r.status = "denied"
    else:
        r.status = "error"
    return r


__all__ = [
    "from_context",
    "run_with_event",
    "event_scope",
    "prepare_event",
]
