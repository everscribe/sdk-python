"""Request-scoped event context and the record lifecycle, backed by
:mod:`contextvars`.

A framework adapter installs a template Event (and an optional
:class:`OutcomeCapture`) for the duration of a request. Handlers pull a
fresh, independent Event out of it with :func:`new_from_context`, or reach
the single request-scoped event with :func:`current_event`. Adapters that
want auto-record support (the single request-scoped event, dedupe, and a
final record-on-exit) use :func:`request_scope` and :func:`end_event`
instead of the lower-level :func:`event_scope`.

This module is the framework-neutral core: it owns everything an adapter
would otherwise have to re-derive (the ``recorded`` dedupe flag, the
idempotency-key stamp, and the rule for when the "no response written"
sentinel may be applied), so a Flask, Django, or gRPC adapter gets the same
lifecycle an ASGI adapter gets, by supplying only transport bindings
(an actor resolver, an origin extractor, and an :class:`OutcomeCapture`).
This mirrors ``pkg/event/lifecycle.go`` in ``sdk-go``, ``begin``/``current``/
``prepareEvent`` in ``sdk-node``, and ``scope``/``current``/``end`` in
``sdk-rust`` - all hard-won designs after real bugs, carried over here
rather than re-derived.

``contextvars`` propagate across ``await`` boundaries and into tasks, so this
works unchanged under both sync (WSGI) and async (ASGI) servers.
"""

from __future__ import annotations

from contextlib import contextmanager
from contextvars import ContextVar
from dataclasses import dataclass, replace
from typing import Callable, Iterator, Protocol, TypeVar, runtime_checkable

from .event import Event, _new_id, _now_utc
from .types import Logger, OutcomeCapture, Result

T = TypeVar("T")


@runtime_checkable
class Recorder(Protocol):
    """Minimal recording sink :func:`end_event` needs.

    Redeclared here rather than imported from ``everscribe.recorder.base``:
    ``recorder/base.py`` imports from this package (``from ..event import
    Event``), so importing the real ``Recorder`` protocol here would create
    a cycle. Both :class:`~everscribe.recorder.HTTPRecorder` and
    :class:`~everscribe.recorder.BufferedRecorder` satisfy this
    structurally.
    """

    def record(self, e: Event) -> None: ...


@dataclass
class _RequestState:
    """Per-request lifecycle state installed by :func:`request_scope`.

    ``recorded`` is state, not inference: both the auto-record path
    (:func:`end_event`) and a handler's own manual record of ``current``
    (via :func:`prepare_event`) check and set this same flag, so a handler
    that sets ``action`` and records ``current`` explicitly cannot also have
    it auto-recorded a second time by the adapter. Inferring dedupe from
    "action is empty" does not work: the handler in that scenario has a
    non-empty action.
    """

    current: Event
    recorded: bool = False


@dataclass
class _EventContext:
    template: Event
    capture: "OutcomeCapture | None" = None
    #: Present only when installed via :func:`request_scope`. :func:`event_scope`
    #: / :func:`run_with_event` callers get template/capture only -
    #: :func:`current_event` and the dedupe mark are no-ops in that scope.
    state: "_RequestState | None" = None


_event_context: ContextVar["_EventContext | None"] = ContextVar(
    "everscribe_event_context", default=None
)


def new_from_context() -> Event:
    """Return a fresh Event pre-populated from the request-scoped template
    installed by :func:`run_with_event` / :func:`event_scope` / :func:`request_scope`
    (typically from a framework adapter).

    Each call returns an independent Event - mutations don't leak across
    events derived from the same context, and the clone never inherits
    ``idempotency_key``: that key is stamped once, by :func:`request_scope`,
    onto the single request-scoped event :func:`current_event` returns. A
    clone sharing it would let the server dedupe distinct events against
    each other.

    When called outside any scope (e.g. from a background job), returns a
    minimal ``Event()``.
    """
    ctx = _event_context.get()
    if ctx is None:
        return Event()
    return _clone_template(ctx.template)


def current_event() -> Event:
    """Return the request-scoped mutable event installed by
    :func:`request_scope` - the event a framework adapter will auto-record
    via :func:`end_event`.

    Framework-neutral: callable with no arguments from anywhere inside a
    request (ASGI, and eventually Flask/Django/gRPC adapters), unlike a
    transport-specific accessor. Handlers recording several events per
    request should call :func:`new_from_context` instead, which returns a
    clone with a fresh id.

    If no adapter installed a lifecycle on this context (no
    :func:`request_scope` has run, or the call happens outside a request
    entirely), returns a throwaway ``Event()`` - harmless to call, but its
    return value is never recorded since nothing owns it.
    """
    ctx = _event_context.get()
    if ctx is None or ctx.state is None:
        return Event()
    return ctx.state.current


@contextmanager
def event_scope(
    template: Event, capture: "OutcomeCapture | None" = None
) -> Iterator[None]:
    """Context manager that installs ``template`` and ``capture`` for the
    duration of the block, with no request-scoped current event or dedupe
    state. Useful for exercising :func:`new_from_context` / :func:`prepare_event`'s
    capture-fill behavior without the full auto-record lifecycle. Adapters
    driving a real request/response should use :func:`request_scope`
    instead::

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
    template: Event, capture: "OutcomeCapture | None", fn: Callable[[], T]
) -> T:
    """Run ``fn`` with ``template`` and ``capture`` installed in scope, and
    return its result. Async adapters generally prefer
    :func:`event_scope` or :func:`request_scope`."""
    with event_scope(template, capture):
        return fn()


@contextmanager
def request_scope(
    template: Event, capture: "OutcomeCapture | None" = None
) -> Iterator[Event]:
    """Install ``template`` and ``capture`` for the duration of the block,
    plus a request-scoped current event that :func:`current_event` returns
    and :func:`end_event` auto-records. Yields that current event.

    The current event is a clone of ``template`` (so ``template`` itself
    stays unstamped - see :func:`new_from_context`), stamped here with
    ``idempotency_key = id``, unconditionally, before the block runs. Both
    the auto-record path (:func:`end_event`) and a handler's own manual
    record of this same event must submit that key so the server's
    duplicate-absorbing constraint collapses a retried submission instead of
    colliding on the events primary key. Stamping later (inside
    :func:`end_event`) would key only the second submission, which dedupes
    nothing. Stamping ``template`` instead would be worse:
    :func:`new_from_context` clones it for every mid-handler event, so a
    handler emitting several events would have them all share one key and
    the server would silently discard all but the first. A handler that
    sets its own ``idempotency_key`` on the current event simply overwrites
    this default, since the handler runs after this stamp.

    Framework adapters (ASGI today; Flask/Django/gRPC later) use this
    instead of the lower-level :func:`event_scope` when they want
    auto-record support: :func:`current_event`, dedupe via the shared
    ``recorded`` flag, and :func:`end_event`.
    """
    current = _clone_template(template)
    current.idempotency_key = current.id
    state = _RequestState(current=current)
    token = _event_context.set(
        _EventContext(template=template, capture=capture, state=state)
    )
    try:
        yield current
    finally:
        _event_context.reset(token)


def prepare_event(e: Event) -> None:
    """Fill defaults on ``e``: ``id`` if empty, ``occurred_at`` if unset, and
    ``result`` auto-populated from the in-scope :class:`OutcomeCapture` when
    ``result.status`` is unset. Recorder implementations call this on each
    event before persisting so handlers can rely on auto-populated fields.

    Result population here is never final: ``prepare_event`` can run
    mid-handler, when a handler records an extra event before the response
    is written. At that moment the capture legitimately reports no outcome
    yet - that does not mean no outcome ever - so unlike :func:`end_event`,
    this leaves ``result`` untouched rather than stamping the "no response
    written" sentinel. Only :func:`end_event` runs after the handler has
    genuinely finished and may apply that sentinel.

    Also has a dedupe side effect, not obvious from the name: when ``e`` is
    the request-scoped event installed by :func:`request_scope` (checked by
    identity, not by id), this marks it recorded so :func:`end_event`'s
    auto-record backstop does not submit it a second time.
    """
    if not e.id:
        e.id = _new_id()
    if e.occurred_at is None:  # defensive; the default factory always sets it
        e.occurred_at = _now_utc()
    _apply_outcome(e, final=False)
    ctx = _event_context.get()
    if ctx is not None and ctx.state is not None and e is ctx.state.current:
        ctx.state.recorded = True


def end_event(
    e: Event, recorder: "Recorder | None", logger: "Logger | None" = None
) -> None:
    """Record ``e`` once, if ``recorder`` is configured and the handler
    named it (non-empty ``action``). Call exactly once, after the handler
    has genuinely finished - the in-scope :class:`OutcomeCapture` is applied
    as final, so a still-unset result gets the "no response written"
    sentinel here (never from :func:`prepare_event`).

    Safe to call even when a handler already recorded ``e`` manually (e.g.
    ``recorder.record(current_event())``): both paths check and set the
    same request-scoped ``recorded`` flag - state, not inferred from
    ``action`` - so whichever submission happens first wins and this call
    becomes a no-op for the loser.

    Record failures are logged via ``logger``, never raised: an audit
    failure must not break the caller's response.
    """
    if recorder is None or not e.action:
        return
    if not _claim_recorded(e):
        return
    if not e.id:
        e.id = _new_id()
    if e.occurred_at is None:  # defensive; the default factory always sets it
        e.occurred_at = _now_utc()
    _apply_outcome(e, final=True)
    try:
        recorder.record(e)
    except Exception as err:  # audit failure must not break the caller
        if logger is not None:
            logger.error("everscribe: auto-record failed: %s", err)


def _claim_recorded(e: Event) -> bool:
    """Claim the request-scoped ``recorded`` flag for ``e``, if ``e`` is the
    tracked current event. Returns ``True`` when the caller should proceed
    to record now: either ``e`` isn't the tracked current event at all (a
    standalone ``Event()``, or a :func:`new_from_context` clone - those have
    no shared dedupe state and are always recordable), or this is the first
    claim on it. Returns ``False`` when ``e`` is the current event and
    something already claimed it (a prior :func:`prepare_event` call or a
    previous :func:`end_event` call), which is the caller's cue to skip
    recording.
    """
    ctx = _event_context.get()
    if ctx is None or ctx.state is None or e is not ctx.state.current:
        return True
    if ctx.state.recorded:
        return False
    ctx.state.recorded = True
    return True


def _clone_template(tmpl: Event) -> Event:
    clone = Event(action=tmpl.action)  # fresh id + occurred_at
    clone.actor = replace(tmpl.actor)
    clone.tenant_id = tmpl.tenant_id
    clone.target = replace(tmpl.target)
    clone.origin = replace(tmpl.origin)
    clone.result = replace(tmpl.result)
    if tmpl.change is not None:
        clone.change = replace(tmpl.change)
    # metadata is deliberately NOT cloned - each event owns its own map.
    # idempotency_key is deliberately NOT copied from tmpl - see
    # new_from_context and request_scope's docstrings for why a clone must
    # stay keyless.
    return clone


def _apply_outcome(e: Event, final: bool) -> None:
    """Fill ``e.result`` from the in-scope :class:`OutcomeCapture` when
    ``e`` doesn't already have one.

    ``final`` distinguishes :func:`end_event` - which runs after the handler
    has genuinely completed and is guaranteed to be the last word on the
    event's outcome - from :func:`prepare_event`, which can run mid-handler.
    Only a final caller may stamp the "no response written" sentinel when
    the capture reports no outcome: from :func:`prepare_event`, no outcome
    yet just means "nothing written yet", not "nothing ever will be", and
    stamping the sentinel there would bake a false error into an event
    recorded before the response.
    """
    if e.result.status:
        return
    ctx = _event_context.get()
    if ctx is None or ctx.capture is None:
        return
    outcome = ctx.capture.outcome
    if outcome is not None:
        e.result = outcome
        return
    if not final:
        return
    # No outcome was produced, and this call is final: keeps the diagnostic
    # explicit in core so no adapter can silently drop it.
    e.result = Result(status="error", message="no response written")


def result_from_http_status(code: int) -> Result:
    """Derive a :class:`Result` from an HTTP status code. Opt-in: core never
    calls this implicitly. Adapters over an HTTP-shaped status call it
    rather than each carrying a copy of the table.

    ``code`` 0 means "no response written" and yields the same diagnostic
    :func:`end_event`'s final outcome-fill produces when the capture never
    reports anything.
    """
    if code == 0:
        return Result(status="error", message="no response written")
    r = Result(code=code)
    if 200 <= code < 400:
        r.status = "ok"
    elif code in (401, 403):
        r.status = "denied"
    else:
        r.status = "error"
    return r


__all__ = [
    "new_from_context",
    "current_event",
    "run_with_event",
    "event_scope",
    "request_scope",
    "prepare_event",
    "end_event",
    "result_from_http_status",
]
