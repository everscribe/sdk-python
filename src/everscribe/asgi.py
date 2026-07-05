"""ASGI middleware for FastAPI / Starlette (and any ASGI app).

It:

1. Builds an Event template (actor from ``resolve_actor``, origin from the
   request) and installs it in the request-scoped context so handlers can pull
   fresh events with :func:`everscribe.event.from_context`.
2. Installs a status capture so :func:`prepare_event` can auto-populate the
   event's ``result`` from the final HTTP status when the handler hasn't set
   one explicitly.
3. Exposes a single per-request mutable Event via :func:`current_event` (and
   ``request.state.everscribe_event``) for handlers to enrich (action, target,
   metadata, optionally result).
4. If a ``recorder`` was configured, records that event once when the response
   finishes - but only when the handler set ``action`` (empty action is a
   no-op). Auto-record failures are logged, never raised: an audit failure must
   not break the user-facing response.

Mount it AFTER any session/auth middleware, since ``resolve_actor`` typically
reads identity off the request.

Requires Starlette (``pip install "everscribe[fastapi]"``).
"""

from __future__ import annotations

import logging
from contextvars import ContextVar
from typing import Callable, Optional

try:
    from starlette.requests import Request
    from starlette.types import ASGIApp, Message, Receive, Scope, Send
except ImportError as e:  # pragma: no cover - exercised via install extras
    raise ImportError(
        "everscribe.asgi requires Starlette. Install it with: "
        'pip install "everscribe[fastapi]"'
    ) from e

from .event import Event, event_scope, from_context, origin_from_request, prepare_event
from .event.types import Actor, Logger
from .recorder import Recorder

ActorResolver = Callable[[Request], Actor]
"""Derives the :class:`Actor` for a request. Typically reads session/auth
state off the Starlette ``Request``. Must not consume the request body."""

_ANONYMOUS = Actor(type="anonymous")

_current_event: ContextVar[Optional[Event]] = ContextVar(
    "everscribe_current_event", default=None
)


def current_event() -> Event:
    """Return the per-request event installed by :class:`EverscribeMiddleware`
    for handlers to enrich. Outside a request (or when the middleware isn't
    mounted) returns a detached ``Event()`` that is never auto-recorded."""
    e = _current_event.get()
    return e if e is not None else Event()


def event_from_request(request: Request) -> Event:
    """Return the per-request event from ``request.state``, falling back to
    :func:`current_event`."""
    e = getattr(request.state, "everscribe_event", None)
    return e if isinstance(e, Event) else current_event()


class _StatusCapture:
    """Captures the response status from the ASGI ``http.response.start``
    message. Satisfies the :class:`everscribe.event.StatusCapture` protocol."""

    def __init__(self) -> None:
        self._status = 0

    def set_status(self, status: int) -> None:
        self._status = status

    @property
    def status(self) -> int:
        return self._status


class EverscribeMiddleware:
    """ASGI middleware that installs a per-request audit event and (optionally)
    auto-records it on response finish.

    Add it to a Starlette/FastAPI app::

        app.add_middleware(
            EverscribeMiddleware,
            recorder=rec,
            resolve_actor=lambda req: Actor(type="user", id=req.state.user_id),
        )
    """

    def __init__(
        self,
        app: ASGIApp,
        *,
        recorder: "Recorder | None" = None,
        resolve_actor: "ActorResolver | None" = None,
        logger: "Logger | None" = None,
    ) -> None:
        self.app = app
        self.recorder = recorder
        self.resolve_actor: ActorResolver = resolve_actor or (lambda req: _ANONYMOUS)
        self.logger: Logger = logger or logging.getLogger("everscribe.asgi")

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        if scope["type"] != "http":
            await self.app(scope, receive, send)
            return

        request = Request(scope, receive=receive)
        template = Event()
        template.actor = self.resolve_actor(request)
        remote = request.client.host if request.client else ""
        template.origin = origin_from_request(dict(request.headers), remote)

        capture = _StatusCapture()

        async def send_wrapper(message: Message) -> None:
            if message["type"] == "http.response.start":
                capture.set_status(int(message["status"]))
            await send(message)

        with event_scope(template, capture):
            user_event = from_context()
            token = _current_event.set(user_event)
            scope.setdefault("state", {})["everscribe_event"] = user_event
            try:
                await self.app(scope, receive, send_wrapper)
            finally:
                _current_event.reset(token)
                self._auto_record(user_event)

    def _auto_record(self, e: Event) -> None:
        if self.recorder is None or not e.action:
            return
        # prepare_event runs inside the still-active event_scope so the status
        # capture is applied; record here (not the worker) also carries the
        # request context for a synchronous inner recorder.
        prepare_event(e)
        try:
            self.recorder.record(e)
        except Exception as err:  # audit failure must not break the response
            self.logger.error("everscribe: auto-record failed: %s", err)


__all__ = [
    "EverscribeMiddleware",
    "ActorResolver",
    "current_event",
    "event_from_request",
]
