"""ASGI middleware for FastAPI / Starlette (and any ASGI app).

This adapter supplies only ASGI-specific transport bindings; the record
lifecycle itself (the request-scoped current event, dedupe, idempotency-key
stamping, and the final-outcome rule) lives in :mod:`everscribe.event` so
other framework adapters (Flask, Django, gRPC) can reuse it identically.

It:

1. Builds an Event template (actor from ``resolve_actor``, origin from the
   request) and installs it, via :func:`everscribe.event.request_scope`, in
   the request-scoped context so handlers can pull fresh events with
   :func:`everscribe.event.new_from_context`.
2. Installs a status capture so :func:`everscribe.event.prepare_event` can
   auto-populate the event's ``result`` from the final HTTP status when the
   handler hasn't set one explicitly.
3. Exposes a single per-request mutable Event via :func:`everscribe.event.current_event`
   (re-exported here as :func:`current_event`, and available at
   ``request.state.everscribe_event``) for handlers to enrich (action,
   target, metadata, optionally result).
4. If a ``recorder`` was configured, records that event once when the response
   finishes via :func:`everscribe.event.end_event` - but only when the handler
   set ``action`` (empty action is a no-op), and only if nothing already
   claimed the dedupe flag. Auto-record failures are logged, never raised: an
   audit failure must not break the user-facing response.

Mount it AFTER any session/auth middleware, since ``resolve_actor`` typically
reads identity off the request.

Requires Starlette (``pip install "everscribe[fastapi]"``).
"""

from __future__ import annotations

import logging
from typing import Callable

try:
    from starlette.requests import Request
    from starlette.types import ASGIApp, Message, Receive, Scope, Send
except ImportError as e:  # pragma: no cover - exercised via install extras
    raise ImportError(
        "everscribe.asgi requires Starlette. Install it with: "
        'pip install "everscribe[fastapi]"'
    ) from e

from .event import (
    Event,
    current_event,
    end_event,
    origin_from_request,
    request_scope,
    result_from_http_status,
)
from .event.types import Actor, Logger, Result
from .recorder import Recorder

ActorResolver = Callable[[Request], Actor]
"""Derives the :class:`Actor` for a request. Typically reads session/auth
state off the Starlette ``Request``. Must not consume the request body."""

_ANONYMOUS = Actor(type="anonymous")


def event_from_request(request: Request) -> Event:
    """Return the per-request event from ``request.state``, falling back to
    :func:`current_event`."""
    e = getattr(request.state, "everscribe_event", None)
    return e if isinstance(e, Event) else current_event()


class _StatusCapture:
    """Captures the response status from the ASGI ``http.response.start``
    message and translates it into a :class:`Result` on demand, satisfying
    :class:`everscribe.event.OutcomeCapture`.

    Status 0 (nothing written yet) maps to ``outcome`` being ``None`` -
    :func:`result_from_http_status` is only consulted once a status has
    actually been captured.
    """

    def __init__(self) -> None:
        self._status = 0

    def set_status(self, status: int) -> None:
        self._status = status

    @property
    def outcome(self) -> "Result | None":
        if self._status == 0:
            return None
        return result_from_http_status(self._status)


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

        with request_scope(template, capture) as user_event:
            scope.setdefault("state", {})["everscribe_event"] = user_event
            try:
                await self.app(scope, receive, send_wrapper)
            finally:
                # end_event applies the final outcome (including the "no
                # response written" sentinel) and records user_event once,
                # skipping it if a handler already recorded it manually.
                end_event(user_event, self.recorder, self.logger)


__all__ = [
    "EverscribeMiddleware",
    "ActorResolver",
    "current_event",
    "event_from_request",
]
