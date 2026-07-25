"""Flask adapter for Flask 2+.

This adapter supplies only Flask-specific transport bindings; the record
lifecycle itself (the request-scoped current event, dedupe, idempotency-key
stamping, and the final-outcome rule) lives in :mod:`everscribe.event` so
this adapter stays as thin as :mod:`everscribe.asgi`.

Two things make this a real Flask adapter rather than a generic WSGI
wrapper:

1. It hooks ``before_request``/``teardown_request``, not ``app.wsgi_app``. A
   WSGI-level middleware wraps the app from *outside* the Flask application
   context, so ``flask.g``, ``flask.session``, and ``flask_login.current_user``
   are unreachable from an actor resolver running there. ``before_request``
   runs inside the app context, so that is where the lifecycle gets
   installed and where a resolver can read identity.
2. It records on ``teardown_request``, not ``after_request``. Flask skips
   ``after_request`` when a view raises and the exception propagates
   unhandled (the default under ``app.testing`` / ``app.debug``, i.e. under
   most test suites) - exactly the case an audit log most needs to capture.
   ``teardown_request`` always runs, so that is where :func:`end_event` is
   called. ``after_request`` still runs in the normal, non-raising case (and
   for ``abort()``-raised ``HTTPException``s, which Flask handles before
   ``teardown_request`` ever sees an exception), so it is used there only to
   capture the final status code; the two hooks are reconciled by
   :class:`_OutcomeCapture` below.

Mount it after any session/auth extension (e.g. Flask-Login), since
``resolve_actor`` typically reads identity off ``flask.g``/``flask.session``/
``flask_login.current_user``.

Requires Flask (``pip install "everscribe[flask]"``).
"""

from __future__ import annotations

import logging
from collections.abc import Callable
from contextlib import ExitStack

try:
    from flask import Flask, Response, g, request
except ImportError as e:  # pragma: no cover - exercised via install extras
    raise ImportError(
        "everscribe.flask requires Flask. Install it with: "
        'pip install "everscribe[flask]"'
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

ActorResolver = Callable[[], Actor]
"""Derives the :class:`Actor` for the current request. Runs inside
``before_request``, so it can read ``flask.g``, ``flask.session``, or
``flask_login.current_user`` directly - all ambient, app-context-scoped
state, which is why this takes no arguments (unlike the ASGI adapter's
``Callable[[Request], Actor]``: Flask has no single request-like object a
resolver needs passed in, since ``flask.request`` is itself already an
ambient proxy)."""

_ANONYMOUS = Actor(type="anonymous")
_STATE_KEY = "_everscribe_state"


class _OutcomeCapture:
    """Reconciles Flask's two post-dispatch hooks into a single
    :class:`OutcomeCapture`.

    ``after_request`` supplies the status code on the success path
    (including ``abort()``-raised ``HTTPException``s, which Flask converts to
    a response before ``after_request`` runs). An exception that instead
    propagates all the way to ``teardown_request`` means ``after_request``
    never ran and no status code exists at all; that path is captured
    separately via :meth:`set_error` and takes precedence, since it is the
    more specific, later-observed signal.
    """

    def __init__(self) -> None:
        self._status = 0
        self._error: BaseException | None = None

    def set_status(self, status: int) -> None:
        self._status = status

    def set_error(self, err: BaseException) -> None:
        self._error = err

    @property
    def outcome(self) -> Result | None:
        if self._error is not None:
            return Result(status="error", code=500, message=self._error)
        if self._status == 0:
            return None
        return result_from_http_status(self._status)


class EverscribeFlask:
    """Flask extension that installs a per-request audit event and
    (optionally) auto-records it when the request tears down.

    Supports both direct construction and the app-factory pattern::

        ext = EverscribeFlask(app, recorder=rec, resolve_actor=resolve_actor)

        # or

        ext = EverscribeFlask()
        ext.init_app(app, recorder=rec, resolve_actor=resolve_actor)
    """

    def __init__(
        self,
        app: Flask | None = None,
        *,
        recorder: Recorder | None = None,
        resolve_actor: ActorResolver | None = None,
        logger: Logger | None = None,
    ) -> None:
        self.recorder = recorder
        self.resolve_actor: ActorResolver = resolve_actor or (lambda: _ANONYMOUS)
        self.logger: Logger = logger or logging.getLogger("everscribe.flask")
        if app is not None:
            self.init_app(app)

    def init_app(
        self,
        app: Flask,
        *,
        recorder: Recorder | None = None,
        resolve_actor: ActorResolver | None = None,
        logger: Logger | None = None,
    ) -> None:
        if recorder is not None:
            self.recorder = recorder
        if resolve_actor is not None:
            self.resolve_actor = resolve_actor
        if logger is not None:
            self.logger = logger
        app.before_request(self._before_request)
        app.after_request(self._after_request)
        app.teardown_request(self._teardown_request)

    def _before_request(self) -> None:
        template = Event()
        template.actor = self.resolve_actor()
        template.origin = origin_from_request(dict(request.headers), request.remote_addr or "")

        capture = _OutcomeCapture()
        stack = ExitStack()
        current = stack.enter_context(request_scope(template, capture))
        setattr(g, _STATE_KEY, (stack, capture, current))

    def _after_request(self, response: Response) -> Response:
        state = g.get(_STATE_KEY)
        if state is not None:
            _, capture, _ = state
            capture.set_status(response.status_code)
        return response

    def _teardown_request(self, exc: BaseException | None) -> None:
        state = g.pop(_STATE_KEY, None)
        if state is None:
            return
        stack, capture, current = state
        try:
            if exc is not None:
                capture.set_error(exc)
            # end_event applies the final outcome (including the raised-
            # exception case above) and records `current` once, skipping it
            # if a handler already recorded it manually.
            end_event(current, self.recorder, self.logger)
        finally:
            stack.close()


__all__ = ["ActorResolver", "EverscribeFlask", "current_event"]
