"""Django adapter, implemented as a standard Django middleware.

This adapter supplies only Django-specific transport bindings; the record
lifecycle itself (the request-scoped current event, dedupe, idempotency-key
stamping, and the final-outcome rule) lives in :mod:`everscribe.event` so
this adapter stays as thin as :mod:`everscribe.asgi` / :mod:`everscribe.flask`.

Django's middleware protocol - a class instantiated once per server process
as ``Middleware(get_response)``, whose ``__call__(request)`` wraps the rest
of the chain - is not bare WSGI: Django itself already sits between the WSGI
handler and the view, has already parsed ``request.user``/``request.session``
by the time a middleware later in ``MIDDLEWARE`` runs, and already converts
view exceptions into an ``HttpResponse`` before an outer middleware's
``get_response(request)`` call returns (Django wraps every middleware and the
view individually via ``convert_exception_to_response``). That is why this
gets its own adapter instead of reusing a generic WSGI wrapper: identity
lives on ``request.user`` rather than anything a WSGI-level wrapper could
read, and the exception-to-response conversion already happens beneath this
middleware, so (unlike the Flask adapter) a single ``__call__`` typically
sees a real response even when a view raised.

Mount it in ``MIDDLEWARE`` after ``AuthenticationMiddleware`` (and any other
session/auth middleware), since ``resolve_actor`` typically reads identity
off ``request.user``.

Requires Django (``pip install "everscribe[django]"``).
"""

from __future__ import annotations

import logging
from collections.abc import Callable

try:
    from django.conf import settings
    from django.http import HttpRequest, HttpResponse
except ImportError as e:  # pragma: no cover - exercised via install extras
    raise ImportError(
        "everscribe.django requires Django. Install it with: "
        'pip install "everscribe[django]"'
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

ActorResolver = Callable[[HttpRequest], Actor]
"""Derives the :class:`Actor` for a request. Typically reads
``request.user`` (populated by ``AuthenticationMiddleware``, which must run
before this middleware) or ``request.session``. Must not consume the request
body."""

_ANONYMOUS = Actor(type="anonymous")


class _OutcomeCapture:
    """Captures the response status from the value ``get_response`` returns,
    or the exception it raises, and translates either into a :class:`Result`
    on demand, satisfying :class:`everscribe.event.OutcomeCapture`.

    Django's own exception-to-response conversion (``convert_exception_to_
    response``, applied to every middleware and the view) means
    ``get_response`` ordinarily returns a real ``HttpResponse`` even when a
    view raised further down the chain; the exception path here exists for
    the same reason the Flask adapter's does - defense against whatever
    still reaches this middleware unconverted (for example a later
    middleware, or ASGI streaming responses, that Django's own conversion
    doesn't cover) - so an audit event still records rather than crashing
    silently.
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


class EverscribeMiddleware:
    """Django middleware that installs a per-request audit event and
    (optionally) auto-records it once ``get_response`` returns (or raises).

    Django itself instantiates middleware as ``Middleware(get_response)``,
    with no seam to pass extra constructor arguments, so ``recorder``,
    ``resolve_actor``, and ``logger`` can also be supplied via Django
    settings instead of (or in addition to) constructor keywords - whichever
    is set wins if both are given:

    - ``EVERSCRIBE_RECORDER``: a :class:`~everscribe.recorder.Recorder`
      instance. Omit to install the per-request event without auto-recording.
    - ``EVERSCRIBE_RESOLVE_ACTOR``: an :data:`ActorResolver`. Defaults to a
      resolver that returns an anonymous actor.
    - ``EVERSCRIBE_LOGGER``: a :class:`~everscribe.event.Logger` for
      auto-record failures. Defaults to a stdlib logger.

    Add to ``MIDDLEWARE``::

        MIDDLEWARE = [
            ...,
            "django.contrib.auth.middleware.AuthenticationMiddleware",
            "myproject.middleware.EverscribeMiddleware",  # after auth
        ]
    """

    def __init__(
        self,
        get_response: Callable[[HttpRequest], HttpResponse],
        *,
        recorder: Recorder | None = None,
        resolve_actor: ActorResolver | None = None,
        logger: Logger | None = None,
    ) -> None:
        self.get_response = get_response
        self.recorder: Recorder | None = (
            recorder if recorder is not None else getattr(settings, "EVERSCRIBE_RECORDER", None)
        )
        self.resolve_actor: ActorResolver = (
            resolve_actor
            or getattr(settings, "EVERSCRIBE_RESOLVE_ACTOR", None)
            or (lambda req: _ANONYMOUS)
        )
        self.logger: Logger = (
            logger
            or getattr(settings, "EVERSCRIBE_LOGGER", None)
            or logging.getLogger("everscribe.django")
        )

    def __call__(self, request: HttpRequest) -> HttpResponse:
        template = Event()
        template.actor = self.resolve_actor(request)
        template.origin = origin_from_request(dict(request.headers), request.META.get("REMOTE_ADDR", ""))

        capture = _OutcomeCapture()
        with request_scope(template, capture) as current:
            request.everscribe_event = current
            try:
                response = self.get_response(request)
            except Exception as exc:
                capture.set_error(exc)
                # end_event applies the final outcome (including the raised-
                # exception case above) and records `current` once, skipping
                # it if a handler already recorded it manually.
                end_event(current, self.recorder, self.logger)
                raise
            capture.set_status(response.status_code)
            end_event(current, self.recorder, self.logger)
            return response


def event_from_request(request: HttpRequest) -> Event:
    """Return the per-request event stashed on ``request`` by
    :class:`EverscribeMiddleware`, falling back to :func:`current_event`."""
    e = getattr(request, "everscribe_event", None)
    return e if isinstance(e, Event) else current_event()


__all__ = [
    "ActorResolver",
    "EverscribeMiddleware",
    "current_event",
    "event_from_request",
]
