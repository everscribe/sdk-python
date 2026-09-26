"""gRPC server interceptors for grpcio (sync and async).

Transport bindings only; the record lifecycle (the request-scoped current
event, dedupe, idempotency-key stamping, and the final-outcome rule) lives in
:mod:`everscribe.event`.

Pick the class matching your server, since grpcio's sync and async servers are
not interchangeable: :class:`EverscribeServerInterceptor` for a
``grpc.server()``, :class:`EverscribeAsyncServerInterceptor` for a
``grpc.aio.server()``. All four RPC shapes are supported: unary_unary,
unary_stream, stream_unary, and stream_stream.

Two behaviours differ from the HTTP adapters and change how you query:

- ``action`` defaults to the full RPC method name (e.g.
  ``/everscribe.v1.Ingest/Record``), so every RPC records unless a handler
  clears it. The HTTP adapters record nothing until a handler names the event.
- ``Result.code`` carries the canonical HTTP equivalent of the gRPC status via
  :func:`http_status_for_grpc_code`, never the native gRPC code, because gRPC's
  ``OK`` is 0 and every wire encoder drops a zero code as empty.

Streaming caveat: the ``contextvars`` lifecycle assumes one call is driven by a
single worker thread (sync) or a single asyncio Task (async) for its whole
lifetime. That holds in grpcio today and the streaming tests cover it, but it
rests on grpcio's execution model rather than a documented guarantee.

Requires grpcio (``pip install "everscribe[grpc]"``).
"""

from __future__ import annotations

import logging
from collections.abc import Callable
from dataclasses import dataclass
from typing import Any

try:
    import grpc
    from grpc import aio as grpc_aio
except ImportError as e:  # pragma: no cover - exercised via install extras
    raise ImportError(
        "everscribe.grpc requires grpcio. Install it with: "
        'pip install "everscribe[grpc]"'
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


@dataclass
class GrpcCallInfo:
    """What ``resolve_actor`` sees for an incoming call: the pieces
    available once request metadata has arrived, bundled the way ASGI's
    ``Request`` or Django's ``HttpRequest`` are for the HTTP adapters. gRPC
    has no single "request object" a resolver needs - metadata and peer
    identity live on the ``ServicerContext``, not the request message - so
    this is synthesized rather than passed through as-is."""

    #: Full RPC method name, e.g. "/everscribe.v1.Ingest/Record".
    method: str
    #: Incoming call metadata, as a plain string mapping (binary (``-bin``)
    #: values are dropped - see :func:`_metadata_to_dict`).
    metadata: dict[str, str]
    #: Remote peer address, when known (empty for transports with no
    #: meaningful client IP, e.g. a Unix domain socket).
    peer: str


ActorResolver = Callable[[GrpcCallInfo], Actor]
"""Derives the :class:`~everscribe.event.Actor` for an incoming call.
Typically reads identity out of call metadata (e.g. a bearer token already
validated by an auth interceptor earlier in the chain)."""

_ANONYMOUS = Actor(type="anonymous")

# Ports sdk-go's HTTPStatusFor (pkg/event/adapter_codes.go) case for case.
# grpc.StatusCode's values are numerically identical to
# google.golang.org/grpc/codes.Code (both are the same 0-16 wire-level gRPC
# status space), so no reinterpretation was needed. OK, UNKNOWN, INTERNAL,
# and DATA_LOSS are handled by http_status_for_grpc_code's explicit/default
# branches rather than listed here.
_HTTP_STATUS_FOR_GRPC_CODE: dict[Any, int] = {
    grpc.StatusCode.CANCELLED: 499,  # nginx's client-closed-request; no stdlib constant
    grpc.StatusCode.INVALID_ARGUMENT: 400,
    grpc.StatusCode.FAILED_PRECONDITION: 400,
    grpc.StatusCode.OUT_OF_RANGE: 400,
    grpc.StatusCode.UNAUTHENTICATED: 401,
    grpc.StatusCode.PERMISSION_DENIED: 403,
    grpc.StatusCode.NOT_FOUND: 404,
    grpc.StatusCode.ALREADY_EXISTS: 409,
    grpc.StatusCode.ABORTED: 409,
    grpc.StatusCode.RESOURCE_EXHAUSTED: 429,
    grpc.StatusCode.UNIMPLEMENTED: 501,
    grpc.StatusCode.UNAVAILABLE: 503,
    grpc.StatusCode.DEADLINE_EXCEEDED: 504,
}


def http_status_for_grpc_code(code: Any) -> int:
    """Maps a native gRPC status code to its canonical HTTP equivalent,
    following the grpc-gateway / Google API design guide mapping - the same
    mapping ``sdk-go``'s ``HTTPStatusFor`` and ``sdk-node``'s
    ``httpStatusForGrpcCode`` use. ``OK`` maps to 200; ``UNKNOWN``,
    ``INTERNAL``, and ``DATA_LOSS`` fall through to 500. The cost of this
    mapping is accepted deliberately: ``INVALID_ARGUMENT``,
    ``FAILED_PRECONDITION``, and ``OUT_OF_RANGE`` all collapse to 400, so the
    exact gRPC code is not recoverable from ``Result.code`` alone. The full
    status message is preserved in ``Result.message``."""
    if code == grpc.StatusCode.OK:
        return 200
    return _HTTP_STATUS_FOR_GRPC_CODE.get(code, 500)


def result_from_grpc_status(code: Any, details: str | None = None) -> Result:
    """Derives a :class:`Result` from a gRPC status code and an optional
    details string. Opt-in: core never calls this implicitly, the same
    contract as :func:`everscribe.event.result_from_http_status`.

    ``code`` ``OK`` always yields ``Result(status="ok", code=200)`` with no
    message, matching how :func:`~everscribe.event.result_from_http_status`
    treats a plain HTTP 200 - regardless of any ``details`` a caller passes
    in, since a successful call carries no diagnostic worth recording."""
    if code == grpc.StatusCode.OK:
        return result_from_http_status(200)
    result = result_from_http_status(http_status_for_grpc_code(code))
    if details:
        result.message = details
    return result


def _metadata_to_dict(metadata: Any) -> dict[str, str]:
    """Converts gRPC invocation metadata (a sequence of key/value tuples,
    possibly with duplicate keys - last one wins) into the plain string
    mapping :func:`everscribe.event.origin_from_request` expects. Binary
    (``-bin``-suffixed) metadata values are ``bytes``, not ``str``, and are
    skipped: none of the headers ``origin_from_request`` reads
    (``x-forwarded-for``, ``x-real-ip``, ``x-request-id``, ``user-agent``)
    are ever binary."""
    if not metadata:
        return {}
    return {k: v for k, v in metadata if isinstance(v, str)}


def _peer_addr(peer: str) -> str:
    """Strips gRPC's peer scheme prefix (``ipv4:``/``ipv6:``) so the
    remaining ``host:port`` (or bracketed ``[ipv6]:port``) can be handed to
    :func:`everscribe.event.origin_from_request`, whose ``client_ip`` helper
    already strips a trailing port correctly for both forms - stripping the
    port here first, before that shared helper sees it, would double-strip a
    bracketed IPv6 address (its own colons would be mistaken for the port
    separator). Unix domain socket peers (``unix:...``, ``unix-abstract:...``)
    carry no meaningful client IP and yield ``""``."""
    for prefix in ("ipv4:", "ipv6:"):
        if peer.startswith(prefix):
            return peer[len(prefix) :]
    return ""


def _result_from_context(context: Any, exc: BaseException | None) -> Result:
    """Reads the final outcome off a ``ServicerContext`` after the wrapped
    handler has returned or raised.

    ``context.code()`` / ``context.details()`` are public (if EXPERIMENTAL)
    accessor methods on both ``grpc.ServicerContext`` and
    ``grpc.aio.ServicerContext``, and read back whatever produced the
    call's final status: an explicit ``context.abort(code, details)`` (which
    raises internally to unwind the handler), a plain
    ``context.set_code(code)`` with no exception, or neither (the ordinary
    successful case, where ``code()`` returns ``None``). A raised exception
    that never set a code - a genuine bug in the handler, not an intentional
    abort - is treated as ``UNKNOWN``, the same status grpcio's own framework
    code assigns an unhandled exception."""
    code = context.code()
    if code is None:
        if exc is None:
            return result_from_http_status(200)
        code = grpc.StatusCode.UNKNOWN
    details = context.details()
    if isinstance(details, bytes):
        details = details.decode("utf-8", "replace")
    result = result_from_grpc_status(code, details or None)
    if not result.message and exc is not None:
        result.message = exc
    return result


def _build_template(resolve_actor: ActorResolver, context: Any, method: str) -> Event:
    info = GrpcCallInfo(
        method=method,
        metadata=_metadata_to_dict(context.invocation_metadata()),
        peer=_peer_addr(context.peer()),
    )
    template = Event()
    template.actor = resolve_actor(info)
    template.origin = origin_from_request(info.metadata, info.peer)
    return template


class _GrpcOutcomeCapture:
    """Satisfies :class:`everscribe.event.OutcomeCapture`. Reports no
    outcome (``None``) until :meth:`finalize` runs exactly once, after the
    wrapped handler has genuinely finished - never from inside the handler
    itself, so a handler that records an extra event mid-call via
    :func:`everscribe.event.new_from_context` still sees "no outcome yet"
    for that event, same as the HTTP adapters."""

    def __init__(self) -> None:
        self._result: Result | None = None

    def finalize(self, context: Any, exc: BaseException | None) -> None:
        self._result = _result_from_context(context, exc)

    @property
    def outcome(self) -> Result | None:
        return self._result


class EverscribeServerInterceptor(grpc.ServerInterceptor):
    """Sync ``grpc.ServerInterceptor`` for a ``grpc.server()``. Mount with::

        server = grpc.server(
            futures.ThreadPoolExecutor(),
            interceptors=[EverscribeServerInterceptor(recorder=rec, resolve_actor=resolve_actor)],
        )

    See the module docstring for the handler-resolution wart this interceptor
    works around, the ``action`` default, the gRPC-to-HTTP status mapping,
    and the arity scope decision.
    """

    def __init__(
        self,
        *,
        recorder: Recorder | None = None,
        resolve_actor: ActorResolver | None = None,
        logger: Logger | None = None,
    ) -> None:
        self.recorder = recorder
        self.resolve_actor: ActorResolver = resolve_actor or (lambda info: _ANONYMOUS)
        self.logger: Logger = logger or logging.getLogger("everscribe.grpc")

    def intercept_service(self, continuation: Any, handler_call_details: Any) -> Any:
        handler = continuation(handler_call_details)
        if handler is None:
            return None
        method = handler_call_details.method

        if not handler.request_streaming and not handler.response_streaming:
            return grpc.unary_unary_rpc_method_handler(
                self._wrap_unary(handler.unary_unary, method),
                request_deserializer=handler.request_deserializer,
                response_serializer=handler.response_serializer,
            )
        if not handler.request_streaming and handler.response_streaming:
            return grpc.unary_stream_rpc_method_handler(
                self._wrap_stream(handler.unary_stream, method),
                request_deserializer=handler.request_deserializer,
                response_serializer=handler.response_serializer,
            )
        if handler.request_streaming and not handler.response_streaming:
            return grpc.stream_unary_rpc_method_handler(
                self._wrap_unary(handler.stream_unary, method),
                request_deserializer=handler.request_deserializer,
                response_serializer=handler.response_serializer,
            )
        return grpc.stream_stream_rpc_method_handler(
            self._wrap_stream(handler.stream_stream, method),
            request_deserializer=handler.request_deserializer,
            response_serializer=handler.response_serializer,
        )

    def _wrap_unary(self, inner: Callable[[Any, Any], Any], method: str) -> Callable[[Any, Any], Any]:
        """Wraps a single-response behavior. ``request`` may be a lone
        message (unary_unary) or a request iterator (stream_unary); this
        wrapper never inspects it, only forwards it, so one implementation
        covers both arities."""

        def behavior(request: Any, context: Any) -> Any:
            template = _build_template(self.resolve_actor, context, method)
            capture = _GrpcOutcomeCapture()
            with request_scope(template, capture) as current:
                # Stamped on the request-scoped event, not the template -
                # see the module docstring for why.
                current.action = method
                try:
                    response = inner(request, context)
                except Exception as exc:
                    capture.finalize(context, exc)
                    end_event(current, self.recorder, self.logger)
                    raise
                capture.finalize(context, None)
                end_event(current, self.recorder, self.logger)
                return response

        return behavior

    def _wrap_stream(
        self, inner: Callable[[Any, Any], Any], method: str
    ) -> Callable[[Any, Any], Any]:
        """Wraps a streaming-response behavior (unary_stream or
        stream_stream). The lifecycle stays installed across every yield:
        verified directly against a real, concurrent, interleaved server -
        see the module docstring's Arity scope section."""

        def behavior(request: Any, context: Any) -> Any:
            template = _build_template(self.resolve_actor, context, method)
            capture = _GrpcOutcomeCapture()
            with request_scope(template, capture) as current:
                current.action = method
                try:
                    yield from inner(request, context)
                except Exception as exc:
                    capture.finalize(context, exc)
                    end_event(current, self.recorder, self.logger)
                    raise
                capture.finalize(context, None)
                end_event(current, self.recorder, self.logger)

        return behavior


class EverscribeAsyncServerInterceptor(grpc_aio.ServerInterceptor):
    """Async counterpart of :class:`EverscribeServerInterceptor`, for a
    ``grpc.aio.server()``. ``grpc.ServerInterceptor`` (sync) and
    ``grpc.aio.ServerInterceptor`` (async) are not interchangeable - grpcio
    gives them different base classes and an ``async def intercept_service``
    signature for the latter - so this is a separate class, not a code path
    on the sync one. Mount with::

        server = grpc.aio.server(interceptors=[EverscribeAsyncServerInterceptor(recorder=rec)])
    """

    def __init__(
        self,
        *,
        recorder: Recorder | None = None,
        resolve_actor: ActorResolver | None = None,
        logger: Logger | None = None,
    ) -> None:
        self.recorder = recorder
        self.resolve_actor: ActorResolver = resolve_actor or (lambda info: _ANONYMOUS)
        self.logger: Logger = logger or logging.getLogger("everscribe.grpc")

    async def intercept_service(self, continuation: Any, handler_call_details: Any) -> Any:
        handler = await continuation(handler_call_details)
        if handler is None:
            return None
        method = handler_call_details.method

        if not handler.request_streaming and not handler.response_streaming:
            return grpc.unary_unary_rpc_method_handler(
                self._wrap_unary(handler.unary_unary, method),
                request_deserializer=handler.request_deserializer,
                response_serializer=handler.response_serializer,
            )
        if not handler.request_streaming and handler.response_streaming:
            return grpc.unary_stream_rpc_method_handler(
                self._wrap_stream(handler.unary_stream, method),
                request_deserializer=handler.request_deserializer,
                response_serializer=handler.response_serializer,
            )
        if handler.request_streaming and not handler.response_streaming:
            return grpc.stream_unary_rpc_method_handler(
                self._wrap_unary(handler.stream_unary, method),
                request_deserializer=handler.request_deserializer,
                response_serializer=handler.response_serializer,
            )
        return grpc.stream_stream_rpc_method_handler(
            self._wrap_stream(handler.stream_stream, method),
            request_deserializer=handler.request_deserializer,
            response_serializer=handler.response_serializer,
        )

    def _wrap_unary(self, inner: Callable[[Any, Any], Any], method: str) -> Callable[[Any, Any], Any]:
        async def behavior(request: Any, context: Any) -> Any:
            template = _build_template(self.resolve_actor, context, method)
            capture = _GrpcOutcomeCapture()
            with request_scope(template, capture) as current:
                current.action = method
                try:
                    response = await inner(request, context)
                except Exception as exc:
                    capture.finalize(context, exc)
                    end_event(current, self.recorder, self.logger)
                    raise
                capture.finalize(context, None)
                end_event(current, self.recorder, self.logger)
                return response

        return behavior

    def _wrap_stream(
        self, inner: Callable[[Any, Any], Any], method: str
    ) -> Callable[[Any, Any], Any]:
        async def behavior(request: Any, context: Any) -> Any:
            template = _build_template(self.resolve_actor, context, method)
            capture = _GrpcOutcomeCapture()
            with request_scope(template, capture) as current:
                current.action = method
                try:
                    async for message in inner(request, context):
                        yield message
                except Exception as exc:
                    capture.finalize(context, exc)
                    end_event(current, self.recorder, self.logger)
                    raise
                capture.finalize(context, None)
                end_event(current, self.recorder, self.logger)

        return behavior


__all__ = [
    "ActorResolver",
    "EverscribeAsyncServerInterceptor",
    "EverscribeServerInterceptor",
    "GrpcCallInfo",
    "current_event",
    "http_status_for_grpc_code",
    "result_from_grpc_status",
]
