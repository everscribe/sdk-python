"""gRPC server interceptors for grpcio (sync and async).

This adapter supplies only gRPC-specific transport bindings; the record
lifecycle itself (the request-scoped current event, dedupe, idempotency-key
stamping, and the final-outcome rule) lives in :mod:`everscribe.event` so
this adapter stays as thin as :mod:`everscribe.asgi` / :mod:`everscribe.flask`
/ :mod:`everscribe.django`.

Two classes, because grpcio's sync and async servers are not interchangeable:
:class:`EverscribeServerInterceptor` extends ``grpc.ServerInterceptor`` for a
``grpc.server()``; :class:`EverscribeAsyncServerInterceptor` extends
``grpc.aio.ServerInterceptor`` for a ``grpc.aio.server()``. This split is
grpcio's design, not a choice made here.

grpcio's ``intercept_service(continuation, handler_call_details)`` is a known
wart: it intercepts at handler *resolution*, not per call - ``continuation``
returns an ``RpcMethodHandler`` bundling one of four business-logic callables
(``unary_unary``, ``unary_stream``, ``stream_unary``, ``stream_stream``,
exactly one non-``None`` depending on the method's request/response streaming
shape) plus the request/response (de)serializers. To observe an outcome, this
module unwraps that handler, rebuilds a new one whose inner callable installs
the lifecycle around a call into the original, and passes the original
(de)serializers through unchanged - dropping either one would break the RPC's
wire format.

``action`` defaults to the full RPC method name (e.g.
``/everscribe.v1.Ingest/Record``), so every RPC records unless a handler
clears it - deliberately different from the HTTP adapters (ASGI/Flask/
Django), which record nothing until a handler names the event. There is no
equivalent of an HTTP router path that is meaningful without a handler-chosen
action: the RPC method name already *is* the action a gRPC service was built
around. ``sdk-go``'s ``UnaryInterceptor`` and ``sdk-node``'s
``grpcServerInterceptor`` both default the same way. The default is stamped
on the request-scoped event (:func:`everscribe.event.current_event`'s
return value), never on the template passed into :func:`everscribe.event.request_scope`:
stamping the template would leak the method name into every
:func:`everscribe.event.new_from_context` clone a handler makes, so a
secondary event the handler never named would inherit the RPC method name
instead of being dropped by the empty-action no-op every stock recorder
applies - a real bug caught in ``sdk-go``'s review before it shipped there.

``Result.code`` always carries the canonical HTTP equivalent of the gRPC
status, via :func:`http_status_for_grpc_code` (ported case for case from
``sdk-go``'s ``HTTPStatusFor``, ``pkg/event/adapter_codes.go``), never the
native gRPC code: gRPC's ``OK`` is code 0, which every SDK's wire encoder
drops as empty, so a native code would make successful RPCs unmatchable by a
``result.code`` query.

## Arity scope

Supports all four RPC shapes: unary_unary, unary_stream (server streaming),
stream_unary (client streaming), and stream_stream (bidi streaming). This is
broader than ``sdk-go``/``sdk-rust`` (unary plus server-streaming only) and
``sdk-node`` (unary only) - both of those are bounded by their own runtime's
hook timing (grpc-js, for example, invokes the handler from a hook this
interceptor never gets to wrap, for client-streaming and bidi calls), not by
anything inherent to gRPC itself. grpcio's model is structurally different:
because ``intercept_service`` replaces the handler's own callable rather than
hooking a fixed lifecycle point, every arity gets wrapped the same way.

The one real risk for a streaming arity is whether the ``contextvars``-based
lifecycle (:func:`everscribe.event.request_scope`) survives a handler
resumption on a different thread (sync) or a different asyncio Task (async):
if grpcio ever drove a single call's generator/async-generator from more than
one thread or Task, ``current_event()`` could silently return a throwaway
event mid-stream instead of the call's real one. This was verified directly,
not assumed: concurrent, artificially-interleaved unary_stream/stream_unary/
stream_stream calls against a real ``grpc.server()`` and a real
``grpc.aio.server()`` over a real socket confirmed a single call is always
driven by exactly one worker thread (sync) or one Task (async) for its entire
lifetime, so the ``contextvar`` set when the call begins stays visible on
every subsequent yield/iteration. This relies on grpcio's current internal
execution model rather than a documented public guarantee; see the
adapters report for the spike that established it.

Requires grpcio (``pip install "everscribe[grpc]"``).
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Any, Callable

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
    metadata: "dict[str, str]"
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
_HTTP_STATUS_FOR_GRPC_CODE: "dict[Any, int]" = {
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


def result_from_grpc_status(code: Any, details: "str | None" = None) -> Result:
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


def _metadata_to_dict(metadata: Any) -> "dict[str, str]":
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


def _result_from_context(context: Any, exc: "BaseException | None") -> Result:
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
        self._result: "Result | None" = None

    def finalize(self, context: Any, exc: "BaseException | None") -> None:
        self._result = _result_from_context(context, exc)

    @property
    def outcome(self) -> "Result | None":
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
        recorder: "Recorder | None" = None,
        resolve_actor: "ActorResolver | None" = None,
        logger: "Logger | None" = None,
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
                    for message in inner(request, context):
                        yield message
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
        recorder: "Recorder | None" = None,
        resolve_actor: "ActorResolver | None" = None,
        logger: "Logger | None" = None,
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
    "EverscribeServerInterceptor",
    "EverscribeAsyncServerInterceptor",
    "ActorResolver",
    "GrpcCallInfo",
    "current_event",
    "result_from_grpc_status",
    "http_status_for_grpc_code",
]
