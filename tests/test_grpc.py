"""Tests for the gRPC adapter (sync and async), exercised against a real
grpc.server() / grpc.aio.server() bound to a real loopback socket - no mocks,
no .proto/codegen (rejected for this SDK generally): request/response bodies
are raw bytes with identity (de)serializers, the same "wire format defined
inline" approach sdk-node's gRPC adapter tests use.
"""

from __future__ import annotations

import asyncio
import threading
from concurrent import futures
from typing import Any, Dict, List

import grpc
import pytest
from grpc import aio as grpc_aio

from everscribe.event import Actor, Event, current_event, new_from_context
from everscribe.grpc import (
    EverscribeAsyncServerInterceptor,
    EverscribeServerInterceptor,
    GrpcCallInfo,
    http_status_for_grpc_code,
    result_from_grpc_status,
)


class FakeRecorder:
    def __init__(self) -> None:
        self.events: List[Event] = []
        self.lock = threading.Lock()

    def record(self, e: Event) -> None:
        with self.lock:
            self.events.append(e)


def _identity(x: bytes) -> bytes:
    return x


# ---------------------------------------------------------------------------
# Sync server fixture
# ---------------------------------------------------------------------------


def _sync_handlers(diagnostics: "Dict[str, Any]") -> "dict[str, Any]":
    def ping(request: bytes, context: Any) -> bytes:
        return request

    def custom_action(request: bytes, context: Any) -> bytes:
        current_event().action = "custom.action"
        return request

    def denied(request: bytes, context: Any) -> bytes:
        context.abort(grpc.StatusCode.PERMISSION_DENIED, "no")
        raise AssertionError("unreachable")  # abort() always raises

    def crashes(request: bytes, context: Any) -> bytes:
        raise RuntimeError("boom")

    def clone_check(request: bytes, context: Any) -> bytes:
        clone = new_from_context()
        diagnostics["clone_action_before"] = clone.action
        current_event().action = "primary"
        return request

    def echo_stream(request: bytes, context: Any) -> Any:
        for _ in range(3):
            yield request

    def sum_stream(request_iterator: Any, context: Any) -> bytes:
        return b",".join(request_iterator)

    def echo_bidi(request_iterator: Any, context: Any) -> Any:
        for req in request_iterator:
            yield req

    return {
        "Ping": grpc.unary_unary_rpc_method_handler(
            ping, request_deserializer=_identity, response_serializer=_identity
        ),
        "CustomAction": grpc.unary_unary_rpc_method_handler(
            custom_action, request_deserializer=_identity, response_serializer=_identity
        ),
        "Denied": grpc.unary_unary_rpc_method_handler(
            denied, request_deserializer=_identity, response_serializer=_identity
        ),
        "Crashes": grpc.unary_unary_rpc_method_handler(
            crashes, request_deserializer=_identity, response_serializer=_identity
        ),
        "Clone": grpc.unary_unary_rpc_method_handler(
            clone_check, request_deserializer=_identity, response_serializer=_identity
        ),
        "EchoStream": grpc.unary_stream_rpc_method_handler(
            echo_stream, request_deserializer=_identity, response_serializer=_identity
        ),
        "SumStream": grpc.stream_unary_rpc_method_handler(
            sum_stream, request_deserializer=_identity, response_serializer=_identity
        ),
        "EchoBidi": grpc.stream_stream_rpc_method_handler(
            echo_bidi, request_deserializer=_identity, response_serializer=_identity
        ),
    }


class SyncServer:
    def __init__(self, recorder: "FakeRecorder | None" = None, resolve_actor: Any = None) -> None:
        self.recorder = recorder
        self.diagnostics: "Dict[str, Any]" = {}
        interceptor = EverscribeServerInterceptor(recorder=recorder, resolve_actor=resolve_actor)
        self.server = grpc.server(futures.ThreadPoolExecutor(max_workers=8), interceptors=[interceptor])
        handlers = _sync_handlers(self.diagnostics)
        generic = grpc.method_handlers_generic_handler("test.Test", handlers)
        self.server.add_generic_rpc_handlers((generic,))
        self.port = self.server.add_insecure_port("127.0.0.1:0")
        self.server.start()
        self.channel = grpc.insecure_channel(f"127.0.0.1:{self.port}")

    def unary(self, method: str) -> Any:
        return self.channel.unary_unary(
            f"/test.Test/{method}", request_serializer=_identity, response_deserializer=_identity
        )

    def unary_stream(self, method: str) -> Any:
        return self.channel.unary_stream(
            f"/test.Test/{method}", request_serializer=_identity, response_deserializer=_identity
        )

    def stream_unary(self, method: str) -> Any:
        return self.channel.stream_unary(
            f"/test.Test/{method}", request_serializer=_identity, response_deserializer=_identity
        )

    def stream_stream(self, method: str) -> Any:
        return self.channel.stream_stream(
            f"/test.Test/{method}", request_serializer=_identity, response_deserializer=_identity
        )

    def close(self) -> None:
        self.channel.close()
        self.server.stop(None)


@pytest.fixture
def sync_server():  # type: ignore[no-untyped-def]
    servers: "List[SyncServer]" = []

    def make(recorder: "FakeRecorder | None" = None, resolve_actor: Any = None) -> SyncServer:
        s = SyncServer(recorder=recorder, resolve_actor=resolve_actor)
        servers.append(s)
        return s

    yield make
    for s in servers:
        s.close()


def test_sync_records_once_per_rpc(sync_server) -> None:  # type: ignore[no-untyped-def]
    rec = FakeRecorder()
    s = sync_server(recorder=rec)
    resp = s.unary("Ping")(b"hello")
    assert resp == b"hello"
    assert len(rec.events) == 1


def test_sync_current_event_resolves_in_handler_with_fresh_event_per_call(sync_server) -> None:  # type: ignore[no-untyped-def]
    rec = FakeRecorder()
    s = sync_server(recorder=rec)
    s.unary("Ping")(b"a")
    s.unary("Ping")(b"b")
    assert len(rec.events) == 2
    assert rec.events[0].id != rec.events[1].id


def test_sync_action_defaults_to_full_method_name(sync_server) -> None:  # type: ignore[no-untyped-def]
    rec = FakeRecorder()
    s = sync_server(recorder=rec)
    s.unary("Ping")(b"hello")
    assert rec.events[0].action == "/test.Test/Ping"


def test_sync_handler_set_action_wins(sync_server) -> None:  # type: ignore[no-untyped-def]
    rec = FakeRecorder()
    s = sync_server(recorder=rec)
    s.unary("CustomAction")(b"hello")
    assert rec.events[0].action == "custom.action"


def test_sync_clone_does_not_inherit_method_name(sync_server) -> None:  # type: ignore[no-untyped-def]
    rec = FakeRecorder()
    s = sync_server(recorder=rec)
    s.unary("Clone")(b"hello")
    # Read before the handler set anything on current_event(): if action were
    # stamped on the template instead of the request-scoped event, this would
    # be "/test.Test/Clone" instead of "".
    assert s.diagnostics["clone_action_before"] == ""


def test_sync_permission_denied_records_as_denied_403(sync_server) -> None:  # type: ignore[no-untyped-def]
    rec = FakeRecorder()
    s = sync_server(recorder=rec)
    with pytest.raises(grpc.RpcError) as exc_info:
        s.unary("Denied")(b"hello")
    assert exc_info.value.code() == grpc.StatusCode.PERMISSION_DENIED
    assert rec.events[0].result.status == "denied"
    assert rec.events[0].result.code == 403


def test_sync_ok_records_as_200_never_0(sync_server) -> None:  # type: ignore[no-untyped-def]
    rec = FakeRecorder()
    s = sync_server(recorder=rec)
    s.unary("Ping")(b"hello")
    assert rec.events[0].result.status == "ok"
    assert rec.events[0].result.code == 200
    assert rec.events[0].result.code != 0


def test_sync_unhandled_exception_records_as_error(sync_server) -> None:  # type: ignore[no-untyped-def]
    rec = FakeRecorder()
    s = sync_server(recorder=rec)
    with pytest.raises(grpc.RpcError):
        s.unary("Crashes")(b"hello")
    assert rec.events[0].result.status == "error"
    assert "boom" in str(rec.events[0].result.message)


def test_sync_resolver_receives_call_info(sync_server) -> None:  # type: ignore[no-untyped-def]
    rec = FakeRecorder()
    seen: "list[GrpcCallInfo]" = []

    def resolver(info: GrpcCallInfo) -> Actor:
        seen.append(info)
        return Actor(type="user", id="u1")

    s = sync_server(recorder=rec, resolve_actor=resolver)
    s.unary("Ping")(b"hello", metadata=(("x-request-id", "req-9"),))
    assert rec.events[0].actor == Actor(type="user", id="u1")
    assert seen[0].method == "/test.Test/Ping"
    assert rec.events[0].origin.request_id == "req-9"


def test_sync_unary_stream_records_once_and_yields_all_messages(sync_server) -> None:  # type: ignore[no-untyped-def]
    rec = FakeRecorder()
    s = sync_server(recorder=rec)
    msgs = list(s.unary_stream("EchoStream")(b"x"))
    assert msgs == [b"x", b"x", b"x"]
    assert len(rec.events) == 1
    assert rec.events[0].result.code == 200


def test_sync_stream_unary_records_once(sync_server) -> None:  # type: ignore[no-untyped-def]
    rec = FakeRecorder()
    s = sync_server(recorder=rec)
    resp = s.stream_unary("SumStream")(iter([b"a", b"b", b"c"]))
    assert resp == b"a,b,c"
    assert len(rec.events) == 1
    assert rec.events[0].result.code == 200


def test_sync_stream_stream_records_once(sync_server) -> None:  # type: ignore[no-untyped-def]
    rec = FakeRecorder()
    s = sync_server(recorder=rec)
    msgs = list(s.stream_stream("EchoBidi")(iter([b"a", b"b", b"c"])))
    assert msgs == [b"a", b"b", b"c"]
    assert len(rec.events) == 1


def test_sync_concurrent_rpcs_do_not_cross_talk(sync_server) -> None:  # type: ignore[no-untyped-def]
    rec = FakeRecorder()
    s = sync_server(recorder=rec)

    def call(tag: str) -> None:
        s.unary("Ping")(tag.encode())

    threads = [threading.Thread(target=call, args=(f"t{i}",)) for i in range(10)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    assert len(rec.events) == 10
    assert len({e.id for e in rec.events}) == 10


# ---------------------------------------------------------------------------
# Async server
# ---------------------------------------------------------------------------


def _async_handlers(diagnostics: "Dict[str, Any]") -> "dict[str, Any]":
    async def ping(request: bytes, context: Any) -> bytes:
        return request

    async def custom_action(request: bytes, context: Any) -> bytes:
        current_event().action = "custom.action"
        return request

    async def denied(request: bytes, context: Any) -> bytes:
        await context.abort(grpc.StatusCode.PERMISSION_DENIED, "no")
        raise AssertionError("unreachable")

    async def crashes(request: bytes, context: Any) -> bytes:
        raise RuntimeError("boom")

    async def clone_check(request: bytes, context: Any) -> bytes:
        clone = new_from_context()
        diagnostics["clone_action_before"] = clone.action
        current_event().action = "primary"
        return request

    async def echo_stream(request: bytes, context: Any) -> Any:
        for _ in range(3):
            yield request

    async def sum_stream(request_iterator: Any, context: Any) -> bytes:
        parts = [p async for p in request_iterator]
        return b",".join(parts)

    async def echo_bidi(request_iterator: Any, context: Any) -> Any:
        async for req in request_iterator:
            yield req

    return {
        "Ping": grpc.unary_unary_rpc_method_handler(
            ping, request_deserializer=_identity, response_serializer=_identity
        ),
        "CustomAction": grpc.unary_unary_rpc_method_handler(
            custom_action, request_deserializer=_identity, response_serializer=_identity
        ),
        "Denied": grpc.unary_unary_rpc_method_handler(
            denied, request_deserializer=_identity, response_serializer=_identity
        ),
        "Crashes": grpc.unary_unary_rpc_method_handler(
            crashes, request_deserializer=_identity, response_serializer=_identity
        ),
        "Clone": grpc.unary_unary_rpc_method_handler(
            clone_check, request_deserializer=_identity, response_serializer=_identity
        ),
        "EchoStream": grpc.unary_stream_rpc_method_handler(
            echo_stream, request_deserializer=_identity, response_serializer=_identity
        ),
        "SumStream": grpc.stream_unary_rpc_method_handler(
            sum_stream, request_deserializer=_identity, response_serializer=_identity
        ),
        "EchoBidi": grpc.stream_stream_rpc_method_handler(
            echo_bidi, request_deserializer=_identity, response_serializer=_identity
        ),
    }


class AsyncServer:
    def __init__(self) -> None:
        self.diagnostics: "Dict[str, Any]" = {}
        self.server: Any = None
        self.channel: Any = None
        self.port = 0

    async def start(self, recorder: "FakeRecorder | None" = None, resolve_actor: Any = None) -> None:
        interceptor = EverscribeAsyncServerInterceptor(recorder=recorder, resolve_actor=resolve_actor)
        self.server = grpc_aio.server(interceptors=[interceptor])
        handlers = _async_handlers(self.diagnostics)
        generic = grpc.method_handlers_generic_handler("test.Test", handlers)
        self.server.add_generic_rpc_handlers((generic,))
        self.port = self.server.add_insecure_port("127.0.0.1:0")
        await self.server.start()
        self.channel = grpc_aio.insecure_channel(f"127.0.0.1:{self.port}")

    def unary(self, method: str) -> Any:
        return self.channel.unary_unary(
            f"/test.Test/{method}", request_serializer=_identity, response_deserializer=_identity
        )

    def unary_stream(self, method: str) -> Any:
        return self.channel.unary_stream(
            f"/test.Test/{method}", request_serializer=_identity, response_deserializer=_identity
        )

    def stream_unary(self, method: str) -> Any:
        return self.channel.stream_unary(
            f"/test.Test/{method}", request_serializer=_identity, response_deserializer=_identity
        )

    def stream_stream(self, method: str) -> Any:
        return self.channel.stream_stream(
            f"/test.Test/{method}", request_serializer=_identity, response_deserializer=_identity
        )

    async def close(self) -> None:
        await self.channel.close()
        await self.server.stop(None)


def run_async(coro: Any) -> Any:
    return asyncio.run(coro)


def test_async_records_once_per_rpc() -> None:
    async def body() -> None:
        rec = FakeRecorder()
        s = AsyncServer()
        await s.start(recorder=rec)
        try:
            resp = await s.unary("Ping")(b"hello")
            assert resp == b"hello"
            assert len(rec.events) == 1
        finally:
            await s.close()

    run_async(body())


def test_async_current_event_resolves_with_fresh_event_per_call() -> None:
    async def body() -> None:
        rec = FakeRecorder()
        s = AsyncServer()
        await s.start(recorder=rec)
        try:
            await s.unary("Ping")(b"a")
            await s.unary("Ping")(b"b")
            assert len(rec.events) == 2
            assert rec.events[0].id != rec.events[1].id
        finally:
            await s.close()

    run_async(body())


def test_async_action_defaults_to_full_method_name() -> None:
    async def body() -> None:
        rec = FakeRecorder()
        s = AsyncServer()
        await s.start(recorder=rec)
        try:
            await s.unary("Ping")(b"hello")
            assert rec.events[0].action == "/test.Test/Ping"
        finally:
            await s.close()

    run_async(body())


def test_async_handler_set_action_wins() -> None:
    async def body() -> None:
        rec = FakeRecorder()
        s = AsyncServer()
        await s.start(recorder=rec)
        try:
            await s.unary("CustomAction")(b"hello")
            assert rec.events[0].action == "custom.action"
        finally:
            await s.close()

    run_async(body())


def test_async_clone_does_not_inherit_method_name() -> None:
    async def body() -> None:
        rec = FakeRecorder()
        s = AsyncServer()
        await s.start(recorder=rec)
        try:
            await s.unary("Clone")(b"hello")
            assert s.diagnostics["clone_action_before"] == ""
        finally:
            await s.close()

    run_async(body())


def test_async_permission_denied_records_as_denied_403() -> None:
    async def body() -> None:
        rec = FakeRecorder()
        s = AsyncServer()
        await s.start(recorder=rec)
        try:
            with pytest.raises(grpc_aio.AioRpcError) as exc_info:
                await s.unary("Denied")(b"hello")
            assert exc_info.value.code() == grpc.StatusCode.PERMISSION_DENIED
            assert rec.events[0].result.status == "denied"
            assert rec.events[0].result.code == 403
        finally:
            await s.close()

    run_async(body())


def test_async_ok_records_as_200_never_0() -> None:
    async def body() -> None:
        rec = FakeRecorder()
        s = AsyncServer()
        await s.start(recorder=rec)
        try:
            await s.unary("Ping")(b"hello")
            assert rec.events[0].result.status == "ok"
            assert rec.events[0].result.code == 200
            assert rec.events[0].result.code != 0
        finally:
            await s.close()

    run_async(body())


def test_async_unhandled_exception_records_as_error() -> None:
    async def body() -> None:
        rec = FakeRecorder()
        s = AsyncServer()
        await s.start(recorder=rec)
        try:
            with pytest.raises(grpc_aio.AioRpcError):
                await s.unary("Crashes")(b"hello")
            assert rec.events[0].result.status == "error"
            assert "boom" in str(rec.events[0].result.message)
        finally:
            await s.close()

    run_async(body())


def test_async_unary_stream_records_once_and_yields_all_messages() -> None:
    async def body() -> None:
        rec = FakeRecorder()
        s = AsyncServer()
        await s.start(recorder=rec)
        try:
            call = s.unary_stream("EchoStream")(b"x")
            msgs = [m async for m in call]
            assert msgs == [b"x", b"x", b"x"]
            assert len(rec.events) == 1
            assert rec.events[0].result.code == 200
        finally:
            await s.close()

    run_async(body())


def test_async_stream_unary_records_once() -> None:
    async def body() -> None:
        rec = FakeRecorder()
        s = AsyncServer()
        await s.start(recorder=rec)
        try:

            async def reqs() -> Any:
                for x in (b"a", b"b", b"c"):
                    yield x

            resp = await s.stream_unary("SumStream")(reqs())
            assert resp == b"a,b,c"
            assert len(rec.events) == 1
        finally:
            await s.close()

    run_async(body())


def test_async_stream_stream_records_once() -> None:
    async def body() -> None:
        rec = FakeRecorder()
        s = AsyncServer()
        await s.start(recorder=rec)
        try:

            async def reqs() -> Any:
                for x in (b"a", b"b", b"c"):
                    yield x

            call = s.stream_stream("EchoBidi")(reqs())
            msgs = [m async for m in call]
            assert msgs == [b"a", b"b", b"c"]
            assert len(rec.events) == 1
        finally:
            await s.close()

    run_async(body())


def test_async_concurrent_rpcs_do_not_cross_talk() -> None:
    async def body() -> None:
        rec = FakeRecorder()
        s = AsyncServer()
        await s.start(recorder=rec)
        try:
            await asyncio.gather(*[s.unary("Ping")(f"t{i}".encode()) for i in range(10)])
            assert len(rec.events) == 10
            assert len({e.id for e in rec.events}) == 10
        finally:
            await s.close()

    run_async(body())


# ---------------------------------------------------------------------------
# Exhaustive 17-code mapping (unit level, no server needed)
# ---------------------------------------------------------------------------

_ALL_17_CODE_CASES = [
    (grpc.StatusCode.OK, 200),
    (grpc.StatusCode.CANCELLED, 499),
    (grpc.StatusCode.UNKNOWN, 500),
    (grpc.StatusCode.INVALID_ARGUMENT, 400),
    (grpc.StatusCode.DEADLINE_EXCEEDED, 504),
    (grpc.StatusCode.NOT_FOUND, 404),
    (grpc.StatusCode.ALREADY_EXISTS, 409),
    (grpc.StatusCode.PERMISSION_DENIED, 403),
    (grpc.StatusCode.RESOURCE_EXHAUSTED, 429),
    (grpc.StatusCode.FAILED_PRECONDITION, 400),
    (grpc.StatusCode.ABORTED, 409),
    (grpc.StatusCode.OUT_OF_RANGE, 400),
    (grpc.StatusCode.UNIMPLEMENTED, 501),
    (grpc.StatusCode.INTERNAL, 500),
    (grpc.StatusCode.UNAVAILABLE, 503),
    (grpc.StatusCode.DATA_LOSS, 500),
    (grpc.StatusCode.UNAUTHENTICATED, 401),
]


def test_all_17_grpc_status_codes_are_covered() -> None:
    assert len(_ALL_17_CODE_CASES) == 17
    assert len(set(grpc.StatusCode)) == 17


@pytest.mark.parametrize("code,expected_http", _ALL_17_CODE_CASES)
def test_http_status_for_grpc_code(code: Any, expected_http: int) -> None:
    assert http_status_for_grpc_code(code) == expected_http


@pytest.mark.parametrize("code,expected_http", _ALL_17_CODE_CASES)
def test_result_from_grpc_status(code: Any, expected_http: int) -> None:
    result = result_from_grpc_status(code, "some details")
    assert result.code == expected_http
    if code == grpc.StatusCode.OK:
        assert result.status == "ok"
        assert result.message is None  # OK never carries framework boilerplate
    elif expected_http in (401, 403):
        assert result.status == "denied"
        assert result.message == "some details"
    else:
        assert result.status == "error"
        assert result.message == "some details"
