"""Tests for the ASGI adapter, exercised through Starlette's TestClient."""

from __future__ import annotations

from typing import List, Optional

from starlette.applications import Starlette
from starlette.responses import PlainTextResponse, Response
from starlette.routing import Route
from starlette.testclient import TestClient

from everscribe.asgi import (
    ActorResolver,
    EverscribeMiddleware,
    current_event,
    event_from_request,
)
from everscribe.event import Actor, Event, Result, from_context


class FakeRecorder:
    def __init__(self) -> None:
        self.events: List[Event] = []

    def record(self, e: Event) -> None:
        self.events.append(e)


def make_app(
    routes: list,
    *,
    recorder: "Optional[FakeRecorder]" = None,
    resolve_actor: "Optional[ActorResolver]" = None,
) -> Starlette:
    app = Starlette(routes=routes)
    app.add_middleware(
        EverscribeMiddleware, recorder=recorder, resolve_actor=resolve_actor
    )
    return app


def test_auto_record_on_finish() -> None:
    rec = FakeRecorder()

    async def login(request):  # type: ignore[no-untyped-def]
        current_event().action = "user.login"
        return PlainTextResponse("ok")

    client = TestClient(make_app([Route("/login", login)], recorder=rec))
    r = client.get("/login")
    assert r.status_code == 200
    assert len(rec.events) == 1
    e = rec.events[0]
    assert e.action == "user.login"
    assert e.actor.type == "anonymous"
    assert e.result.status == "ok"
    assert e.result.code == 200


def test_empty_action_is_not_recorded() -> None:
    rec = FakeRecorder()

    async def noop(request):  # type: ignore[no-untyped-def]
        return PlainTextResponse("x")

    client = TestClient(make_app([Route("/", noop)], recorder=rec))
    client.get("/")
    assert rec.events == []


def test_resolver_sets_actor() -> None:
    rec = FakeRecorder()

    def resolver(req):  # type: ignore[no-untyped-def]
        return Actor(type="user", id="u1", email="a@b.c")

    async def h(request):  # type: ignore[no-untyped-def]
        current_event().action = "a"
        return PlainTextResponse("")

    client = TestClient(make_app([Route("/", h)], recorder=rec, resolve_actor=resolver))
    client.get("/")
    assert rec.events[0].actor == Actor(type="user", id="u1", email="a@b.c")


def test_explicit_result_wins_over_status() -> None:
    rec = FakeRecorder()

    async def h(request):  # type: ignore[no-untyped-def]
        ev = current_event()
        ev.action = "a"
        ev.result = Result(status="denied", code=418)
        return PlainTextResponse("ok")  # 200

    client = TestClient(make_app([Route("/", h)], recorder=rec))
    client.get("/")
    assert rec.events[0].result.status == "denied"
    assert rec.events[0].result.code == 418


def test_origin_from_request() -> None:
    rec = FakeRecorder()

    async def h(request):  # type: ignore[no-untyped-def]
        current_event().action = "a"
        return PlainTextResponse("")

    client = TestClient(make_app([Route("/", h)], recorder=rec))
    client.get("/", headers={"X-Request-ID": "req-1"})
    origin = rec.events[0].origin
    assert origin.request_id == "req-1"
    assert origin.user_agent  # TestClient sends a user-agent
    assert origin.ip  # TestClient sets a client host


def test_status_500_maps_to_error() -> None:
    rec = FakeRecorder()

    async def h(request):  # type: ignore[no-untyped-def]
        current_event().action = "a"
        return Response(status_code=500)

    client = TestClient(make_app([Route("/", h)], recorder=rec))
    client.get("/")
    assert rec.events[0].result.status == "error"
    assert rec.events[0].result.code == 500


def test_status_403_maps_to_denied() -> None:
    rec = FakeRecorder()

    async def h(request):  # type: ignore[no-untyped-def]
        current_event().action = "a"
        return Response(status_code=403)

    client = TestClient(make_app([Route("/", h)], recorder=rec))
    client.get("/")
    assert rec.events[0].result.status == "denied"
    assert rec.events[0].result.code == 403


def test_no_recorder_still_installs_event() -> None:
    async def h(request):  # type: ignore[no-untyped-def]
        current_event().action = "a"  # no crash even without a recorder
        return PlainTextResponse("ok")

    client = TestClient(make_app([Route("/", h)]))
    assert client.get("/").status_code == 200


def test_sync_endpoint_context_propagates_to_threadpool() -> None:
    rec = FakeRecorder()

    def sync_h(request):  # sync def -> runs in a threadpool
        current_event().action = "sync.event"
        return PlainTextResponse("ok")

    client = TestClient(make_app([Route("/", sync_h)], recorder=rec))
    client.get("/")
    assert rec.events[0].action == "sync.event"


def test_multiple_events_via_from_context() -> None:
    rec = FakeRecorder()

    async def multi(request):  # type: ignore[no-untyped-def]
        current_event().action = "primary"
        extra = from_context()  # fresh clone of the template
        extra.action = "extra"
        rec.record(extra)  # recorded manually
        return PlainTextResponse("ok")

    client = TestClient(make_app([Route("/", multi)], recorder=rec))
    client.get("/")
    # extra (manual) + primary (auto)
    assert {e.action for e in rec.events} == {"primary", "extra"}
    assert all(e.actor.type == "anonymous" for e in rec.events)


def test_event_from_request_is_the_recorded_event() -> None:
    rec = FakeRecorder()

    async def h(request):  # type: ignore[no-untyped-def]
        event_from_request(request).action = "via.state"
        return PlainTextResponse("ok")

    client = TestClient(make_app([Route("/", h)], recorder=rec))
    client.get("/")
    assert rec.events[0].action == "via.state"


def test_independent_events_across_requests() -> None:
    rec = FakeRecorder()

    async def h(request):  # type: ignore[no-untyped-def]
        current_event().action = "a"
        current_event().with_field("path", request.url.path)
        return PlainTextResponse("ok")

    client = TestClient(make_app([Route("/{p}", h)], recorder=rec))
    client.get("/one")
    client.get("/two")
    assert len(rec.events) == 2
    assert rec.events[0].id != rec.events[1].id
    assert rec.events[0].metadata != rec.events[1].metadata


def test_lifespan_passthrough() -> None:
    rec = FakeRecorder()

    async def h(request):  # type: ignore[no-untyped-def]
        return PlainTextResponse("ok")

    # Entering the context manager runs the (non-http) lifespan scope, which
    # the middleware must pass through untouched.
    with TestClient(make_app([Route("/", h)], recorder=rec)) as client:
        assert client.get("/").status_code == 200
