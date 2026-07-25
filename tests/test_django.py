"""Tests for the Django adapter, exercised through django.test.RequestFactory
and the middleware's __init__(get_response) / __call__(request) directly -
the same shape Django's own middleware loader uses, minus the full
MIDDLEWARE-list/URL-routing ceremony a real project would add."""

from __future__ import annotations

import django
from django.conf import settings

if not settings.configured:
    settings.configure(DEBUG=True, ALLOWED_HOSTS=["testserver"], USE_TZ=True)
    django.setup()

from django.http import HttpResponse
from django.test import RequestFactory

from everscribe.django import ActorResolver, EverscribeMiddleware, event_from_request
from everscribe.event import Actor, Event, current_event

rf = RequestFactory()


class FakeRecorder:
    def __init__(self) -> None:
        self.events: list[Event] = []

    def record(self, e: Event) -> None:
        self.events.append(e)


class FakeUser:
    def __init__(self, id: str) -> None:
        self.id = id
        self.is_authenticated = True


def test_auto_record_on_finish() -> None:
    rec = FakeRecorder()

    def view(request):  # type: ignore[no-untyped-def]
        current_event().action = "user.login"
        return HttpResponse("ok", status=200)

    mw = EverscribeMiddleware(view, recorder=rec)
    request = rf.get("/login")
    response = mw(request)

    assert response.status_code == 200
    assert len(rec.events) == 1
    e = rec.events[0]
    assert e.action == "user.login"
    assert e.actor.type == "anonymous"
    assert e.result.status == "ok"
    assert e.result.code == 200


def test_empty_action_is_not_recorded() -> None:
    rec = FakeRecorder()

    def view(request):  # type: ignore[no-untyped-def]
        return HttpResponse("x")

    mw = EverscribeMiddleware(view, recorder=rec)
    mw(rf.get("/"))
    assert rec.events == []


def test_resolver_reads_request_user() -> None:
    """The middleware runs after AuthenticationMiddleware in a real project,
    so resolve_actor must be able to read request.user - simulated here by
    setting it directly, the same thing AuthenticationMiddleware would have
    done earlier in the chain."""
    rec = FakeRecorder()

    def resolver(request) -> Actor:  # type: ignore[no-untyped-def]
        return Actor(type="user", id=request.user.id)

    def view(request):  # type: ignore[no-untyped-def]
        current_event().action = "a"
        return HttpResponse("")

    mw = EverscribeMiddleware(view, recorder=rec, resolve_actor=resolver)
    request = rf.get("/")
    request.user = FakeUser(id="u1")
    mw(request)
    assert rec.events[0].actor == Actor(type="user", id="u1")


def test_status_403_maps_to_denied() -> None:
    rec = FakeRecorder()

    def view(request):  # type: ignore[no-untyped-def]
        current_event().action = "a"
        return HttpResponse(status=403)

    mw = EverscribeMiddleware(view, recorder=rec)
    mw(rf.get("/"))
    assert rec.events[0].result.status == "denied"
    assert rec.events[0].result.code == 403


def test_status_500_maps_to_error() -> None:
    rec = FakeRecorder()

    def view(request):  # type: ignore[no-untyped-def]
        current_event().action = "a"
        return HttpResponse(status=500)

    mw = EverscribeMiddleware(view, recorder=rec)
    mw(rf.get("/"))
    assert rec.events[0].result.status == "error"
    assert rec.events[0].result.code == 500


def test_unhandled_exception_still_records_and_reraises() -> None:
    rec = FakeRecorder()

    def view(request):  # type: ignore[no-untyped-def]
        current_event().action = "user.crash"
        raise ValueError("kaboom")

    mw = EverscribeMiddleware(view, recorder=rec)
    try:
        mw(rf.get("/boom"))
        raised = False
    except ValueError:
        raised = True

    assert raised
    assert len(rec.events) == 1
    e = rec.events[0]
    assert e.action == "user.crash"
    assert e.result.status == "error"
    assert "kaboom" in str(e.result.message)


def test_no_recorder_still_installs_event() -> None:
    def view(request):  # type: ignore[no-untyped-def]
        current_event().action = "a"  # no crash even without a recorder
        return HttpResponse("ok")

    mw = EverscribeMiddleware(view)
    assert mw(rf.get("/")).status_code == 200


def test_settings_provide_recorder_and_resolver() -> None:
    """recorder/resolve_actor/logger may come from Django settings instead
    of constructor kwargs - the only wiring seam Django's own middleware
    loader (`Middleware(get_response)`) leaves available in a real project."""
    rec = FakeRecorder()
    settings.EVERSCRIBE_RECORDER = rec
    settings.EVERSCRIBE_RESOLVE_ACTOR = lambda request: Actor(type="user", id="from-settings")
    try:

        def view(request):  # type: ignore[no-untyped-def]
            current_event().action = "a"
            return HttpResponse("ok")

        mw = EverscribeMiddleware(view)
        mw(rf.get("/"))
        assert rec.events[0].actor.id == "from-settings"
    finally:
        del settings.EVERSCRIBE_RECORDER
        del settings.EVERSCRIBE_RESOLVE_ACTOR


def test_origin_from_request() -> None:
    rec = FakeRecorder()

    def view(request):  # type: ignore[no-untyped-def]
        current_event().action = "a"
        return HttpResponse("")

    mw = EverscribeMiddleware(view, recorder=rec)
    mw(rf.get("/", HTTP_X_REQUEST_ID="req-1"))
    origin = rec.events[0].origin
    assert origin.request_id == "req-1"
    assert origin.ip  # RequestFactory sets REMOTE_ADDR


def test_event_from_request_is_the_recorded_event() -> None:
    rec = FakeRecorder()

    def view(request):  # type: ignore[no-untyped-def]
        event_from_request(request).action = "via.request"
        return HttpResponse("ok")

    mw = EverscribeMiddleware(view, recorder=rec)
    mw(rf.get("/"))
    assert rec.events[0].action == "via.request"


def test_independent_events_across_requests() -> None:
    rec = FakeRecorder()

    def view(request):  # type: ignore[no-untyped-def]
        current_event().action = "a"
        current_event().with_field("path", request.path)
        return HttpResponse("ok")

    mw = EverscribeMiddleware(view, recorder=rec)
    mw(rf.get("/one"))
    mw(rf.get("/two"))
    assert len(rec.events) == 2
    assert rec.events[0].id != rec.events[1].id
    assert rec.events[0].metadata != rec.events[1].metadata


def test_actor_resolver_type() -> None:
    resolver: ActorResolver = lambda request: Actor(type="anonymous")  # noqa: E731
    assert resolver(rf.get("/")).type == "anonymous"
