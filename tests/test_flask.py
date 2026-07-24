"""Tests for the Flask adapter, exercised through Flask's test client."""

from __future__ import annotations

from typing import List, Optional

from flask import Flask, g

from everscribe.event import Actor, Event, current_event
from everscribe.flask import ActorResolver, EverscribeFlask


class FakeRecorder:
    def __init__(self) -> None:
        self.events: List[Event] = []

    def record(self, e: Event) -> None:
        self.events.append(e)


def make_app(
    *,
    recorder: "Optional[FakeRecorder]" = None,
    resolve_actor: "Optional[ActorResolver]" = None,
    testing: bool = True,
) -> Flask:
    app = Flask(__name__)
    app.testing = testing
    EverscribeFlask(app, recorder=recorder, resolve_actor=resolve_actor)
    return app


def test_auto_record_on_finish() -> None:
    rec = FakeRecorder()
    app = make_app(recorder=rec)

    @app.route("/login")
    def login():  # type: ignore[no-untyped-def]
        current_event().action = "user.login"
        return "ok", 200

    client = app.test_client()
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
    app = make_app(recorder=rec)

    @app.route("/")
    def noop():  # type: ignore[no-untyped-def]
        return "x"

    client = app.test_client()
    client.get("/")
    assert rec.events == []


def test_resolver_sets_actor() -> None:
    rec = FakeRecorder()

    def resolver() -> Actor:
        return Actor(type="user", id="u1", email="a@b.c")

    app = make_app(recorder=rec, resolve_actor=resolver)

    @app.route("/")
    def h():  # type: ignore[no-untyped-def]
        current_event().action = "a"
        return ""

    client = app.test_client()
    client.get("/")
    assert rec.events[0].actor == Actor(type="user", id="u1", email="a@b.c")


def test_resolver_reads_flask_g() -> None:
    """The resolver runs inside before_request, so it must be able to read
    app-context state set earlier in the before_request chain (simulating
    flask.g / flask_login.current_user). This is the whole reason this
    adapter hooks before_request instead of wrapping app.wsgi_app: a WSGI
    wrapper runs outside the app context and could never see this."""
    rec = FakeRecorder()

    def resolver() -> Actor:
        return Actor(type="user", id=g.user_id)

    app = Flask(__name__)
    app.testing = True

    # Registered before the extension, so it runs first: Flask runs
    # before_request functions in registration order, and g.user_id must
    # exist by the time resolve_actor runs.
    @app.before_request
    def stash_user() -> None:
        g.user_id = "from-g"

    EverscribeFlask(app, recorder=rec, resolve_actor=resolver)

    @app.route("/")
    def h():  # type: ignore[no-untyped-def]
        current_event().action = "a"
        return ""

    client = app.test_client()
    client.get("/")
    assert rec.events[0].actor.id == "from-g"


def test_status_403_maps_to_denied() -> None:
    rec = FakeRecorder()
    app = make_app(recorder=rec)

    @app.route("/")
    def h():  # type: ignore[no-untyped-def]
        current_event().action = "a"
        return "", 403

    client = app.test_client()
    client.get("/")
    assert rec.events[0].result.status == "denied"
    assert rec.events[0].result.code == 403


def test_status_500_maps_to_error() -> None:
    rec = FakeRecorder()
    app = make_app(recorder=rec)

    @app.route("/")
    def h():  # type: ignore[no-untyped-def]
        current_event().action = "a"
        return "", 500

    client = app.test_client()
    client.get("/")
    assert rec.events[0].result.status == "error"
    assert rec.events[0].result.code == 500


def test_view_that_raises_still_records_via_teardown() -> None:
    """The whole reason this adapter records on teardown_request instead of
    after_request: after_request is skipped when a view raises and the
    exception propagates (the default under app.testing), but teardown_request
    always runs."""
    rec = FakeRecorder()
    app = make_app(recorder=rec, testing=True)

    @app.route("/boom")
    def boom():  # type: ignore[no-untyped-def]
        current_event().action = "user.crash"
        raise ValueError("kaboom")

    client = app.test_client()
    try:
        client.get("/boom")
    except ValueError:
        pass  # propagates out under app.testing, as Flask documents

    assert len(rec.events) == 1
    e = rec.events[0]
    assert e.action == "user.crash"
    assert e.result.status == "error"
    assert "kaboom" in str(e.result.message)


def test_no_recorder_still_installs_event() -> None:
    app = make_app()

    @app.route("/")
    def h():  # type: ignore[no-untyped-def]
        current_event().action = "a"  # no crash even without a recorder
        return "ok"

    client = app.test_client()
    assert client.get("/").status_code == 200


def test_independent_events_across_requests() -> None:
    rec = FakeRecorder()
    app = make_app(recorder=rec)

    @app.route("/<p>")
    def h(p: str):  # type: ignore[no-untyped-def]
        current_event().action = "a"
        current_event().with_field("path", p)
        return "ok"

    client = app.test_client()
    client.get("/one")
    client.get("/two")
    assert len(rec.events) == 2
    assert rec.events[0].id != rec.events[1].id
    assert rec.events[0].metadata != rec.events[1].metadata


def test_origin_from_request() -> None:
    rec = FakeRecorder()
    app = make_app(recorder=rec)

    @app.route("/")
    def h():  # type: ignore[no-untyped-def]
        current_event().action = "a"
        return ""

    client = app.test_client()
    client.get("/", headers={"X-Request-ID": "req-1"})
    origin = rec.events[0].origin
    assert origin.request_id == "req-1"
