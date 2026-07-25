"""Tests for HTTPRecorder: request shape, batching, errors, idempotency, and
a real-server integration test exercising the urllib transport."""

from __future__ import annotations

import json
import threading
from collections.abc import Mapping
from http.server import BaseHTTPRequestHandler, HTTPServer
from typing import ClassVar

import pytest

from everscribe.event import Event
from everscribe.recorder import HTTPRecorder
from everscribe.recorder.errors import HTTPError


class RecordingTransport:
    """Captures posted requests and returns a canned response."""

    def __init__(self, status: int = 202, body: bytes = b"") -> None:
        self.calls: list[tuple[str, bytes, Mapping[str, str], float]] = []
        self.status = status
        self.body = body

    def __call__(
        self, url: str, body: bytes, headers: Mapping[str, str], timeout: float
    ) -> tuple[int, bytes]:
        self.calls.append((url, body, headers, timeout))
        return self.status, self.body

    def last_json(self) -> dict:
        return json.loads(self.calls[-1][1])


def test_record_posts_single_event() -> None:
    t = RecordingTransport()
    r = HTTPRecorder("proj1", "key1", base_url="https://x.test", transport=t)
    r.record(Event("user.login"))
    url, _body, headers, timeout = t.calls[0]
    assert url == "https://x.test/v1/projects/proj1/events"
    assert headers["Authorization"] == "Bearer key1"
    assert headers["Content-Type"] == "application/json"
    assert timeout == 10.0
    assert t.last_json()["action"] == "user.login"


def test_base_url_trailing_slash_trimmed() -> None:
    t = RecordingTransport()
    r = HTTPRecorder("p", "k", base_url="https://x.test/", transport=t)
    r.record(Event("a"))
    assert t.calls[0][0] == "https://x.test/v1/projects/p/events"


def test_empty_action_is_noop() -> None:
    t = RecordingTransport()
    r = HTTPRecorder("p", "k", transport=t)
    r.record(Event())
    assert t.calls == []


def test_record_batch_wraps_in_events_key_and_filters_empty() -> None:
    t = RecordingTransport()
    r = HTTPRecorder("p", "k", base_url="https://x.test", transport=t)
    r.record_batch([Event("a"), Event(), Event("b")])
    url, _body, _, _ = t.calls[0]
    assert url == "https://x.test/v1/projects/p/events/batch"
    payload = t.last_json()
    assert [e["action"] for e in payload["events"]] == ["a", "b"]


def test_record_batch_all_empty_is_noop() -> None:
    t = RecordingTransport()
    r = HTTPRecorder("p", "k", transport=t)
    r.record_batch([Event(), Event()])
    r.record_batch([])
    assert t.calls == []


def test_non_2xx_raises_http_error() -> None:
    t = RecordingTransport(status=400, body=b"  bad request  ")
    r = HTTPRecorder("p", "k", transport=t)
    with pytest.raises(HTTPError) as ei:
        r.record(Event("a"))
    assert ei.value.status_code == 400
    assert ei.value.body == "bad request"  # trimmed
    assert ei.value.transient() is False


def test_transient_classification() -> None:
    for status in (500, 502, 503, 429):
        assert HTTPError(status, "").transient() is True
    for status in (400, 401, 403, 404):
        assert HTTPError(status, "").transient() is False


def test_prepare_event_fills_id_before_send() -> None:
    t = RecordingTransport()
    r = HTTPRecorder("p", "k", transport=t)
    e = Event("a")
    e.id = ""
    r.record(e)
    assert t.last_json()["id"]  # non-empty


def test_auto_idempotency_key_copies_id() -> None:
    t = RecordingTransport()
    r = HTTPRecorder("p", "k", transport=t, auto_idempotency_key=True)
    r.record(Event("a"))
    j = t.last_json()
    assert j["idempotency_key"] == j["id"]


def test_auto_idempotency_key_respects_explicit_key() -> None:
    t = RecordingTransport()
    r = HTTPRecorder("p", "k", transport=t, auto_idempotency_key=True)
    e = Event("a")
    e.idempotency_key = "explicit"
    r.record(e)
    assert t.last_json()["idempotency_key"] == "explicit"


def test_auto_idempotency_off_by_default() -> None:
    t = RecordingTransport()
    r = HTTPRecorder("p", "k", transport=t)
    r.record(Event("a"))
    assert "idempotency_key" not in t.last_json()


def test_timeout_override_passed_to_transport() -> None:
    t = RecordingTransport()
    r = HTTPRecorder("p", "k", transport=t, request_timeout=5.0)
    r.record(Event("a"), timeout=1.5)
    assert t.calls[0][3] == 1.5


# --- real-server integration (exercises the urllib transport) --------------


class _Handler(BaseHTTPRequestHandler):
    received: ClassVar[list[tuple[str, bytes, str]]] = []
    reply_status = 202

    def do_POST(self) -> None:
        length = int(self.headers.get("Content-Length", "0"))
        body = self.rfile.read(length)
        auth = self.headers.get("Authorization", "")
        type(self).received.append((self.path, body, auth))
        self.send_response(type(self).reply_status)
        self.end_headers()
        self.wfile.write(b"")

    def log_message(self, *args: object) -> None:  # silence test noise
        pass


def _serve() -> tuple[HTTPServer, str]:
    server = HTTPServer(("127.0.0.1", 0), _Handler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    host, port = server.server_address
    return server, f"http://{host}:{port}"


def test_urllib_transport_against_real_server() -> None:
    _Handler.received = []
    _Handler.reply_status = 202
    server, base_url = _serve()
    try:
        r = HTTPRecorder("projX", "secret", base_url=base_url)
        r.record(Event("user.login").with_field("k", "v"))
    finally:
        server.shutdown()
    assert len(_Handler.received) == 1
    path, body, auth = _Handler.received[0]
    assert path == "/v1/projects/projX/events"
    assert auth == "Bearer secret"
    assert json.loads(body)["action"] == "user.login"


def test_urllib_transport_non_2xx_raises() -> None:
    _Handler.received = []
    _Handler.reply_status = 401
    server, base_url = _serve()
    try:
        r = HTTPRecorder("p", "k", base_url=base_url)
        with pytest.raises(HTTPError) as ei:
            r.record(Event("a"))
    finally:
        server.shutdown()
    assert ei.value.status_code == 401
