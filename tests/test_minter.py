"""Tests for the minter: option validation, wire shape, and the mint flow."""

from __future__ import annotations

import json
import threading
from collections.abc import Mapping
from datetime import timedelta
from http.server import BaseHTTPRequestHandler, HTTPServer
from typing import ClassVar

import pytest

from everscribe.minter import (
    MAX_EXPIRES_IN,
    MIN_EXPIRES_IN,
    Client,
    MinterError,
    TokenOptions,
    token_options_to_wire,
)

# --- validation / wire -----------------------------------------------------


def test_default_options_produce_empty_wire() -> None:
    assert token_options_to_wire(TokenOptions()) == {}


def test_tenant_id_trimmed() -> None:
    assert token_options_to_wire(TokenOptions(tenant_id="  t1 ")) == {"tenant_id": "t1"}


def test_tenant_id_empty_after_trim_rejected() -> None:
    with pytest.raises(ValueError, match="empty after trim"):
        token_options_to_wire(TokenOptions(tenant_id="   "))


def test_tenant_id_too_long_rejected() -> None:
    with pytest.raises(ValueError, match="256"):
        token_options_to_wire(TokenOptions(tenant_id="x" * 257))


def test_tenant_id_max_length_ok() -> None:
    out = token_options_to_wire(TokenOptions(tenant_id="x" * 256))
    assert out["tenant_id"] == "x" * 256


def test_expires_in_seconds_to_wire() -> None:
    assert token_options_to_wire(TokenOptions(expires_in=3600))["expires_in"] == 3600


def test_expires_in_timedelta_accepted() -> None:
    out = token_options_to_wire(TokenOptions(expires_in=timedelta(minutes=5)))
    assert out["expires_in"] == 300


def test_expires_in_zero_omitted() -> None:
    assert "expires_in" not in token_options_to_wire(TokenOptions(expires_in=0))


def test_expires_in_below_min_rejected() -> None:
    with pytest.raises(ValueError, match="below minimum"):
        token_options_to_wire(TokenOptions(expires_in=MIN_EXPIRES_IN - 1))


def test_expires_in_above_max_rejected() -> None:
    with pytest.raises(ValueError, match="above maximum"):
        token_options_to_wire(TokenOptions(expires_in=MAX_EXPIRES_IN + 1))


def test_expires_in_bounds_inclusive() -> None:
    assert token_options_to_wire(TokenOptions(expires_in=MIN_EXPIRES_IN))
    assert token_options_to_wire(TokenOptions(expires_in=MAX_EXPIRES_IN))


def test_allowed_columns_valid() -> None:
    out = token_options_to_wire(TokenOptions(allowed_columns=["actor", "action"]))
    assert out["columns"] == ["actor", "action"]


def test_allowed_columns_empty_rejected() -> None:
    with pytest.raises(ValueError, match="empty; pass None"):
        token_options_to_wire(TokenOptions(allowed_columns=[]))


def test_allowed_columns_unknown_rejected() -> None:
    with pytest.raises(ValueError, match="unknown column"):
        token_options_to_wire(TokenOptions(allowed_columns=["actor", "bogus"]))


def test_allowed_columns_none_omitted() -> None:
    assert "columns" not in token_options_to_wire(TokenOptions(allowed_columns=None))


@pytest.mark.parametrize(
    "action",
    ["user.login", "user.*", "a", "a.b.c", "api_key.revoke", "a1_b2.c3"],
)
def test_allowed_actions_valid_grammar(action: str) -> None:
    out = token_options_to_wire(TokenOptions(allowed_actions=[action]))
    assert out["actions"] == [action]


@pytest.mark.parametrize(
    "action",
    ["*", "*.create", "user.*.create", "user*", "user.", ".user", "user.log-in", ""],
)
def test_allowed_actions_invalid_grammar_rejected(action: str) -> None:
    with pytest.raises(ValueError, match="does not match grammar"):
        token_options_to_wire(TokenOptions(allowed_actions=[action]))


def test_allowed_actions_empty_rejected() -> None:
    with pytest.raises(ValueError, match="empty; pass None"):
        token_options_to_wire(TokenOptions(allowed_actions=[]))


def test_allowed_fields_passed_through() -> None:
    out = token_options_to_wire(TokenOptions(allowed_fields=["metadata.plan"]))
    assert out["allowed_fields"] == ["metadata.plan"]


def test_allowed_fields_empty_rejected() -> None:
    with pytest.raises(ValueError, match="empty; pass None"):
        token_options_to_wire(TokenOptions(allowed_fields=[]))


def test_allow_flags_only_when_true() -> None:
    assert token_options_to_wire(TokenOptions()) == {}
    assert token_options_to_wire(TokenOptions(allow_dsl_input=True)) == {
        "allow_dsl_input": True
    }
    assert token_options_to_wire(TokenOptions(allow_nlp=True)) == {"allow_nlp": True}


def test_full_wire_shape() -> None:
    out = token_options_to_wire(
        TokenOptions(
            tenant_id="acme",
            expires_in=1800,
            allowed_columns=["actor", "action", "occurred_at"],
            allowed_actions=["user.*", "billing.charge"],
            allowed_fields=["actor.email"],
            allow_dsl_input=True,
            allow_nlp=True,
        )
    )
    assert out == {
        "tenant_id": "acme",
        "expires_in": 1800,
        "columns": ["actor", "action", "occurred_at"],
        "actions": ["user.*", "billing.charge"],
        "allowed_fields": ["actor.email"],
        "allow_dsl_input": True,
        "allow_nlp": True,
    }


# --- client (injected transport) -------------------------------------------


class RecordingTransport:
    def __init__(self, status: int, body: bytes) -> None:
        self.calls: list[tuple[str, bytes, Mapping[str, str], float]] = []
        self.status = status
        self.body = body

    def __call__(
        self, url: str, body: bytes, headers: Mapping[str, str], timeout: float
    ) -> tuple[int, bytes]:
        self.calls.append((url, body, headers, timeout))
        return self.status, self.body


def test_mint_token_success_returns_jwt() -> None:
    t = RecordingTransport(201, json.dumps({"token": "jwt.abc"}).encode())
    c = Client("proj1", "key1", base_url="https://x.test", transport=t)
    token = c.mint_token(TokenOptions(tenant_id="acme"))
    assert token == "jwt.abc"
    url, body, headers, _timeout = t.calls[0]
    assert url == "https://x.test/v1/projects/proj1/embed-tokens"
    assert headers["Authorization"] == "Bearer key1"
    assert json.loads(body) == {"tenant_id": "acme"}


def test_mint_token_defaults_when_no_opts() -> None:
    t = RecordingTransport(201, json.dumps({"token": "j"}).encode())
    c = Client("p", "k", transport=t)
    assert c.mint_token() == "j"
    assert json.loads(t.calls[0][1]) == {}


def test_mint_token_validation_error_makes_no_http_call() -> None:
    t = RecordingTransport(201, b"{}")
    c = Client("p", "k", transport=t)
    with pytest.raises(ValueError):
        c.mint_token(TokenOptions(allowed_columns=[]))
    assert t.calls == []


def test_mint_token_non_201_raises_minter_error() -> None:
    t = RecordingTransport(400, b"  bad options  ")
    c = Client("p", "k", transport=t)
    with pytest.raises(MinterError) as ei:
        c.mint_token()
    assert ei.value.status_code == 400
    assert ei.value.body == "bad options"


def test_mint_token_timeout_override() -> None:
    t = RecordingTransport(201, json.dumps({"token": "j"}).encode())
    c = Client("p", "k", transport=t, request_timeout=5.0)
    c.mint_token(timeout=2.0)
    assert t.calls[0][3] == 2.0


# --- real-server integration -----------------------------------------------


class _Handler(BaseHTTPRequestHandler):
    received: ClassVar[list[tuple[str, bytes, str]]] = []
    reply_status = 201
    reply_body = b'{"token": "real.jwt"}'

    def do_POST(self) -> None:
        length = int(self.headers.get("Content-Length", "0"))
        body = self.rfile.read(length)
        type(self).received.append(
            (self.path, body, self.headers.get("Authorization", ""))
        )
        self.send_response(type(self).reply_status)
        self.end_headers()
        self.wfile.write(type(self).reply_body)

    def log_message(self, *args: object) -> None:
        pass


def _serve() -> tuple[HTTPServer, str]:
    server = HTTPServer(("127.0.0.1", 0), _Handler)
    threading.Thread(target=server.serve_forever, daemon=True).start()
    host, port = server.server_address
    return server, f"http://{host}:{port}"


def test_urllib_mint_against_real_server() -> None:
    _Handler.received = []
    _Handler.reply_status = 201
    _Handler.reply_body = b'{"token": "real.jwt"}'
    server, base_url = _serve()
    try:
        c = Client("projX", "secret", base_url=base_url)
        token = c.mint_token(TokenOptions(expires_in=900))
    finally:
        server.shutdown()
    assert token == "real.jwt"
    path, body, auth = _Handler.received[0]
    assert path == "/v1/projects/projX/embed-tokens"
    assert auth == "Bearer secret"
    assert json.loads(body) == {"expires_in": 900}
