"""Tests for framework-agnostic origin extraction."""

from __future__ import annotations

from everscribe.event import Origin, client_ip, origin_from_request


def test_origin_from_headers_and_remote_addr() -> None:
    o = origin_from_request(
        {"User-Agent": "curl/8", "X-Request-ID": "req-1"}, "9.9.9.9"
    )
    assert o == Origin(ip="9.9.9.9", user_agent="curl/8", request_id="req-1")


def test_origin_empty_when_no_data() -> None:
    assert origin_from_request(None) == Origin()
    assert origin_from_request({}) == Origin()


def test_client_ip_prefers_xff_first_entry() -> None:
    ip = client_ip({"X-Forwarded-For": "1.1.1.1, 2.2.2.2, 3.3.3.3"}, "9.9.9.9")
    assert ip == "1.1.1.1"


def test_client_ip_xff_single_value_trimmed() -> None:
    assert client_ip({"x-forwarded-for": "  4.4.4.4 "}) == "4.4.4.4"


def test_client_ip_falls_back_to_x_real_ip() -> None:
    assert client_ip({"X-Real-IP": "5.5.5.5"}, "9.9.9.9") == "5.5.5.5"


def test_client_ip_xff_beats_x_real_ip() -> None:
    assert client_ip({"X-Forwarded-For": "1.1.1.1", "X-Real-IP": "5.5.5.5"}) == "1.1.1.1"


def test_client_ip_falls_back_to_remote_addr() -> None:
    assert client_ip({}, "6.6.6.6") == "6.6.6.6"


def test_client_ip_strips_port_from_remote_addr() -> None:
    assert client_ip({}, "6.6.6.6:54321") == "6.6.6.6"


def test_client_ip_empty_when_nothing_available() -> None:
    assert client_ip({}) == ""


def test_header_lookup_is_case_insensitive() -> None:
    assert client_ip({"X-REAL-IP": "7.7.7.7"}) == "7.7.7.7"
    assert client_ip({"x-real-ip": "7.7.7.7"}) == "7.7.7.7"
