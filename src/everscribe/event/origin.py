"""Framework-agnostic HTTP request origin extraction.

Takes a plain header mapping and an optional remote address so any framework
(or the bundled ASGI adapter) can feed it.
"""

from __future__ import annotations

from typing import Mapping

from .types import Origin


def origin_from_request(
    headers: "Mapping[str, str] | None", remote_addr: str = ""
) -> Origin:
    """Extract network context from request headers and a remote address.
    Respects ``X-Forwarded-For`` (first entry) and ``X-Real-IP`` before
    falling back to ``remote_addr``. Callers behind a proxy should sanitize
    untrusted client-supplied headers upstream."""
    o = Origin()
    if headers is None:
        headers = {}
    o.ip = client_ip(headers, remote_addr)
    o.user_agent = _header(headers, "user-agent")
    o.request_id = _header(headers, "x-request-id")
    return o


def client_ip(headers: "Mapping[str, str] | None", remote_addr: str = "") -> str:
    """Extract the client IP from ``X-Forwarded-For`` (first entry), then
    ``X-Real-IP``, then the supplied ``remote_addr`` (with a trailing
    ``:port`` stripped)."""
    if headers is None:
        headers = {}
    xff = _header(headers, "x-forwarded-for")
    if xff:
        comma = xff.find(",")
        return (xff[:comma] if comma >= 0 else xff).strip()
    xri = _header(headers, "x-real-ip")
    if xri:
        return xri
    if not remote_addr:
        return ""
    last_colon = remote_addr.rfind(":")
    return remote_addr[:last_colon] if last_colon >= 0 else remote_addr


def _header(headers: "Mapping[str, str]", name: str) -> str:
    """Case-insensitive single-header lookup. Returns "" if absent."""
    value = headers.get(name)
    if value is not None:
        return value
    for k, v in headers.items():
        if k.lower() == name:
            return v
    return ""


__all__ = ["origin_from_request", "client_ip"]
