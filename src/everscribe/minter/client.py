"""The minter client: exchanges a project API key for short-lived embed
tokens.

An embed token is a signed, short-lived JWT your backend mints (with the
project API key) and forwards to your frontend, which passes it to the
Everscribe embed component to authenticate read-only requests without exposing
the API key.

Wire format::

    POST {base_url}/v1/projects/{project_id}/embed-tokens
        body: the validated TokenOptions wire shape
    Authorization: Bearer {api_key}
    Content-Type:  application/json

    201 Created -> {"token": "jwt-string"}

Non-201 responses raise :class:`MinterError`.
"""

from __future__ import annotations

import json
import urllib.error
import urllib.request
from collections.abc import Callable, Mapping

from .errors import MinterError
from .options import TokenOptions, token_options_to_wire

DEFAULT_BASE_URL = "https://api.everscribe.io"
DEFAULT_REQUEST_TIMEOUT = 10.0

# (url, body, headers, timeout) -> (status_code, response_body).
Transport = Callable[[str, bytes, Mapping[str, str], float], "tuple[int, bytes]"]

_MAX_ERROR_BODY = 4096


def _urllib_transport(
    url: str, body: bytes, headers: Mapping[str, str], timeout: float
) -> tuple[int, bytes]:
    req = urllib.request.Request(url, data=body, headers=dict(headers), method="POST")
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            return resp.status, resp.read()
    except urllib.error.HTTPError as e:  # non-2xx
        return e.code, e.read()


class Client:
    """Mints embed tokens for a single project. Reuse for the lifetime of the
    process - safe for concurrent use."""

    def __init__(
        self,
        project_id: str,
        api_key: str,
        *,
        base_url: str = DEFAULT_BASE_URL,
        request_timeout: float = DEFAULT_REQUEST_TIMEOUT,
        transport: Transport | None = None,
    ) -> None:
        self.base_url = base_url.rstrip("/")
        self.project_id = project_id
        self._api_key = api_key
        self._request_timeout = request_timeout
        self._transport: Transport = transport or _urllib_transport

    def mint_token(
        self,
        opts: TokenOptions | None = None,
        *,
        timeout: float | None = None,
    ) -> str:
        """Request a new embed token and return the JWT string. Validates
        ``opts`` client-side first (raising :class:`ValueError` without any
        HTTP call); non-201 responses raise :class:`MinterError`."""
        if opts is None:
            opts = TokenOptions()
        body = json.dumps(token_options_to_wire(opts)).encode("utf-8")

        headers = {
            "Authorization": f"Bearer {self._api_key}",
            "Content-Type": "application/json",
        }
        url = f"{self.base_url}/v1/projects/{self.project_id}/embed-tokens"
        t = timeout if timeout is not None else self._request_timeout
        status, resp_body = self._transport(url, body, headers, t)

        if status == 201:
            data = json.loads(resp_body.decode("utf-8"))
            token = data.get("token", "")
            if not isinstance(token, str):
                raise MinterError(status, "response token was not a string")
            return token

        text = resp_body[:_MAX_ERROR_BODY].decode("utf-8", "replace").strip()
        raise MinterError(status, text)


__all__ = ["DEFAULT_BASE_URL", "DEFAULT_REQUEST_TIMEOUT", "Client", "Transport"]
