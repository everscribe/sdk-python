"""HTTP recorder: posts events to the audit-log ingestion API over the
standard library (``urllib``).

Wire format::

    POST {base_url}/v1/projects/{project_id}/events
        body: a single Event JSON object
    POST {base_url}/v1/projects/{project_id}/events/batch
        body: {"events": [Event, Event, ...]}
    Authorization: Bearer {api_key}
    Content-Type:  application/json

Non-2xx responses raise :class:`HTTPError`; transport-level failures
(connection refused, timeout) raise :class:`NetworkError`; unserializable
events raise :class:`EncodeError`.
"""

from __future__ import annotations

import http.client
import json
import urllib.error
import urllib.request
from collections.abc import Callable, Mapping, Sequence
from typing import Any

from ..event import Event, event_to_wire, prepare_event
from .errors import EncodeError, HTTPError, NetworkError

DEFAULT_BASE_URL = "https://api.everscribe.io"
DEFAULT_REQUEST_TIMEOUT = 10.0

# A transport takes (url, body, headers, timeout) and returns (status_code,
# response_body) or (status_code, response_body, response_headers); headers
# feed Retry-After. The default uses urllib; tests inject their own.
Transport = Callable[
    [str, bytes, Mapping[str, str], float],
    "tuple[int, bytes] | tuple[int, bytes, Mapping[str, str]]",
]

_MAX_ERROR_BODY = 4096
_MAX_RETRY_AFTER_DIGITS = 10


def _urllib_transport(
    url: str, body: bytes, headers: Mapping[str, str], timeout: float
) -> tuple[int, bytes, Mapping[str, str]]:
    req = urllib.request.Request(url, data=body, headers=dict(headers), method="POST")
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            resp.read()  # drain so the connection can be reused
            status: int = resp.status
            return status, b"", dict(resp.headers.items())
    except urllib.error.HTTPError as e:  # non-2xx
        return e.code, e.read(_MAX_ERROR_BODY), dict(e.headers.items())


def _retry_after(headers: Mapping[str, str]) -> float:
    """Seconds from a positive-integer Retry-After; 0 otherwise (dates ignored)."""
    for k, v in headers.items():
        if k.lower() == "retry-after":
            v = v.strip()
            if len(v) > _MAX_RETRY_AFTER_DIGITS or not (v.isascii() and v.isdigit()):
                return 0.0
            try:
                return float(max(int(v), 0))
            except (ValueError, OverflowError):
                return 0.0
    return 0.0


def _read_error_body(e: urllib.error.HTTPError) -> bytes:
    try:
        return e.read(_MAX_ERROR_BODY) or b""
    except Exception:  # noqa: BLE001 -- the status matters more than the body
        return b""


def _dumps(obj: object) -> bytes:
    try:
        return json.dumps(obj).encode("utf-8")
    except (TypeError, ValueError) as e:
        raise EncodeError(f"recorder: encode event: {e}") from e


class HTTPRecorder:
    """Posts events to the audit-log ingestion API for one project,
    authenticating with ``api_key`` as a bearer token. Implements both
    :class:`Recorder` and :class:`BatchRecorder`."""

    def __init__(
        self,
        project_id: str,
        api_key: str,
        *,
        base_url: str = DEFAULT_BASE_URL,
        request_timeout: float = DEFAULT_REQUEST_TIMEOUT,
        auto_idempotency_key: bool = False,
        transport: Transport | None = None,
    ) -> None:
        self.base_url = base_url.rstrip("/")
        self.project_id = project_id
        self._api_key = api_key
        self._request_timeout = request_timeout
        self._auto_idempotency_key = auto_idempotency_key
        self._transport: Transport = transport or _urllib_transport

    def record(self, e: Event, *, timeout: float | None = None) -> None:
        """POST a single event. Empty-``action`` events are no-ops."""
        if e is None or e.action == "":
            return
        body = _dumps(self._wire(e))
        self._post(self._events_path(), body, timeout)

    def record_batch(self, events: Sequence[Event], *, timeout: float | None = None) -> None:
        """POST multiple events in one request. Empty-``action`` events are
        filtered out; an all-empty (or empty) batch is a no-op."""
        if not events:
            return
        wire = [self._wire(e) for e in events if e.action != ""]
        if not wire:
            return
        body = _dumps({"events": wire})
        self._post(self._batch_path(), body, timeout)

    def _wire(self, e: Event) -> dict[str, Any]:
        """Prepare ``e`` and convert it to its wire dict. A wrongly typed
        field raises :class:`EncodeError`, so a batch splits around it."""
        try:
            prepare_event(e)
            self._finalize(e)
            return event_to_wire(e)
        except (TypeError, ValueError, AttributeError) as ex:
            raise EncodeError(f"recorder: encode event: {ex}") from ex

    def _finalize(self, e: Event) -> None:
        """Post-``prepare_event`` fixups: copy id into idempotency_key when
        ``auto_idempotency_key`` is on and the key is empty. Caller-supplied
        keys always win."""
        if self._auto_idempotency_key and not e.idempotency_key:
            e.idempotency_key = e.id

    def _events_path(self) -> str:
        return f"/v1/projects/{self.project_id}/events"

    def _batch_path(self) -> str:
        return f"/v1/projects/{self.project_id}/events/batch"

    def _post(self, path: str, body: bytes, timeout: float | None) -> None:
        headers = {
            "Authorization": f"Bearer {self._api_key}",
            "Content-Type": "application/json",
        }
        t = timeout if timeout is not None else self._request_timeout
        try:
            res = self._transport(self.base_url + path, body, headers, t)
        except urllib.error.HTTPError as e:  # an OSError; keep the status
            res = (e.code, _read_error_body(e), dict(e.headers.items()) if e.headers else {})
        except NetworkError:  # an OSError too; don't wrap it twice
            raise
        except (OSError, http.client.HTTPException) as e:
            raise NetworkError(f"recorder: {e}") from e
        status, resp_body = res[0], res[1]
        if 200 <= status < 300:
            return
        text = resp_body[:_MAX_ERROR_BODY].decode("utf-8", "replace").strip()
        err = HTTPError(status, text)
        if len(res) > 2:
            err.retry_after = _retry_after(res[2])
        raise err


__all__ = ["DEFAULT_BASE_URL", "DEFAULT_REQUEST_TIMEOUT", "HTTPRecorder", "Transport"]
