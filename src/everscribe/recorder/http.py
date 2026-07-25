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
(connection refused, timeout) propagate as the underlying
``urllib``/``socket`` exception.
"""

from __future__ import annotations

import json
import urllib.error
import urllib.request
from collections.abc import Callable, Mapping, Sequence

from ..event import Event, event_to_wire, prepare_event
from .errors import HTTPError

DEFAULT_BASE_URL = "https://api.everscribe.io"
DEFAULT_REQUEST_TIMEOUT = 10.0

# A transport takes (url, body, headers, timeout) and returns
# (status_code, response_body). The default uses urllib; tests inject their
# own to avoid the network.
Transport = Callable[[str, bytes, Mapping[str, str], float], "tuple[int, bytes]"]

_MAX_ERROR_BODY = 4096


def _urllib_transport(
    url: str, body: bytes, headers: Mapping[str, str], timeout: float
) -> tuple[int, bytes]:
    req = urllib.request.Request(url, data=body, headers=dict(headers), method="POST")
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            resp.read()  # drain so the connection can be reused
            status: int = resp.status
            return status, b""
    except urllib.error.HTTPError as e:  # non-2xx
        return e.code, e.read(_MAX_ERROR_BODY)


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
        prepare_event(e)
        self._finalize(e)
        body = json.dumps(event_to_wire(e)).encode("utf-8")
        self._post(self._events_path(), body, timeout)

    def record_batch(
        self, events: Sequence[Event], *, timeout: float | None = None
    ) -> None:
        """POST multiple events in one request. Empty-``action`` events are
        filtered out; an all-empty (or empty) batch is a no-op."""
        if not events:
            return
        wire = []
        for e in events:
            if e.action == "":
                continue
            prepare_event(e)
            self._finalize(e)
            wire.append(event_to_wire(e))
        if not wire:
            return
        body = json.dumps({"events": wire}).encode("utf-8")
        self._post(self._batch_path(), body, timeout)

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
        status, resp_body = self._transport(self.base_url + path, body, headers, t)
        if 200 <= status < 300:
            return
        text = resp_body[:_MAX_ERROR_BODY].decode("utf-8", "replace").strip()
        raise HTTPError(status, text)


__all__ = ["DEFAULT_BASE_URL", "DEFAULT_REQUEST_TIMEOUT", "HTTPRecorder", "Transport"]
