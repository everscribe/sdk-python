"""The top-level Everscribe client: binds a project's credentials once and
hands out per-surface subclients (recorder for ingest, minter for read-side
embed tokens) that share the same auth.

Typical usage::

    import everscribe

    es = everscribe.new(project_id, api_key)      # or new_from_env()
    rec = es.new_recorder()
    try:
        ...
    finally:
        rec.close()

    m = es.new_minter()
    token = m.mint_token(everscribe.minter.TokenOptions(...))

Customers who only need one surface can call its constructor directly -
``everscribe.recorder.new(project_id, api_key)`` and
``everscribe.minter.Client(project_id, api_key)`` both work and skip the
client step.
"""

from __future__ import annotations

import os
from typing import Any

from .minter import Client as MinterClient
from .recorder import BufferedRecorder
from .recorder import new as _recorder_new

#: Environment-variable names read by :func:`new_from_env`.
EVERSCRIBE_PROJECT_ID = "EVERSCRIBE_PROJECT_ID"
EVERSCRIBE_API_KEY = "EVERSCRIBE_API_KEY"


class Client:
    """Credential-bearing handle to an Everscribe project. Holds no network
    state itself; the subclient constructors build per-surface clients that own
    their own connections. Reuse a single Client for the lifetime of the
    process - safe for concurrent use."""

    def __init__(self, project_id: str, api_key: str) -> None:
        """Validate and trim credentials. Raises :class:`ValueError` if either
        is empty or whitespace-only, so configuration bugs surface at
        construction rather than at the first network call."""
        project_id = project_id.strip()
        if not project_id:
            raise ValueError("everscribe: project_id is empty")
        api_key = api_key.strip()
        if not api_key:
            raise ValueError("everscribe: api_key is empty")
        self.project_id = project_id
        self._api_key = api_key

    @classmethod
    def from_env(cls) -> "Client":
        """Construct a Client from ``EVERSCRIBE_PROJECT_ID`` and
        ``EVERSCRIBE_API_KEY``. Raises :class:`ValueError` naming the missing
        variable if either is unset or empty after trimming."""
        project_id = os.environ.get(EVERSCRIBE_PROJECT_ID, "").strip()
        if not project_id:
            raise ValueError(f"everscribe: {EVERSCRIBE_PROJECT_ID} is not set or empty")
        api_key = os.environ.get(EVERSCRIBE_API_KEY, "").strip()
        if not api_key:
            raise ValueError(f"everscribe: {EVERSCRIBE_API_KEY} is not set or empty")
        return cls(project_id, api_key)

    def new_recorder(self, **opts: Any) -> BufferedRecorder:
        """Return a buffered recorder for the bound project. Keyword options
        forward to :func:`everscribe.recorder.new` (buffer_size, flush_interval,
        base_url, ...)."""
        return _recorder_new(self.project_id, self._api_key, **opts)

    def new_minter(self, **opts: Any) -> MinterClient:
        """Return a minter client for the bound project. Keyword options
        forward to :class:`everscribe.minter.Client` (base_url,
        request_timeout, ...)."""
        return MinterClient(self.project_id, self._api_key, **opts)


def new(project_id: str, api_key: str) -> Client:
    """Construct a :class:`Client`. Raises :class:`ValueError` on
    empty/whitespace credentials."""
    return Client(project_id, api_key)


def new_from_env() -> Client:
    """Construct a :class:`Client` from environment variables. See
    :meth:`Client.from_env`."""
    return Client.from_env()


__all__ = [
    "Client",
    "new",
    "new_from_env",
    "EVERSCRIBE_PROJECT_ID",
    "EVERSCRIBE_API_KEY",
]
