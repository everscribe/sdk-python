"""Everscribe Python SDK.

Python SDK for the Everscribe audit-log API. Two coordinated surfaces:

- :mod:`everscribe.recorder` - append-only event ingest.
- :mod:`everscribe.minter` - short-lived embed tokens for frontend audit views.

Bind credentials once with :func:`new` (or :func:`new_from_env`) and create
per-surface subclients from the returned :class:`Client`::

    import everscribe

    es = everscribe.new(project_id, api_key)
    rec = es.new_recorder()
    rec.record(everscribe.Event("user.login"))

The core is zero-dependency (standard library only). The optional ASGI adapter
lives in :mod:`everscribe.asgi` (install the ``fastapi`` extra).
"""

from __future__ import annotations

from . import event, minter, recorder
from .client import (
    EVERSCRIBE_API_KEY,
    EVERSCRIBE_PROJECT_ID,
    Client,
    new,
    new_from_env,
)
from .event import Event

__version__ = "0.0.0"

__all__ = [
    "__version__",
    # Root client
    "Client",
    "new",
    "new_from_env",
    "EVERSCRIBE_PROJECT_ID",
    "EVERSCRIBE_API_KEY",
    # Event model
    "Event",
    # Subpackages
    "event",
    "recorder",
    "minter",
]
