"""The set of Event field names accepted by the mint endpoint.

The Go SDK derives these via reflection over ``event.Event``'s JSON tags; we
hard-code them (as the Node SDK does) because the wire shape is owned by this
SDK - the list can't drift without an intentional change here. Expanding
``event.Event`` requires updating this set and the server's allowlist too.
"""

from __future__ import annotations

ALLOWED_COLUMNS = frozenset(
    {
        "id",
        "tenant_id",
        "occurred_at",
        "actor",
        "action",
        "target",
        "metadata",
        "origin",
        "result",
        "change",
        "idempotency_key",
    }
)

__all__ = ["ALLOWED_COLUMNS"]
