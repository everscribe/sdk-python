"""The canonical audit record and its builder methods.

Construct via ``Event("action")`` for non-HTTP callers (background jobs,
cron, CLI). HTTP handlers prefer ``from_context()`` (see
``everscribe.event.context``), which additionally populates Origin and
Actor from the request. Populate the handler-specific fields (``action``,
``target``, ``metadata``, optionally ``result``) and pass to
``Recorder.record``.
"""

from __future__ import annotations

import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any

from .redact import DiffConfig, DiffOption, apply_redaction
from .types import Actor, Change, Origin, Result, Target


def _new_id() -> str:
    return str(uuid.uuid4())


def _now_utc() -> datetime:
    return datetime.now(timezone.utc)


@dataclass
class Event:
    """Canonical audit record.

    ``action`` is the dotted verb (e.g. "user.lock"). An empty ``action`` is
    a no-op at record time, which is what makes the "prepare an event, record
    it in a finally block, and only some code paths set the action" idiom
    safe.
    """

    action: str = ""
    id: str = field(default_factory=_new_id)
    occurred_at: datetime = field(default_factory=_now_utc)
    actor: Actor = field(default_factory=Actor)
    tenant_id: str = ""
    target: Target = field(default_factory=Target)
    metadata: dict[str, Any] = field(default_factory=dict)
    origin: Origin = field(default_factory=Origin)
    result: Result = field(default_factory=Result)
    change: "Change | None" = None
    idempotency_key: str = ""

    def with_field(self, key: str, value: Any) -> "Event":
        """Set a single metadata key/value pair. Returns ``self`` for chaining."""
        self.metadata[key] = value
        return self

    def with_fields(self, *args: Any) -> "Event":
        """Set metadata from alternating key/value pairs, slog-style::

            e.with_fields("reason", "spam", "severity", "high")

        Odd-length argument lists drop the trailing value. Non-string keys are
        silently skipped. Returns ``self`` for chaining.
        """
        for i in range(0, len(args) - 1, 2):
            key = args[i]
            if not isinstance(key, str):
                continue
            self.metadata[key] = args[i + 1]
        return self

    def diff(self, before: Any, after: Any, *opts: DiffOption) -> "Event":
        """Record a state transition for a mutation event. ``before`` and
        ``after`` are JSON-normalized (matching Go's marshal round-trip) and
        any :func:`with_redacted_fields` paths are scrubbed before storage.
        The audit-log API computes the patch on ingest.

        Serialization failures (e.g. non-JSON-serializable values) leave
        ``change`` unset; the event still records. Returns ``self`` for
        chaining.
        """
        cfg = DiffConfig()
        for opt in opts:
            opt(cfg)
        try:
            before_out = apply_redaction(before, cfg.redact_paths)
            after_out = apply_redaction(after, cfg.redact_paths)
        except (TypeError, ValueError):
            return self
        self.change = Change(before=before_out, after=after_out)
        return self

    def raw_diff(
        self, before: Any = None, after: Any = None, patch: Any = None
    ) -> "Event":
        """Escape hatch for callers that already have JSON-shaped before/after
        state, or who want to supply their own precomputed RFC 6902 patch. Any
        of the three may be ``None``; if all three are ``None``, the call is a
        no-op. Returns ``self`` for chaining.
        """
        if before is None and after is None and patch is None:
            return self
        change = Change()
        if before is not None:
            change.before = before
        if after is not None:
            change.after = after
        if patch is not None:
            change.patch = patch
        self.change = change
        return self


__all__ = ["Event"]
