"""JSON Pointer (RFC 6901) redaction for event diffs.

Mirrors ``sdk-go/pkg/event`` (``marshalRedacted`` / ``redactPath``) and
``sdk-node/src/event/redact.ts``: values at the given pointer paths are
replaced with the string ``"[REDACTED]"`` before they leave the process.
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from typing import Any, Callable

REDACTED = "[REDACTED]"


@dataclass
class DiffConfig:
    """Accumulates options passed to :meth:`Event.diff`."""

    redact_paths: list[str] = field(default_factory=list)


DiffOption = Callable[[DiffConfig], None]
"""A function that mutates a :class:`DiffConfig`. Produced by
:func:`with_redacted_fields` and passed to :meth:`Event.diff`."""


def with_redacted_fields(*paths: str) -> DiffOption:
    """Return a :data:`DiffOption` that replaces the values at the given JSON
    Pointer paths (RFC 6901) with ``"[REDACTED]"`` in both before and after
    before they leave the process. Use for fields that must not appear in
    audit logs: password hashes, API keys, PII.

        e.diff(before, after,
               with_redacted_fields("/password_hash", "/api_keys/0"))

    Paths that don't exist in the document are silently skipped.
    """

    def apply(cfg: DiffConfig) -> None:
        cfg.redact_paths = list(paths)

    return apply


def apply_redaction(value: Any, paths: "list[str] | tuple[str, ...]" = ()) -> Any:
    """Round-trip ``value`` through JSON to normalize types, then redact the
    listed JSON Pointer paths in place. Mirrors Go's ``marshalRedacted``: the
    result is a wire-shape value the serializer can ``json.dumps`` directly.

    Raises ``TypeError``/``ValueError`` if ``value`` is not JSON-serializable;
    :meth:`Event.diff` catches this and leaves the change unset.
    """
    doc = json.loads(json.dumps(value))
    if not paths:
        return doc
    for p in paths:
        doc = _redact_path(doc, p)
    return doc


def _redact_path(doc: Any, pointer: str) -> Any:
    """Replace the value at the given JSON Pointer with ``"[REDACTED]"``. The
    empty pointer redacts the whole document."""
    if pointer == "":
        return REDACTED
    if not pointer.startswith("/"):
        return doc
    return _redact_tokens(doc, _split_pointer(pointer[1:]))


def _redact_tokens(node: Any, tokens: "list[str]") -> Any:
    if not tokens:
        return REDACTED
    head, rest = tokens[0], tokens[1:]
    if isinstance(node, list):
        idx = _parse_uint(head)
        if idx is None or idx < 0 or idx >= len(node):
            return node
        node[idx] = _redact_tokens(node[idx], rest)
        return node
    if isinstance(node, dict):
        if head not in node:
            return node
        node[head] = _redact_tokens(node[head], rest)
        return node
    return node


def _split_pointer(body: str) -> list[str]:
    """Split an RFC 6901 reference body by '/' and unescape the standard
    "~1" -> "/" and "~0" -> "~" sequences."""
    return [p.replace("~1", "/").replace("~0", "~") for p in body.split("/")]


def _parse_uint(s: str) -> "int | None":
    """Parse a base-10 unsigned integer for a JSON Pointer array index.
    Returns ``None`` on empty input or any non-digit character."""
    if not s or any(c not in "0123456789" for c in s):
        return None
    return int(s)


__all__ = [
    "REDACTED",
    "DiffConfig",
    "DiffOption",
    "with_redacted_fields",
    "apply_redaction",
]
