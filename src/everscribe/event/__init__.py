"""Event domain: the on-the-wire event model, redaction, and (added in a
later step) request-context helpers.

Mirrors ``sdk-go/pkg/event`` and ``sdk-node/src/event``.
"""

from __future__ import annotations

from .event import Event
from .redact import (
    REDACTED,
    DiffConfig,
    DiffOption,
    apply_redaction,
    with_redacted_fields,
)
from .types import (
    UNSET,
    Actor,
    Change,
    Logger,
    Origin,
    Result,
    StatusCapture,
    Target,
)
from .wire import event_to_wire, result_to_wire

__all__ = [
    # Model
    "Event",
    "Actor",
    "Target",
    "Origin",
    "Result",
    "Change",
    "UNSET",
    # Protocols
    "StatusCapture",
    "Logger",
    # Redaction
    "with_redacted_fields",
    "apply_redaction",
    "DiffOption",
    "DiffConfig",
    "REDACTED",
    # Wire
    "event_to_wire",
    "result_to_wire",
]
