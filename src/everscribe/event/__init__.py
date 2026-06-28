"""Event domain: the on-the-wire event model, redaction, and (added in a
later step) request-context helpers.

Mirrors ``sdk-go/pkg/event`` and ``sdk-node/src/event``.
"""

from __future__ import annotations

from .context import event_scope, from_context, prepare_event, run_with_event
from .event import Event
from .origin import client_ip, origin_from_request
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
    # Request context
    "from_context",
    "run_with_event",
    "event_scope",
    "prepare_event",
    # Origin
    "origin_from_request",
    "client_ip",
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
