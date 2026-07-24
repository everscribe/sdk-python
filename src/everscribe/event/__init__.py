"""Event domain: the on-the-wire event model, redaction, and (added in a
later step) request-context helpers.
"""

from __future__ import annotations

from .context import (
    current_event,
    end_event,
    event_scope,
    new_from_context,
    prepare_event,
    request_scope,
    result_from_http_status,
    run_with_event,
)
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
    OutcomeCapture,
    Result,
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
    "OutcomeCapture",
    "Logger",
    # Request context / lifecycle
    "new_from_context",
    "current_event",
    "run_with_event",
    "event_scope",
    "request_scope",
    "prepare_event",
    "end_event",
    "result_from_http_status",
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
