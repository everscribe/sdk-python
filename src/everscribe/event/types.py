"""Event domain types: Actor, Target, Origin, Result, Change, and the
OutcomeCapture / Logger protocols.

Fields use plain values with empty defaults ("" for strings, 0 for ints,
empty structs for nested objects) rather than optionals. Empty means
"not set"; the wire serializer omits empty fields. Plain values are used
throughout in place of optional pointers.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Protocol, runtime_checkable


class _Unset:
    """Sentinel distinguishing "field absent" from an explicit JSON ``null``.

    Used as the default for :class:`Change` fields so ``diff`` can record an
    explicit ``null`` before/after while ``raw_diff`` and construction leave
    unspecified fields out of the wire payload entirely.
    """

    __slots__ = ()

    def __repr__(self) -> str:  # pragma: no cover - cosmetic
        return "UNSET"

    def __bool__(self) -> bool:
        return False


UNSET: _Unset = _Unset()


@dataclass
class Actor:
    """Identifies who caused the event. Type values are conventional, not
    enforced: "user", "admin", "system", "api_key", "anonymous"."""

    type: str = ""
    id: str = ""
    display_name: str = ""
    email: str = ""


@dataclass
class Target:
    """What the event acted on. Both fields empty means "no target"."""

    type: str = ""
    id: str = ""


@dataclass
class Origin:
    """Network/request context where the event was emitted."""

    ip: str = ""
    user_agent: str = ""
    request_id: str = ""


@dataclass
class Result:
    """Outcome of the audited action.

    ``status``: "ok" | "error" | "denied" - empty means unrecorded.
    ``code``:   HTTP status or app-defined code.
    ``message``: free-form. ``Exception`` instances render as ``str(exc)``;
                 empty strings are omitted at the wire boundary. ``None``
                 means "no message".
    """

    status: str = ""
    code: int = 0
    message: Any = None


@dataclass
class Change:
    """State transition for mutation events.

    ``before``/``after`` hold the (already redacted, if applicable)
    wire-shape state on either side; the audit-log API computes the JSON
    Patch on ingest. ``patch`` is optional and only set when the caller
    supplies a precomputed RFC 6902 patch via ``raw_diff``.

    Fields default to :data:`UNSET` so an explicit ``None`` (JSON ``null``)
    is distinguishable from an unspecified field and serialized accordingly.
    """

    before: Any = UNSET
    after: Any = UNSET
    patch: Any = UNSET


@runtime_checkable
class OutcomeCapture(Protocol):
    """Reports the adapter-derived outcome for an in-flight call, so
    ``prepare_event`` (mid-handler) and ``end_event`` (after the handler has
    genuinely finished) can auto-populate an event's :class:`Result` when the
    handler hasn't set one itself.

    ``None`` means the call has not produced an outcome yet. This replaces
    an earlier ``status: int`` sentinel, where ``0`` meant "nothing
    written": that works for HTTP, but gRPC's OK status IS code 0, so an
    integer cannot carry both "no outcome yet" and "outcome is code 0" for a
    transport-neutral capture. Implemented by framework adapters.
    :func:`result_from_http_status` is the opt-in helper an HTTP-shaped
    adapter uses to build the :class:`Result` its ``outcome`` property
    returns.
    """

    @property
    def outcome(self) -> Result | None:
        """The captured outcome, or ``None`` if nothing has been produced
        yet."""
        ...


class Logger(Protocol):
    """Minimal logger used by the buffered/HTTP recorders for diagnostics
    (overflow warnings, flush errors). A stdlib-backed default lives in the
    recorder package."""

    def warning(self, message: str, *args: Any, **kwargs: Any) -> None: ...

    def error(self, message: str, *args: Any, **kwargs: Any) -> None: ...


__all__ = [
    "UNSET",
    "Actor",
    "Change",
    "Logger",
    "Origin",
    "OutcomeCapture",
    "Result",
    "Target",
]
