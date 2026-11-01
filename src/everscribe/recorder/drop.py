"""Drop reasons, the ``on_drop`` callback type, and per-reason counts."""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from enum import Enum

from ..event import Event


class DropReason(str, Enum):
    """Why events were dropped without being delivered."""

    BUFFER_FULL = "buffer_full"  # buffer full under DROP_NEWEST
    REJECTED = "rejected"  # server refused the batch (non-retryable)
    RETRIES_EXHAUSTED = "retries_exhausted"  # retry budget spent
    SHUTDOWN = "shutdown"  # close() drain ended with events pending
    CLOSED = "closed"  # record() after close()


#: ``on_drop(events, reason, err)``. Must not block and must not call
#: ``record``, ``flush``, or ``close`` on the same recorder. May be called
#: concurrently from request threads and the worker. ``events`` is a new list.
OnDrop = Callable[[list[Event], DropReason, BaseException | None], None]


@dataclass(frozen=True)
class DroppedByReason:
    """Cumulative dropped-event counts per :class:`DropReason`."""

    buffer_full: int = 0
    rejected: int = 0
    retries_exhausted: int = 0
    shutdown: int = 0
    closed: int = 0


__all__ = ["DropReason", "DroppedByReason", "OnDrop"]
