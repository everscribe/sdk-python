"""Recorder protocols shared across the recorder implementations."""

from __future__ import annotations

from collections.abc import Sequence
from typing import Protocol, runtime_checkable

from ..event import Event


@runtime_checkable
class Recorder(Protocol):
    """Records audit events. Implementations may be synchronous
    (:class:`HTTPRecorder`) or buffered (:class:`BufferedRecorder`).

    Implementations treat an event with an empty ``action`` as a no-op, and
    populate ``result`` from the in-scope status capture when ``result.status``
    is unset (an explicitly-set result always wins)."""

    def record(self, e: Event) -> None: ...


@runtime_checkable
class BatchRecorder(Protocol):
    """Optional capability for implementations that can persist multiple
    events more efficiently than N serial :meth:`Recorder.record` calls.
    :class:`BufferedRecorder` uses it when available and falls back to looped
    ``record`` calls otherwise."""

    def record_batch(self, events: Sequence[Event]) -> None: ...


__all__ = ["BatchRecorder", "Recorder"]
