"""Recorder: append-only event ingest over HTTP with async buffering.

Mirrors ``sdk-go/pkg/recorder`` and ``sdk-node/src/recorder``. The HTTP
transport uses the standard library (``urllib``); the buffered recorder flushes
batches on a background thread. Populated in a later build step.
"""

from __future__ import annotations

__all__: list[str] = []
