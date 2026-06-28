"""Recorder: append-only event ingest over HTTP with async buffering.

Mirrors ``sdk-go/pkg/recorder`` and ``sdk-node/src/recorder``. The HTTP
transport uses the standard library (``urllib``); :class:`BufferedRecorder`
flushes batches on a background daemon thread. :func:`new` is the recommended
entry point.
"""

from __future__ import annotations

from .base import BatchRecorder, Recorder
from .buffered import (
    DEFAULT_BUFFER_SIZE,
    DEFAULT_DRAIN_TIMEOUT,
    DEFAULT_FLUSH_INTERVAL,
    DEFAULT_FLUSH_SIZE,
    DEFAULT_FLUSH_TIMEOUT,
    BufferedRecorder,
    BufferedStats,
    OverflowPolicy,
)
from .errors import BufferFullError, DrainTimeoutError, HTTPError
from .factory import new
from .http import (
    DEFAULT_BASE_URL,
    DEFAULT_REQUEST_TIMEOUT,
    HTTPRecorder,
    Transport,
)

__all__ = [
    # Entry point
    "new",
    # Recorders
    "HTTPRecorder",
    "BufferedRecorder",
    # Protocols
    "Recorder",
    "BatchRecorder",
    # Config / stats
    "OverflowPolicy",
    "BufferedStats",
    "Transport",
    # Errors
    "HTTPError",
    "BufferFullError",
    "DrainTimeoutError",
    # Defaults
    "DEFAULT_BASE_URL",
    "DEFAULT_REQUEST_TIMEOUT",
    "DEFAULT_BUFFER_SIZE",
    "DEFAULT_FLUSH_SIZE",
    "DEFAULT_FLUSH_INTERVAL",
    "DEFAULT_FLUSH_TIMEOUT",
    "DEFAULT_DRAIN_TIMEOUT",
]
