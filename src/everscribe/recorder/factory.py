"""The recommended recorder entry point: ``new``."""

from __future__ import annotations

from ..event.types import Logger
from .buffered import (
    DEFAULT_BUFFER_SIZE,
    DEFAULT_DRAIN_TIMEOUT,
    DEFAULT_FLUSH_INTERVAL,
    DEFAULT_FLUSH_SIZE,
    DEFAULT_FLUSH_TIMEOUT,
    BufferedRecorder,
    OverflowPolicy,
)
from .http import DEFAULT_BASE_URL, DEFAULT_REQUEST_TIMEOUT, HTTPRecorder, Transport


def new(
    project_id: str,
    api_key: str,
    *,
    # HTTP transport options
    base_url: str = DEFAULT_BASE_URL,
    request_timeout: float = DEFAULT_REQUEST_TIMEOUT,
    auto_idempotency_key: bool = False,
    transport: "Transport | None" = None,
    # Buffering options
    buffer_size: int = DEFAULT_BUFFER_SIZE,
    flush_size: int = DEFAULT_FLUSH_SIZE,
    flush_interval: float = DEFAULT_FLUSH_INTERVAL,
    flush_timeout: float = DEFAULT_FLUSH_TIMEOUT,
    overflow: OverflowPolicy = OverflowPolicy.DROP_NEWEST,
    drain_timeout: float = DEFAULT_DRAIN_TIMEOUT,
    logger: "Logger | None" = None,
) -> BufferedRecorder:
    """Return a :class:`BufferedRecorder` wrapping an :class:`HTTPRecorder`
    using the package defaults - the recommended way to construct a recorder.

    HTTP options (``base_url``, ``request_timeout``, ``auto_idempotency_key``,
    ``transport``) go to the inner recorder; the rest configure buffering.

    Call :meth:`BufferedRecorder.close` on shutdown to drain pending events.
    """
    inner = HTTPRecorder(
        project_id,
        api_key,
        base_url=base_url,
        request_timeout=request_timeout,
        auto_idempotency_key=auto_idempotency_key,
        transport=transport,
    )
    return BufferedRecorder(
        inner,
        buffer_size=buffer_size,
        flush_size=flush_size,
        flush_interval=flush_interval,
        flush_timeout=flush_timeout,
        overflow=overflow,
        drain_timeout=drain_timeout,
        logger=logger,
    )


__all__ = ["new"]
