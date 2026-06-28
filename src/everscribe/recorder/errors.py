"""Recorder error types."""

from __future__ import annotations


class HTTPError(Exception):
    """Raised by :class:`HTTPRecorder` when the ingestion endpoint responds
    with a non-2xx status. Inspect :attr:`status_code` / :attr:`body`, or call
    :meth:`transient` to distinguish retryable failures (5xx, 429) from
    permanent ones (4xx)."""

    def __init__(self, status_code: int, body: str) -> None:
        self.status_code = status_code
        self.body = body
        super().__init__(f"recorder: http {status_code}: {body}")

    def transient(self) -> bool:
        """Whether the error is likely to resolve on retry (5xx server errors
        and 429 rate limits)."""
        return self.status_code >= 500 or self.status_code == 429


class BufferFullError(Exception):
    """Raised by :class:`BufferedRecorder.record` when the overflow policy is
    ``OverflowPolicy.ERROR`` and the buffer has no space. Other overflow
    policies never raise this."""

    def __init__(self, message: str = "recorder: buffer full") -> None:
        super().__init__(message)


class DrainTimeoutError(Exception):
    """Raised by :class:`BufferedRecorder.close` when the drain timeout
    elapses with events still pending."""

    def __init__(
        self, message: str = "recorder: drain timed out with events pending"
    ) -> None:
        super().__init__(message)


__all__ = ["HTTPError", "BufferFullError", "DrainTimeoutError"]
