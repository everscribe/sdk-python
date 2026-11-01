"""Retry classification and backoff math for delivery retries."""

from __future__ import annotations

from collections.abc import Callable

from .errors import EncodeError, HTTPError, NetworkError


def is_retryable(err: BaseException) -> bool:
    """Whether a failed flush is worth another attempt."""
    if isinstance(err, HTTPError):
        return err.transient()
    if isinstance(err, NetworkError | TimeoutError):
        return True
    try:
        t = getattr(err, "transient", None)
        return bool(t()) if callable(t) else bool(t)
    except Exception:  # noqa: BLE001 -- a broken transient() means not retryable
        return False


def is_splittable(err: BaseException) -> bool:
    """Whether halving the batch can isolate the cause."""
    if isinstance(err, HTTPError):
        return err.status_code in (400, 409, 413)
    return isinstance(err, EncodeError)


def backoff_cap(n: int, initial: float, max: float) -> float:
    """min(max, initial*2^(n-1)) for retry n >= 1."""
    d = initial
    for _ in range(1, n):
        if d >= max or d > max / 2:
            return max
        d *= 2
    return min(d, max)


def retry_wait(
    n: int,
    err: BaseException,
    initial: float,
    max: float,
    jitter: Callable[[float], float],
) -> float:
    """Jittered backoff, raised to any Retry-After, capped at max."""
    w = jitter(backoff_cap(n, initial, max))
    if isinstance(err, HTTPError) and err.retry_after > w:
        w = err.retry_after
    return min(w, max)
