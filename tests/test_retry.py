"""Tests for retry classification and backoff math."""

from __future__ import annotations

import pytest

from everscribe.recorder.errors import EncodeError, HTTPError, NetworkError
from everscribe.recorder.retry import backoff_cap, is_retryable, is_splittable, retry_wait


class _AttrTransient(Exception):
    transient = True


class _MethodTransient(Exception):
    def transient(self) -> bool:
        return True


class _NotTransient(Exception):
    def transient(self) -> bool:
        return False


@pytest.mark.parametrize(
    ("err", "want"),
    [
        (HTTPError(500, ""), True),
        (HTTPError(503, ""), True),
        (HTTPError(429, ""), True),
        (NetworkError("x"), True),
        (TimeoutError(), True),
        (_AttrTransient(), True),
        (_MethodTransient(), True),
        (HTTPError(400, ""), False),
        (HTTPError(401, ""), False),
        (HTTPError(404, ""), False),
        (HTTPError(409, ""), False),
        (HTTPError(413, ""), False),
        (EncodeError("x"), False),
        (TypeError("x"), False),
        (RuntimeError("x"), False),
        (_NotTransient(), False),
    ],
)
def test_is_retryable(err: BaseException, want: bool) -> None:
    assert is_retryable(err) is want


@pytest.mark.parametrize(
    ("err", "want"),
    [
        (HTTPError(400, ""), True),
        (HTTPError(409, ""), True),
        (HTTPError(413, ""), True),
        (EncodeError("x"), True),
        (HTTPError(401, ""), False),
        (HTTPError(403, ""), False),
        (HTTPError(404, ""), False),
        (HTTPError(500, ""), False),
        (TypeError("x"), False),
    ],
)
def test_is_splittable(err: BaseException, want: bool) -> None:
    assert is_splittable(err) is want


@pytest.mark.parametrize(("n", "want"), [(1, 0.5), (2, 1.0), (3, 2.0), (7, 30.0), (100, 30.0)])
def test_backoff_cap(n: int, want: float) -> None:
    assert backoff_cap(n, 0.5, 30.0) == want


def test_retry_wait() -> None:
    def ident(d: float) -> float:
        return d

    assert retry_wait(3, HTTPError(503, ""), 0.5, 30.0, ident) == 2.0
    e = HTTPError(429, "")
    e.retry_after = 10.0
    assert retry_wait(3, e, 0.5, 30.0, ident) == 10.0
    e.retry_after = 3600.0
    assert retry_wait(3, e, 0.5, 30.0, ident) == 30.0
    assert retry_wait(3, NetworkError("x"), 0.5, 30.0, ident) == 2.0


class _RaisingTransient(Exception):
    def transient(self) -> bool:
        raise RuntimeError("bug")


def test_raising_transient_is_not_retryable() -> None:
    assert is_retryable(_RaisingTransient()) is False
