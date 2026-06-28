"""Minter error type."""

from __future__ import annotations


class MinterError(Exception):
    """Raised when the mint endpoint responds with a non-2xx status.
    Inspect :attr:`status_code` and :attr:`body`. Typical codes: 400 (invalid
    options), 401 (bad auth), 404 (missing/soft-deleted project)."""

    def __init__(self, status_code: int, body: str) -> None:
        self.status_code = status_code
        self.body = body
        super().__init__(f"minter: http {status_code}: {body}")


__all__ = ["MinterError"]
