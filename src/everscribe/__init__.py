"""Everscribe Python SDK.

Python SDK for the Everscribe audit-log API, built to parity with the Go and
Node SDKs. Two coordinated surfaces:

- ``everscribe.recorder`` — append-only event ingest.
- ``everscribe.minter`` — short-lived embed tokens for frontend audit views.

Bind credentials once with :func:`new` (or :func:`new_from_env`) and create
per-surface subclients from the returned :class:`Client`.

The public API is assembled across subsequent build steps; this module will
re-export the root ``Client``, the constructors, and the ``Event`` type.
"""

from __future__ import annotations

__version__ = "0.0.0"

__all__ = ["__version__"]
