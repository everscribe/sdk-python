"""Token mint options and their client-side validation.

Mirrors ``sdk-go/pkg/minter`` (``TokenOptions`` / ``marshal``) and
``sdk-node/src/minter/options.ts`` (``tokenOptionsToWire``). Validation runs
before any HTTP call; failures raise :class:`ValueError` and short-circuit the
round-trip.
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import timedelta
from typing import Any, List, Optional, Union

from .columns import ALLOWED_COLUMNS

# Lifetime bounds (in seconds) the server enforces on expires_in. Go expresses
# these as time.Duration and Node as milliseconds; this SDK uses seconds to
# match its other timeout options (request_timeout, flush_interval, ...).
MIN_EXPIRES_IN = 60.0
MAX_EXPIRES_IN = 24 * 60 * 60.0

# Action filter grammar: ASCII alphanumeric and underscore, dot-separated
# segments, optional trailing ".*". Mirrors the server's grammar.
_ACTION_GRAMMAR = re.compile(r"^[a-zA-Z0-9_]+(\.[a-zA-Z0-9_]+)*(\.\*)?$")

_ACTION_GRAMMAR_DESC = r"[a-zA-Z0-9_]+(\.[a-zA-Z0-9_]+)*(\.\*)?"


@dataclass
class TokenOptions:
    """Configures a token mint request. All fields are optional; the default
    :class:`TokenOptions` mints a 1-hour, full-project, read-only token.

    The ``allowed_*`` fields use ``None`` for "no restriction" and reject an
    empty list, so building a list from filtered user input can never silently
    widen scope to "everything".
    """

    #: Scopes reads to events with a matching ``tenant_id``. Trimmed by the
    #: SDK; rejected if empty after trim or longer than 256 characters.
    tenant_id: str = ""
    #: Token lifetime in **seconds** (or a ``timedelta``). The server clamps to
    #: [:data:`MIN_EXPIRES_IN`, :data:`MAX_EXPIRES_IN`]. ``0`` uses the server
    #: default (1 hour).
    expires_in: "Union[float, timedelta]" = 0.0
    #: Whitelist of Event field names. ``None`` = no restriction; ``[]`` is
    #: rejected.
    allowed_columns: "Optional[List[str]]" = None
    #: Allowed actions - exact ("user.login") or suffix wildcard ("user.*").
    #: ``None`` = no restriction; ``[]`` is rejected.
    allowed_actions: "Optional[List[str]]" = None
    #: Restricts catalog fields the token's DSL/NLP queries may reference.
    #: ``None`` = no restriction; ``[]`` is rejected. Validated server-side.
    allowed_fields: "Optional[List[str]]" = None
    #: Unlock the Query (advanced DSL) tab and ``?q=`` on the read API.
    allow_dsl_input: bool = False
    #: Unlock the AI ("Ask in plain English") tab and the NLP endpoint.
    allow_nlp: bool = False


def token_options_to_wire(opts: TokenOptions) -> "dict[str, Any]":
    """Validate ``opts`` and return the wire-shape request body. Raises
    :class:`ValueError` on any validation failure."""
    wire: dict[str, Any] = {}

    if opts.tenant_id:
        trimmed = opts.tenant_id.strip()
        if trimmed == "":
            raise ValueError("minter: tenant_id is empty after trim")
        if len(trimmed) > 256:
            raise ValueError("minter: tenant_id exceeds 256 chars")
        wire["tenant_id"] = trimmed

    secs = (
        opts.expires_in.total_seconds()
        if isinstance(opts.expires_in, timedelta)
        else float(opts.expires_in)
    )
    if secs != 0:
        if secs < MIN_EXPIRES_IN:
            raise ValueError(
                f"minter: expires_in {secs}s is below minimum {MIN_EXPIRES_IN}s"
            )
        if secs > MAX_EXPIRES_IN:
            raise ValueError(
                f"minter: expires_in {secs}s is above maximum {MAX_EXPIRES_IN}s"
            )
        wire["expires_in"] = int(secs)

    if opts.allowed_columns is not None:
        if len(opts.allowed_columns) == 0:
            raise ValueError(
                "minter: allowed_columns is empty; pass None for no restriction"
            )
        for col in opts.allowed_columns:
            if col not in ALLOWED_COLUMNS:
                raise ValueError(f"minter: unknown column name {col!r}")
        wire["columns"] = list(opts.allowed_columns)

    if opts.allowed_actions is not None:
        if len(opts.allowed_actions) == 0:
            raise ValueError(
                "minter: allowed_actions is empty; pass None for no restriction"
            )
        for a in opts.allowed_actions:
            if not _ACTION_GRAMMAR.match(a):
                raise ValueError(
                    f"minter: action entry {a!r} does not match grammar "
                    f"{_ACTION_GRAMMAR_DESC}"
                )
        wire["actions"] = list(opts.allowed_actions)

    if opts.allowed_fields is not None:
        if len(opts.allowed_fields) == 0:
            raise ValueError(
                "minter: allowed_fields is empty; pass None for no restriction"
            )
        # Field validation lives on the server (the catalog is canonical
        # there); the SDK ships entries through verbatim.
        wire["allowed_fields"] = list(opts.allowed_fields)

    if opts.allow_dsl_input:
        wire["allow_dsl_input"] = True
    if opts.allow_nlp:
        wire["allow_nlp"] = True

    return wire


__all__ = [
    "TokenOptions",
    "token_options_to_wire",
    "MIN_EXPIRES_IN",
    "MAX_EXPIRES_IN",
]
