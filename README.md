# everscribe (Python SDK)

Python SDK for the Everscribe audit-log API. Two coordinated surfaces:

- **Recorder** — append-only event ingest. Records who did what, when, on
  what resource, and — for mutation events — how the resource changed.
- **Minter** — mints short-lived embed tokens that let a customer's frontend
  mount the Everscribe embeddable component (e.g. `<EverscribeEvents />`) to
  display events without exposing the project API key to the browser.

The core is **zero-dependency** (standard library only) and requires Python
3.10+. An optional [FastAPI](https://fastapi.tiangolo.com/) /
[Starlette](https://www.starlette.io/) ASGI adapter is available via the
`fastapi` extra.

---

## Table of contents

- [Install](#install)
- [Quickstart](#quickstart)
- [Three key behaviors](#three-key-behaviors)
- [The Event shape](#the-event-shape)
- [BufferedRecorder](#bufferedrecorder)
- [Idempotency](#idempotency)
- [Embedded views](#embedded-views)

---

## Install

```sh
pip install everscribe              # core only, zero dependencies
pip install "everscribe[fastapi]"   # + the ASGI adapter (Starlette/FastAPI)
```

```python
import everscribe                       # new, new_from_env, Client, Event
from everscribe import event            # Event, from_context, with_redacted_fields, ...
from everscribe import recorder         # new, BufferedRecorder, HTTPRecorder, OverflowPolicy
from everscribe import minter           # Client, TokenOptions
from everscribe.asgi import EverscribeMiddleware  # ASGI adapter (needs the fastapi extra)
```

The top-level module is the entry point — bind credentials once and hand out
per-surface clients. Customers who only need one surface can call
`recorder.new` or `minter.Client` directly to skip the SDK-client step.

---

## Quickstart

### 1. Bind credentials and construct subclients

The top-level module binds the project ID and API key once and lets you build
per-surface clients without re-passing them:

```python
import everscribe

es = everscribe.new(project_id, api_key)   # raises ValueError if either is empty
rec = es.new_recorder()
# shutdown: rec.close()
```

For 12-factor / containerized deployments, read credentials from the
environment instead — `new_from_env` reads `EVERSCRIBE_PROJECT_ID` and
`EVERSCRIBE_API_KEY` and raises `ValueError` naming the missing variable if
either is unset or empty:

```python
es = everscribe.new_from_env()
```

Override defaults by passing options to the subclient constructor:

```python
from everscribe.recorder import OverflowPolicy

rec = es.new_recorder(
    buffer_size=2000,
    flush_interval=2.0,               # seconds
    overflow=OverflowPolicy.BLOCK,    # or the string "block"
)
```

Customers who only need the recorder can skip the root client:

```python
from everscribe import recorder

rec = recorder.new(project_id, api_key, buffer_size=2000)
```

Both shapes are supported. The root client is the recommended path once you
wire up more than one surface (recorder + minter); the direct factory is a
one-line shortcut for ingest-only setups.

| Option                 | Description                                                                              | Default        |
|------------------------|------------------------------------------------------------------------------------------|----------------|
| `buffer_size`          | Capacity of the in-memory event buffer.                                                  | `1000`         |
| `flush_size`           | Pending-event count that triggers an immediate flush.                                    | `100`          |
| `flush_interval`       | Maximum time between flushes (seconds) when the size threshold isn't reached.            | `5.0`          |
| `flush_timeout`        | Per-flush timeout (seconds) applied to each batch call against the inner recorder.       | `30.0`         |
| `overflow`             | Behavior when `record()` finds the buffer full. See [overflow policies](#overflow-policies). | `"drop-newest"` |
| `drain_timeout`        | Max time (seconds) `close()` waits for in-flight events to flush before raising.          | `30.0`         |
| `logger`               | A `logging.Logger` for SDK diagnostics (overflow warnings, flush errors).                | module logger  |
| `base_url`             | Override the ingestion endpoint. Used for tests and staging environments.                | production URL |
| `request_timeout`      | Per-request timeout (seconds) for HTTP calls.                                            | `10.0`         |
| `auto_idempotency_key` | Copy `event.id` into `event.idempotency_key` at send time when the latter is empty.      | off            |

> **Async note.** `record()` only enqueues an event (it never blocks on the
> network — a background thread does the HTTP flush), so it's safe to call from
> `async def` handlers without touching the event loop.

### 2. Define your actor resolver

The resolver bridges session-provisioned request state to an `Actor`. With the
ASGI adapter it receives the Starlette `Request`, so you read from
`request.state`, `request.session`, or whatever your auth middleware attaches:

```python
from everscribe.event import Actor
from everscribe.asgi import ActorResolver

def resolve_actor(request) -> Actor:
    user = getattr(request.state, "user", None)
    if user is None:
        return Actor(type="anonymous")
    return Actor(
        type="admin" if user.is_admin else "user",
        id=user.id,
        display_name=user.username,
        email=user.email,
    )
```

The resolver must not consume the request body.

### 3. Wire up the middleware

**Ordering matters.** The audit middleware must run **after** any middleware
that provisions the request with session data — the producer (session) has to
run before the consumer (audit, which calls your `resolve_actor`).

In Starlette/FastAPI, the `middleware=[...]` list runs **outermost-first**, so
list your session middleware before the audit middleware:

```python
from starlette.applications import Starlette
from starlette.middleware import Middleware
from everscribe.asgi import EverscribeMiddleware

app = Starlette(
    routes=routes,
    middleware=[
        Middleware(SessionMiddleware, ...),   # runs first (outer): attaches identity
        Middleware(EverscribeMiddleware, recorder=rec, resolve_actor=resolve_actor),
    ],
)
```

> If you use `app.add_middleware(...)` instead, note that it adds to the
> **outside** of the stack, so the **last** call runs first. Add
> `EverscribeMiddleware` first and your session middleware last to get the same
> ordering.

### 4. Record events in handlers

The middleware installs a per-request `Event`, pre-populated with Actor (from
your resolver) and Origin (from request headers). Reach it with
`current_event()`, enrich it during the handler, and the middleware
auto-records on response finish if `action` is set.

```python
from everscribe.event import Target
from everscribe.asgi import current_event

@app.post("/api-keys")
async def create_key(request):
    e = current_event()
    e.action = "api_key.create"

    key = await create_api_key(request)
    e.target = Target(type="api_key", id=key.id)
    return JSONResponse(key, status_code=201)
    # happy path: result auto-captures as {"status": "ok", "code": 201}
```

`current_event()` returns the request-scoped event anywhere in your handler
stack (it's backed by `contextvars`, so it works in both `async def` and plain
`def` endpoints). You can also read it off `request.state.everscribe_event` or
via `event_from_request(request)`.

#### Recording state changes

For mutation events, attach the before/after state with `diff()`. The
audit-log API computes the JSON Patch on ingest.

```python
from everscribe.event import Target, with_redacted_fields

@app.patch("/users/{id}")
async def update_user(request):
    e = current_event()
    e.action = "user.update"
    e.target = Target(type="user", id=request.path_params["id"])

    before = await load_user(request.path_params["id"])
    if before is None:
        return PlainTextResponse("not found", status_code=404)

    after = await save_user({**before, "email": (await request.json())["email"]})

    e.diff(before, after,
        with_redacted_fields("/password_hash"),  # scrub sensitive fields
    )
    return JSONResponse(after)
```

`with_redacted_fields` accepts [JSON Pointer](https://datatracker.ietf.org/doc/html/rfc6901)
paths (RFC 6901): leading `/`, slashes for nesting (`/billing/credit_card`),
integer segments for array indices (`/api_keys/0`). Paths that don't exist in
the document are silently skipped.

#### Recording multiple events per request

Some handlers fan out — one privileged operation can affect many resources,
and each one is independently audit-worthy (e.g. revoking every active session
for a compromised account). Call `from_context()` once per extra event so each
gets a fresh clone of the per-request template (Actor, Origin) without sharing
or mutating metadata:

```python
from everscribe.event import Target, from_context

@app.post("/users/{id}/sessions/revoke-all")
async def revoke_all(request):
    uid = request.path_params["id"]
    reason = (await request.json())["reason"]
    for s in await list_active_sessions(uid):
        e = from_context()  # fresh clone per session
        e.action = "session.revoke"
        e.target = Target(type="session", id=s.id)
        e.with_fields("user_id", uid, "reason", reason)
        try:
            await revoke_session(s.id)
        except Exception as err:
            e.result.status = "error"
            e.result.message = err
        rec.record(e)
    return Response(status_code=204)
```

The buffered recorder coalesces these (and events from other concurrent
requests) into a single batch call to the ingestion API on each flush — no
need to assemble batches yourself.

---

## Three key behaviors

**Empty `action` is a no-op.** The middleware skips auto-record entirely when
`current_event().action` is empty, so handlers that bail out before setting an
action produce no event:

```python
@app.post("/users/{id}/lock")
async def lock_user(request):
    user = await load_user(request.path_params["id"])
    if user is None:
        return PlainTextResponse("not found", status_code=404)
    if user.locked:
        return Response(status_code=200)
    # no action set on the two paths above — we don't record attempts to lock a
    # missing or already-locked user

    e = current_event()
    e.action = "user.lock"
    e.target = Target(type="user", id=user.id)
    await do_lock(user.id)
    return Response(status_code=200)
```

**Overriding the resolver's `actor`** — when there's no session yet (login,
signup) or when the actor isn't a session user (webhooks, system tasks), the
handler overrides `event.actor` directly. Login is the canonical case: at
handler entry the resolver returns `anonymous` because the session doesn't
exist until authentication succeeds.

```python
@app.post("/login")
async def login(request):
    e = current_event()
    body = await request.json()

    user = await authenticate(body)
    if user is None:
        # Failed login: actor stays "anonymous" from the resolver.
        e.action = "user.login_failed"
        e.with_fields("attempted_email", body["email"])
        return PlainTextResponse("invalid credentials", status_code=401)

    # Successful login: override the resolver's "anonymous".
    e.actor = Actor(type="user", id=user.id, display_name=user.username, email=user.email)
    e.action = "user.login"
    return issue_session_cookie(user)
```

**Explicit `result` wins over auto-capture** — when the HTTP status doesn't
reflect the operation's audit outcome. Password reset is the canonical case:
anti-enumeration security requires an identical user-facing response whether the
email matched or not, but audit monitoring still needs to know which happened:

```python
from everscribe.event import Result

@app.post("/password/reset")
async def reset(request):
    e = current_event()
    email = (await request.json())["email"]
    e.action = "password.reset_requested"
    e.with_fields("attempted_email", email)

    user = await lookup_by_email(email)
    if user is None:
        # Explicit denied overrides the auto-captured 303-redirect "ok".
        e.result = Result(status="denied", message="no account for email")
        return RedirectResponse("/password/check-your-email", status_code=303)

    await send_reset_email(user)
    e.target = Target(type="user", id=user.id)
    return RedirectResponse("/password/check-your-email", status_code=303)
    # happy path: auto-captures as {"status": "ok", "code": 303}
```

---

## The Event shape

```python
@dataclass
class Event:
    action: str                    # dotted verb, e.g. "user.lock"
    id: str                        # uuid v4; auto-generated
    occurred_at: datetime          # auto-populated (UTC)
    actor: Actor                   # who caused the event
    tenant_id: str                 # optional within-project dimension
    target: Target                 # what was acted on
    metadata: dict[str, Any]       # freeform context
    origin: Origin                 # IP, user-agent, request ID
    result: Result                 # outcome: ok | error | denied
    change: Change | None          # before/after state for mutations
    idempotency_key: str           # optional dedup key
```

Fields use plain values with empty defaults (`""`, `0`, empty nested
dataclasses) rather than optionals — empty means "not set", and the wire
serializer omits empty fields. The JSON sent to the ingestion API is
**snake_case** and byte-compatible with the Go and Node SDKs.

`project_id` is bound once at `new` and sent on every request as part of the
URL path.

`tenant_id` groups events one level above the actor — set it when you run a
multi-tenant SaaS and want events queryable per workspace, org, or connected
account. Single-tenant apps leave it blank.

`result.message` is free-form and special-cases exceptions — pass an
`Exception` directly and it serializes as `str(exc)`:

```python
e.result = Result(status="error", message=err)
```

Two helpers attach metadata in slog style:

```python
e.with_field("reason", "policy_violation")
e.with_fields("reason", "spam", "severity", "high", "count", 3)
```

For non-HTTP callers (background jobs, cron, CLI), build events directly:

```python
from everscribe.event import Event, Actor, Target

e = Event("subscription.trial_expired")
e.actor = Actor(type="system", id="trial_expirer")
e.target = Target(type="subscription", id=sub_id)
rec.record(e)
```

---

## BufferedRecorder

`recorder.new` (and `Client.new_recorder`) returns a `BufferedRecorder` —
events enqueue on an in-memory buffer and a flush is triggered when the size
threshold or interval is reached, flushed on a background thread. Tuning knobs
live in the [Quickstart options table](#1-bind-credentials-and-construct-subclients);
the subsections below cover runtime concerns.

### Overflow policies

When the buffer is full at `record()` time (`OverflowPolicy`, or the equivalent
string):

| Policy           | Behavior                                                             |
|------------------|----------------------------------------------------------------------|
| `"drop-newest"`  | Drop the incoming event, increment counter, log a warning. Default.  |
| `"block"`        | Wait for space (until the buffer drains or the recorder closes).     |
| `"error"`        | Raise `BufferFullError`.                                             |

A full buffer means you're misconfigured — resize, speed up downstream, or
scale out. Watch `stats().dropped`.

### `flush()`, `close()`, and `stats()`

`flush(timeout=None)` synchronously drains everything buffered at the time of
the call — useful for tests and graceful shutdown sync points. `close()` runs a
final drain, so you don't need to `flush()` before `close()`. Both are no-ops
after `close()`.

`stats()` exposes counters for export to Prometheus/Datadog:

```python
@dataclass
class BufferedStats:
    dropped: int       # total events dropped due to overflow
    flushed: int       # total events successfully flushed to the inner recorder
    flush_errs: int    # total flush calls that failed
    pending: int       # events currently in the buffer
    buffer_size: int   # buffer capacity
```

### Errors

The recorder package exports three error classes:

- `HTTPError` — non-2xx response from the ingestion endpoint. Read
  `status_code` and `body`; call `transient()` (5xx + 429) to distinguish
  retryable failures.
- `BufferFullError` — overflow with `overflow="error"`.
- `DrainTimeoutError` — `close()` exceeded `drain_timeout` with events still
  pending.

---

## Idempotency

`event.idempotency_key` is for caller-supplied stable keys — webhook event IDs,
upstream request IDs, anything that identifies "the same logical event" across
retries the SDK can't see:

```python
e.idempotency_key = stripe_event.id  # dedup if Stripe redelivers
```

For SDK-internal safety against double-sending the same Event object (e.g. a
manual `record()` plus an auto-record fired by the middleware), enable
`auto_idempotency_key`. It copies `event.id` into `idempotency_key` at send time
when the key is empty:

```python
rec = es.new_recorder(auto_idempotency_key=True)
```

Off by default. Caller-supplied keys always win — auto-population only fills
empty keys.

---

## Embedded views

The `everscribe.minter` module mints short-lived JWT tokens that let a
customer's frontend mount the Everscribe embeddable component (e.g.
`<EverscribeEvents />`) without exposing the project API key to the browser.

The flow has three actors:

1. **Customer's backend** (this SDK) holds the project API key and mints embed
   tokens via `Client.mint_token`.
2. **Customer's frontend** receives the token from a route the backend exposes
   and passes it as a prop to the component. Never sees the API key.
3. **Everscribe API** verifies the token on each read and scopes results to the
   token's claims (tenant, columns, actions).

### Minting a token

The cleanest path is via the root client, which already holds the credentials:

```python
import everscribe
from everscribe.minter import TokenOptions

es = everscribe.new(project_id, api_key)
m = es.new_minter()

token = m.mint_token(TokenOptions(
    tenant_id="acme-corp",
    expires_in=60 * 60,                    # seconds (or a datetime.timedelta)
    allowed_columns=["occurred_at", "action", "actor"],
    allowed_actions=["user.*", "billing.invoice.created"],
))
# token is a JWT string; hand it to the customer's frontend via your route.
```

Customers who only need the minter surface can construct it directly:

```python
from everscribe import minter

m = minter.Client(project_id, api_key)
token = m.mint_token(minter.TokenOptions(...))
```

### `TokenOptions`

| Field             | Type              | Behavior |
|-------------------|-------------------|----------|
| `tenant_id`       | `str`             | Optional. Scopes reads to events with the matching `tenant_id`. Trimmed by the SDK; rejected if empty after trim or > 256 chars. |
| `expires_in`      | `float`/`timedelta` | Token lifetime in **seconds** (or a `timedelta`). The server clamps to `[60s, 24h]`. Omit (or pass 0) to use the server default (1h). The SDK exports `MIN_EXPIRES_IN` and `MAX_EXPIRES_IN`. |
| `allowed_columns` | `list[str]`       | Optional whitelist of `Event` field names. `None` for no restriction; an empty list is rejected (avoids silently widening scope when built from filtered user input). The SDK exports `ALLOWED_COLUMNS`. |
| `allowed_actions` | `list[str]`       | Optional filter of allowed actions. Each entry is exact (`user.login`) or a suffix wildcard (`user.*`). `None` for no restriction; empty list rejected. Bare `*`, prefix wildcards (`*.create`), mid-string wildcards (`user.*.create`), and wildcards without a preceding dot (`user*`) are rejected. |
| `allowed_fields`  | `list[str]`       | Restricts which catalog fields the token's DSL/NLP queries may reference. Same `None` / empty-list semantics; validated server-side. |
| `allow_dsl_input` | `bool`            | Unlock the Query (advanced DSL) tab and `?q=` on the read API. Default `False`. |
| `allow_nlp`       | `bool`            | Unlock the AI ("Ask in plain English") tab and the NLP endpoint. Default `False`. |

### Errors

`mint_token` raises one of:

- `ValueError` from client-side validation (caller-supplied options fail the
  SDK's checks; no HTTP call is made).
- `MinterError` for non-2xx responses from the mint endpoint — `status_code`
  matches the spec: 400 for invalid options, 401 for bad auth, 404 for
  missing/soft-deleted project.
- A transport error (timeout, connection refused, network failure).

### Configuration

`minter.Client` accepts options analogous to the recorder:

- `base_url` — override the API host (tests, staging).
- `request_timeout` — per-request timeout in seconds.
- `transport` — supply a custom transport callable (primarily for tests).
