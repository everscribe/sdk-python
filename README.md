# Everscribe Python SDK

Python SDK for the [Everscribe](https://everscribe.io) audit-log API.

> **Status: work in progress.** This package is being built to parity with the
> [Go](https://github.com/everscribe/sdk-go) and
> [Node](https://github.com/everscribe/sdk-node) SDKs. The API is not yet stable.

The SDK exposes two coordinated surfaces:

- **Recorder** — append-only event ingest: who did what, when, on what resource,
  and how state changed.
- **Minter** — short-lived embed tokens that let your frontend render audit-log
  views without exposing your API key.

The core is **zero-dependency** (standard library only). An optional
[FastAPI](https://fastapi.tiangolo.com/) / [Starlette](https://www.starlette.io/)
ASGI adapter installs a request-scoped event and auto-records on response finish;
install it with the `fastapi` extra. The core works with any framework (or none)
without it.

## Install

```sh
pip install everscribe            # core only, zero dependencies
pip install "everscribe[fastapi]" # + ASGI adapter (Starlette/FastAPI)
```

## Quickstart

_Documentation is being written as the SDK lands. See the Go and Node SDK
READMEs for the shape of the API in the meantime._
