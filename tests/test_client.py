"""Tests for the top-level Client: construction, env loading, and subclients."""

from __future__ import annotations

import pytest

import everscribe
from everscribe import Client
from everscribe.minter import Client as MinterClient
from everscribe.recorder import BufferedRecorder


def test_new_returns_client() -> None:
    es = everscribe.new("proj", "key")
    assert isinstance(es, Client)
    assert es.project_id == "proj"


def test_new_trims_credentials() -> None:
    es = everscribe.new("  proj ", "  key ")
    assert es.project_id == "proj"


def test_new_empty_project_id_raises() -> None:
    with pytest.raises(ValueError, match="project_id is empty"):
        everscribe.new("   ", "key")


def test_new_empty_api_key_raises() -> None:
    with pytest.raises(ValueError, match="api_key is empty"):
        everscribe.new("proj", "   ")


def test_from_env_success(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("EVERSCRIBE_PROJECT_ID", "  penv ")
    monkeypatch.setenv("EVERSCRIBE_API_KEY", "kenv")
    es = everscribe.new_from_env()
    assert es.project_id == "penv"


def test_from_env_missing_project_id(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("EVERSCRIBE_PROJECT_ID", raising=False)
    monkeypatch.setenv("EVERSCRIBE_API_KEY", "k")
    with pytest.raises(ValueError, match="EVERSCRIBE_PROJECT_ID is not set or empty"):
        everscribe.new_from_env()


def test_from_env_missing_api_key(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("EVERSCRIBE_PROJECT_ID", "p")
    monkeypatch.delenv("EVERSCRIBE_API_KEY", raising=False)
    with pytest.raises(ValueError, match="EVERSCRIBE_API_KEY is not set or empty"):
        everscribe.new_from_env()


def test_from_env_classmethod(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("EVERSCRIBE_PROJECT_ID", "p")
    monkeypatch.setenv("EVERSCRIBE_API_KEY", "k")
    assert Client.from_env().project_id == "p"


def test_new_recorder_returns_buffered_recorder() -> None:
    es = everscribe.new("proj", "key")
    rec = es.new_recorder(base_url="https://x.test")
    try:
        assert isinstance(rec, BufferedRecorder)
    finally:
        rec.close()


def test_new_recorder_forwards_options() -> None:
    es = everscribe.new("proj", "key")
    rec = es.new_recorder(buffer_size=7)
    try:
        assert rec.stats().buffer_size == 7
    finally:
        rec.close()


def test_new_minter_returns_minter_client() -> None:
    es = everscribe.new("proj", "key")
    m = es.new_minter(base_url="https://x.test/")
    assert isinstance(m, MinterClient)
    assert m.project_id == "proj"
    assert m.base_url == "https://x.test"  # trailing slash trimmed


def test_api_key_not_a_public_attribute() -> None:
    es = everscribe.new("proj", "key")
    assert not hasattr(es, "api_key")


def test_top_level_exports() -> None:
    assert everscribe.Event("x").action == "x"
    assert hasattr(everscribe, "recorder")
    assert hasattr(everscribe, "minter")
    assert hasattr(everscribe, "event")
    assert everscribe.__version__
