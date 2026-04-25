"""Verify TokenRelayBackend is exported from schedlock.backends."""
import pytest

from schedlock.backends.base import BaseBackend
from schedlock.backends.memory_backend import MemoryBackend
from schedlock.backends.token_relay_backend import TokenRelayBackend


def test_token_relay_backend_importable():
    assert TokenRelayBackend is not None


def test_token_relay_backend_is_base_subclass():
    assert issubclass(TokenRelayBackend, BaseBackend)


def test_token_relay_backend_instantiable():
    inner = MemoryBackend()
    backend = TokenRelayBackend(inner, token_fn=lambda: "tok")
    assert isinstance(backend, TokenRelayBackend)


def test_token_relay_backend_has_expected_attrs():
    inner = MemoryBackend()
    backend = TokenRelayBackend(inner, token_fn=lambda: "tok")
    assert hasattr(backend, "inner")
    assert hasattr(backend, "token_fn")
    assert hasattr(backend, "acquire")
    assert hasattr(backend, "release")
    assert hasattr(backend, "is_locked")
    assert hasattr(backend, "refresh")
