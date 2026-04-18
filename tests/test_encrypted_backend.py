"""Tests for EncryptedBackend."""

import pytest
from unittest.mock import MagicMock

from schedlock.backends.base import BaseBackend
from schedlock.backends.encrypted_backend import EncryptedBackend
from schedlock.backends.memory_backend import MemoryBackend


@pytest.fixture
def inner():
    return MagicMock(spec=BaseBackend)


@pytest.fixture
def backend(inner):
    return EncryptedBackend(inner, secret="supersecret")


def test_requires_inner_backend():
    with pytest.raises(TypeError):
        EncryptedBackend("not-a-backend", secret="s")


def test_requires_non_empty_secret(inner):
    with pytest.raises(ValueError):
        EncryptedBackend(inner, secret="")


def test_requires_string_secret(inner):
    with pytest.raises(ValueError):
        EncryptedBackend(inner, secret=None)


def test_inner_property(inner, backend):
    assert backend.inner is inner


def test_acquire_encrypts_owner(inner, backend):
    inner.acquire.return_value = True
    result = backend.acquire("job", "alice", 60)
    assert result is True
    called_owner = inner.acquire.call_args[0][1]
    assert called_owner != "alice"


def test_release_encrypts_owner(inner, backend):
    inner.release.return_value = True
    backend.release("job", "alice")
    called_owner = inner.release.call_args[0][1]
    assert called_owner != "alice"


def test_same_owner_produces_same_encrypted_value(inner, backend):
    inner.acquire.return_value = True
    inner.release.return_value = True
    backend.acquire("job", "bob", 30)
    backend.release("job", "bob")
    enc_acquire = inner.acquire.call_args[0][1]
    enc_release = inner.release.call_args[0][1]
    assert enc_acquire == enc_release


def test_different_secrets_produce_different_encrypted_values(inner):
    b1 = EncryptedBackend(inner, secret="secret1")
    b2 = EncryptedBackend(inner, secret="secret2")
    inner.acquire.return_value = True
    b1.acquire("job", "alice", 60)
    enc1 = inner.acquire.call_args[0][1]
    b2.acquire("job", "alice", 60)
    enc2 = inner.acquire.call_args[0][1]
    assert enc1 != enc2


def test_is_locked_delegates(inner, backend):
    inner.is_locked.return_value = True
    assert backend.is_locked("job") is True
    inner.is_locked.assert_called_once_with("job")


def test_refresh_encrypts_owner(inner, backend):
    inner.refresh.return_value = True
    backend.refresh("job", "alice", 60)
    called_owner = inner.refresh.call_args[0][1]
    assert called_owner != "alice"


def test_integration_with_memory_backend():
    mem = MemoryBackend()
    b = EncryptedBackend(mem, secret="topsecret")
    assert b.acquire("cron:daily", "worker-1", 60) is True
    assert b.is_locked("cron:daily") is True
    assert b.acquire("cron:daily", "worker-2", 60) is False
    assert b.release("cron:daily", "worker-1") is True
    assert b.is_locked("cron:daily") is False
