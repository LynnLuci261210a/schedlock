"""Tests for TrustingBackend."""
import re
from unittest.mock import MagicMock

import pytest

from schedlock.backends.base import BaseBackend
from schedlock.backends.memory_backend import MemoryBackend
from schedlock.backends.trusting_backend import TrustingBackend


@pytest.fixture()
def inner():
    m = MagicMock(spec=BaseBackend)
    m.acquire.return_value = True
    m.release.return_value = True
    m.is_locked.return_value = False
    m.refresh.return_value = True
    return m


@pytest.fixture()
def backend(inner):
    return TrustingBackend(inner, trusted=["alice", "bob"])


def test_requires_inner_backend():
    with pytest.raises(TypeError):
        TrustingBackend("not-a-backend", trusted=["alice"])


def test_requires_non_empty_trusted(inner):
    with pytest.raises(ValueError):
        TrustingBackend(inner, trusted=[])


def test_inner_property(inner, backend):
    assert backend.inner is inner


def test_trusted_owner_can_acquire(inner, backend):
    result = backend.acquire("job", "alice", 30)
    assert result is True
    inner.acquire.assert_called_once_with("job", "alice", 30)


def test_untrusted_owner_blocked(inner, backend):
    result = backend.acquire("job", "eve", 30)
    assert result is False
    inner.acquire.assert_not_called()


def test_trusted_owner_can_release(inner, backend):
    result = backend.release("job", "bob")
    assert result is True
    inner.release.assert_called_once_with("job", "bob")


def test_untrusted_owner_cannot_release(inner, backend):
    result = backend.release("job", "mallory")
    assert result is False
    inner.release.assert_not_called()


def test_is_locked_delegates(inner, backend):
    inner.is_locked.return_value = True
    assert backend.is_locked("job") is True
    inner.is_locked.assert_called_once_with("job")


def test_regex_pattern_trusted(inner):
    b = TrustingBackend(inner, trusted=[re.compile(r"^worker-\d+$")])
    assert b.acquire("job", "worker-42", 10) is True
    assert b.acquire("job", "admin", 10) is False


def test_callable_trusted(inner):
    b = TrustingBackend(inner, trusted=[lambda o: o.startswith("svc-")])
    assert b.acquire("job", "svc-scheduler", 10) is True
    assert b.acquire("job", "rogue", 10) is False


def test_refresh_trusted_owner(inner, backend):
    result = backend.refresh("job", "alice", 60)
    assert result is True
    inner.refresh.assert_called_once_with("job", "alice", 60)


def test_refresh_untrusted_owner(inner, backend):
    result = backend.refresh("job", "hacker", 60)
    assert result is False
    inner.refresh.assert_not_called()


def test_integration_with_memory_backend():
    memory = MemoryBackend()
    b = TrustingBackend(memory, trusted=["worker-1"])
    assert b.acquire("cron", "worker-1", 30) is True
    assert b.is_locked("cron") is True
    assert b.acquire("cron", "worker-2", 30) is False
    assert b.release("cron", "worker-1") is True
    assert b.is_locked("cron") is False
