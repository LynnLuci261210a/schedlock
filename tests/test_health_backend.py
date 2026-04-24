"""Unit tests for HealthBackend."""
from __future__ import annotations

import time
from unittest.mock import MagicMock

import pytest

from schedlock.backends.base import BaseBackend
from schedlock.backends.health_backend import HealthBackend


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
    return HealthBackend(inner, failure_threshold=3, recovery_window=60.0)


def test_requires_inner_backend():
    with pytest.raises(TypeError):
        HealthBackend("not-a-backend")


def test_requires_positive_failure_threshold(inner):
    with pytest.raises(ValueError):
        HealthBackend(inner, failure_threshold=0)
    with pytest.raises(ValueError):
        HealthBackend(inner, failure_threshold=-1)


def test_requires_positive_recovery_window(inner):
    with pytest.raises(ValueError):
        HealthBackend(inner, recovery_window=0)
    with pytest.raises(ValueError):
        HealthBackend(inner, recovery_window=-5.0)


def test_inner_property(inner, backend):
    assert backend.inner is inner


def test_initially_healthy(backend):
    assert backend.is_healthy is True


def test_acquire_success_resets_failures(inner, backend):
    inner.acquire.return_value = False
    backend.acquire("k", "o", 10)
    inner.acquire.return_value = True
    backend.acquire("k", "o", 10)
    assert backend.consecutive_failures == 0


def test_consecutive_failures_tracked(inner, backend):
    inner.acquire.return_value = False
    backend.acquire("k", "o", 10)
    backend.acquire("k", "o", 10)
    assert backend.consecutive_failures == 2


def test_becomes_unhealthy_after_threshold(inner, backend):
    inner.acquire.return_value = False
    for _ in range(3):
        backend.acquire("k", "o", 10)
    assert backend.is_healthy is False


def test_unhealthy_backend_blocks_acquire(inner, backend):
    inner.acquire.return_value = False
    for _ in range(3):
        backend.acquire("k", "o", 10)
    inner.acquire.return_value = True
    result = backend.acquire("k", "o", 10)
    assert result is False
    # inner should NOT have been called the 4th time
    assert inner.acquire.call_count == 3


def test_recovery_after_window(inner, backend):
    inner.acquire.return_value = False
    for _ in range(3):
        backend.acquire("k", "o", 10)
    # Simulate recovery window elapsed
    backend._unhealthy_since = time.monotonic() - 61.0
    assert backend.is_healthy is True


def test_release_delegates(inner, backend):
    backend.release("k", "o")
    inner.release.assert_called_once_with("k", "o")


def test_is_locked_delegates(inner, backend):
    backend.is_locked("k")
    inner.is_locked.assert_called_once_with("k")


def test_refresh_delegates(inner, backend):
    backend.refresh("k", "o", 30)
    inner.refresh.assert_called_once_with("k", "o", 30)
