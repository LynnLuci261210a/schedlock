"""Tests for CircuitBreakerBackend."""

import time
import pytest
from unittest.mock import MagicMock
from schedlock.backends.circuit_breaker_backend import CircuitBreakerBackend
from schedlock.backends.memory_backend import MemoryBackend


@pytest.fixture
def inner():
    return MagicMock(spec=MemoryBackend)


@pytest.fixture
def backend(inner):
    return CircuitBreakerBackend(inner, failure_threshold=3, window=60.0, recovery_timeout=30.0)


def test_requires_inner_backend():
    with pytest.raises(TypeError):
        CircuitBreakerBackend("not-a-backend")


def test_requires_positive_failure_threshold(inner):
    with pytest.raises(ValueError):
        CircuitBreakerBackend(inner, failure_threshold=0)


def test_requires_positive_window(inner):
    with pytest.raises(ValueError)Backend(inner, window=0)


def test_requires_positive_recovery_timeout(inner):
    with pytest.raises(ValueError):
        CircuitBreakerBackend(inner, recovery_timeout=0)


def test_inner, backend):
    assert backend.inner is inner


def test_acquire_delegates_to_inner(inner, backend):
    inner.acquire.return_value = True
    assert backend.acquire("job", "owner1", 60) is True
    inner.acquire.assert_called_once_with("job", "owner1", 60)


def test_acquire_returns_false_on_exception_and_counts_failure(inner, backend):
    inner.acquire.side_effect = ConnectionError("redis down")
    result = backend.acquire("job", "owner1", 60)
    assert result is False


def test_circuit_opens_after_threshold(inner, backend):
    inner.acquire.side_effect = ConnectionError("redis down")
    for _ in range(3):
        backend.acquire("job", "owner1", 60)
    assert backend.circuit_open is True


def test_acquire_blocked_when_circuit_open(inner, backend):
    inner.acquire.side_effect = ConnectionError("redis down")
    for _ in range(3):
        backend.acquire("job", "owner1", 60)
    inner.acquire.side_effect = None
    inner.acquire.return_value = True
    result = backend.acquire("job", "owner1", 60)
    assert result is False
    assert inner.acquire.call_count == 3  # not called again


def test_circuit_recovers_after_timeout(inner, backend):
    inner.acquire.side_effect = ConnectionError()
    for _ in range(3):
        backend.acquire("job", "owner1", 60)
    assert backend.circuit_open is True
    backend._opened_at = time.time() - 31  # simulate timeout elapsed
    assert backend.circuit_open is False
    inner.acquire.side_effect = None
    inner.acquire.return_value = True
    assert backend.acquire("job", "owner1", 60) is True


def test_release_delegates(inner, backend):
    inner.release.return_value = True
    assert backend.release("job", "owner1") is True
    inner.release.assert_called_once_with("job", "owner1")


def test_is_locked_delegates(inner, backend):
    inner.is_locked.return_value = False
    assert backend.is_locked("job") is False
    inner.is_locked.assert_called_once_with("job")
