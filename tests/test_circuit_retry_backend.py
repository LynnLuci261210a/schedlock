"""Tests for CircuitRetryBackend."""
from __future__ import annotations

import pytest
from unittest.mock import MagicMock, patch

from schedlock.backends.circuit_retry_backend import CircuitRetryBackend
from schedlock.backends.memory_backend import MemoryBackend


@pytest.fixture()
def inner():
    return MagicMock(spec=["acquire", "release", "is_locked", "refresh"])


@pytest.fixture()
def backend(inner):
    return CircuitRetryBackend(inner, retries=3, delay=0.0, failure_threshold=3, reset_timeout=60.0)


# ---------------------------------------------------------------------------
# Construction validation
# ---------------------------------------------------------------------------

def test_requires_inner_backend():
    with pytest.raises(TypeError):
        CircuitRetryBackend("not-a-backend")


def test_requires_positive_retries(inner):
    with pytest.raises(ValueError):
        CircuitRetryBackend(inner, retries=0)


def test_requires_non_negative_delay(inner):
    with pytest.raises(ValueError):
        CircuitRetryBackend(inner, delay=-0.1)


def test_requires_positive_failure_threshold(inner):
    with pytest.raises(ValueError):
        CircuitRetryBackend(inner, failure_threshold=0)


def test_requires_positive_reset_timeout(inner):
    with pytest.raises(ValueError):
        CircuitRetryBackend(inner, reset_timeout=0)


# ---------------------------------------------------------------------------
# inner property
# ---------------------------------------------------------------------------

def test_inner_property(inner, backend):
    assert backend.inner is inner


# ---------------------------------------------------------------------------
# Acquire — happy path
# ---------------------------------------------------------------------------

def test_acquire_delegates_on_success(inner, backend):
    inner.acquire.return_value = True
    assert backend.acquire("k", "owner", 30) is True
    inner.acquire.assert_called_once_with("k", "owner", 30)


def test_acquire_retries_on_exception(inner, backend):
    inner.acquire.side_effect = [RuntimeError("boom"), RuntimeError("boom"), True]
    assert backend.acquire("k", "owner", 30) is True
    assert inner.acquire.call_count == 3


def test_acquire_returns_false_after_all_retries_fail(inner, backend):
    inner.acquire.side_effect = RuntimeError("boom")
    assert backend.acquire("k", "owner", 30) is False
    assert inner.acquire.call_count == 3


# ---------------------------------------------------------------------------
# Circuit opens after threshold failures
# ---------------------------------------------------------------------------

def test_circuit_opens_after_threshold(inner, backend):
    inner.acquire.side_effect = RuntimeError("boom")
    # Each call exhausts 3 retries → 3 failures per call → threshold=3 trips on first call
    backend.acquire("k", "owner", 30)
    assert backend.is_open is True


def test_acquire_blocked_when_circuit_open(inner, backend):
    inner.acquire.side_effect = RuntimeError("boom")
    backend.acquire("k", "owner", 30)  # trips circuit
    inner.acquire.reset_mock()
    result = backend.acquire("k", "owner", 30)
    assert result is False
    inner.acquire.assert_not_called()


# ---------------------------------------------------------------------------
# Circuit resets after timeout
# ---------------------------------------------------------------------------

def test_circuit_half_opens_after_reset_timeout(inner, backend):
    import time
    inner.acquire.side_effect = RuntimeError("boom")
    backend.acquire("k", "owner", 30)  # trips circuit
    assert backend.is_open is True

    # Simulate time passing beyond reset_timeout
    backend._opened_at -= 61.0
    assert backend.is_open is False


def test_success_clears_circuit(inner, backend):
    inner.acquire.side_effect = [RuntimeError("x"), RuntimeError("x"), True]
    backend.acquire("k", "owner", 30)
    assert backend.is_open is False
    assert len(backend._failures) == 0


# ---------------------------------------------------------------------------
# release / is_locked / refresh
# ---------------------------------------------------------------------------

def test_release_delegates(inner, backend):
    inner.release.return_value = True
    assert backend.release("k", "owner") is True


def test_is_locked_delegates(inner, backend):
    inner.is_locked.return_value = False
    assert backend.is_locked("k") is False


def test_refresh_delegates(inner, backend):
    inner.refresh.return_value = True
    assert backend.refresh("k", "owner", 30) is True


def test_is_locked_returns_false_when_circuit_open(inner, backend):
    inner.acquire.side_effect = RuntimeError("boom")
    backend.acquire("k", "owner", 30)
    inner.is_locked.reset_mock()
    assert backend.is_locked("k") is False
    inner.is_locked.assert_not_called()
