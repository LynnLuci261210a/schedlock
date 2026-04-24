"""Tests for BurstBackend."""

from __future__ import annotations

import time
from unittest.mock import MagicMock

import pytest

from schedlock.backends.base import BaseBackend
from schedlock.backends.burst_backend import BurstBackend


@pytest.fixture()
def inner() -> MagicMock:
    mock = MagicMock(spec=BaseBackend)
    mock.acquire.return_value = True
    mock.release.return_value = True
    mock.is_locked.return_value = False
    mock.refresh.return_value = True
    return mock


@pytest.fixture()
def backend(inner: MagicMock) -> BurstBackend:
    return BurstBackend(inner, max_burst=3, burst_window=10.0)


# --- construction ---

def test_requires_inner_backend():
    with pytest.raises(TypeError):
        BurstBackend("not-a-backend", max_burst=2, burst_window=5.0)  # type: ignore


def test_requires_positive_max_burst(inner):
    with pytest.raises(ValueError):
        BurstBackend(inner, max_burst=0, burst_window=5.0)


def test_requires_positive_burst_window(inner):
    with pytest.raises(ValueError):
        BurstBackend(inner, max_burst=2, burst_window=0)


def test_requires_numeric_burst_window(inner):
    with pytest.raises(ValueError):
        BurstBackend(inner, max_burst=2, burst_window=-1.0)


# --- properties ---

def test_inner_property(inner, backend):
    assert backend.inner is inner


def test_max_burst_property(backend):
    assert backend.max_burst == 3


def test_burst_window_property(backend):
    assert backend.burst_window == 10.0


# --- acquire behaviour ---

def test_acquire_succeeds_within_burst(inner, backend):
    for _ in range(3):
        assert backend.acquire("job", "owner-1", 60) is True
    assert inner.acquire.call_count == 3


def test_acquire_blocked_after_burst_exhausted(inner, backend):
    for _ in range(3):
        backend.acquire("job", "owner-1", 60)
    result = backend.acquire("job", "owner-1", 60)
    assert result is False
    assert inner.acquire.call_count == 3  # 4th call never reaches inner


def test_burst_resets_after_window(inner, backend):
    # Exhaust the burst
    for _ in range(3):
        backend.acquire("job", "owner-1", 60)
    # Simulate window expiry by manipulating internal state
    backend._state["job"] = (3, time.monotonic() - 20.0)  # 20s ago > 10s window
    result = backend.acquire("job", "owner-1", 60)
    assert result is True


def test_burst_counts_independent_per_key(inner, backend):
    for _ in range(3):
        backend.acquire("job-a", "owner-1", 60)
    # job-b should still have its own fresh counter
    assert backend.acquire("job-b", "owner-1", 60) is True


def test_failed_inner_acquire_does_not_increment_burst(inner, backend):
    inner.acquire.return_value = False
    backend.acquire("job", "owner-1", 60)
    assert backend._state.get("job", (0, 0))[0] == 0


# --- delegation ---

def test_release_delegates(inner, backend):
    backend.release("job", "owner-1")
    inner.release.assert_called_once_with("job", "owner-1")


def test_is_locked_delegates(inner, backend):
    backend.is_locked("job")
    inner.is_locked.assert_called_once_with("job")


def test_refresh_delegates(inner, backend):
    backend.refresh("job", "owner-1", 60)
    inner.refresh.assert_called_once_with("job", "owner-1", 60)
