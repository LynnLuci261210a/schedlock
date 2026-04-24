"""Tests for DampeningBackend."""
from __future__ import annotations

import time
from unittest.mock import MagicMock

import pytest

from schedlock.backends.base import BaseBackend
from schedlock.backends.dampening_backend import DampeningBackend


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
    return DampeningBackend(inner, dampening_seconds=2.0)


def test_requires_inner_backend():
    with pytest.raises(TypeError):
        DampeningBackend(object(), dampening_seconds=1.0)  # type: ignore[arg-type]


def test_requires_positive_dampening_seconds(inner):
    with pytest.raises(ValueError):
        DampeningBackend(inner, dampening_seconds=0)
    with pytest.raises(ValueError):
        DampeningBackend(inner, dampening_seconds=-1.0)


def test_inner_property(inner, backend):
    assert backend.inner is inner


def test_dampening_seconds_property(backend):
    assert backend.dampening_seconds == 2.0


def test_acquire_delegates_to_inner_on_success(inner, backend):
    inner.acquire.return_value = True
    assert backend.acquire("job", "worker-1", 30) is True
    inner.acquire.assert_called_once_with("job", "worker-1", 30)


def test_failed_acquire_dampens_key(inner, backend):
    inner.acquire.return_value = False
    assert backend.acquire("job", "worker-1", 30) is False
    # Second attempt should be blocked without consulting inner
    assert backend.acquire("job", "worker-1", 30) is False
    assert inner.acquire.call_count == 1


def test_dampening_expires_after_window(inner):
    backend = DampeningBackend(inner, dampening_seconds=0.05)
    inner.acquire.return_value = False
    backend.acquire("job", "worker-1", 30)
    time.sleep(0.1)
    # Dampening should have expired; inner should be consulted again
    inner.acquire.return_value = True
    assert backend.acquire("job", "worker-1", 30) is True
    assert inner.acquire.call_count == 2


def test_release_clears_dampening(inner, backend):
    inner.acquire.return_value = False
    backend.acquire("job", "worker-1", 30)
    backend.release("job", "worker-1")
    # After release the dampening should be gone
    inner.acquire.return_value = True
    assert backend.acquire("job", "worker-1", 30) is True
    assert inner.acquire.call_count == 2


def test_release_delegates_to_inner(inner, backend):
    backend.release("job", "worker-1")
    inner.release.assert_called_once_with("job", "worker-1")


def test_is_locked_delegates_to_inner(inner, backend):
    inner.is_locked.return_value = True
    assert backend.is_locked("job") is True
    inner.is_locked.assert_called_once_with("job")


def test_refresh_delegates_to_inner(inner, backend):
    backend.refresh("job", "worker-1", 60)
    inner.refresh.assert_called_once_with("job", "worker-1", 60)


def test_different_keys_dampened_independently(inner, backend):
    inner.acquire.return_value = False
    backend.acquire("job-a", "worker-1", 30)
    # job-b has not failed yet — inner should still be called
    backend.acquire("job-b", "worker-1", 30)
    assert inner.acquire.call_count == 2
