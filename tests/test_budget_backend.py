"""Tests for BudgetBackend."""
import pytest
from unittest.mock import MagicMock
from schedlock.backends.budget_backend import BudgetBackend
from schedlock.backends.base import BaseBackend
from schedlock.backends.memory_backend import MemoryBackend


@pytest.fixture
def inner():
    m = MagicMock(spec=BaseBackend)
    m.acquire.return_value = True
    m.release.return_value = True
    m.is_locked.return_value = False
    m.refresh.return_value = True
    return m


@pytest.fixture
def backend(inner):
    return BudgetBackend(inner, max_acquires=3)


def test_requires_inner_backend():
    with pytest.raises(TypeError):
        BudgetBackend("not-a-backend", max_acquires=3)


def test_requires_positive_max_acquires(inner):
    with pytest.raises(ValueError):
        BudgetBackend(inner, max_acquires=0)
    with pytest.raises(ValueError):
        BudgetBackend(inner, max_acquires=-1)


def test_inner_property(inner, backend):
    assert backend.inner is inner


def test_max_acquires_property(backend):
    assert backend.max_acquires == 3


def test_initial_used_and_remaining(backend):
    assert backend.used == 0
    assert backend.remaining == 3


def test_acquire_success_increments_count(inner, backend):
    assert backend.acquire("k", "o", 60) is True
    assert backend.used == 1
    assert backend.remaining == 2


def test_acquire_blocked_after_budget_exhausted(inner, backend):
    for _ in range(3):
        backend.acquire("k", "o", 60)
    assert backend.used == 3
    assert backend.remaining == 0
    result = backend.acquire("k", "o2", 60)
    assert result is False
    assert inner.acquire.call_count == 3  # 4th call never reaches inner


def test_failed_inner_acquire_does_not_increment(inner, backend):
    inner.acquire.return_value = False
    backend.acquire("k", "o", 60)
    assert backend.used == 0


def test_release_delegates(inner, backend):
    backend.release("k", "o")
    inner.release.assert_called_once_with("k", "o")


def test_is_locked_delegates(inner, backend):
    backend.is_locked("k")
    inner.is_locked.assert_called_once_with("k")


def test_refresh_delegates(inner, backend):
    backend.refresh("k", "o", 60)
    inner.refresh.assert_called_once_with("k", "o", 60)


def test_budget_over_memory_full_lifecycle():
    mem = MemoryBackend()
    b = BudgetBackend(mem, max_acquires=2)
    assert b.acquire("job", "w1", 60) is True
    assert b.release("job", "w1") is True
    assert b.acquire("job", "w2", 60) is True
    assert b.release("job", "w2") is True
    # budget exhausted
    assert b.acquire("job", "w3", 60) is False
