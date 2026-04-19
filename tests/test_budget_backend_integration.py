"""Integration tests for BudgetBackend over MemoryBackend."""
import pytest
from schedlock.backends.memory_backend import MemoryBackend
from schedlock.backends.budget_backend import BudgetBackend
from schedlock.backends.namespaced_backend import NamespacedBackend


@pytest.fixture
def memory():
    return MemoryBackend()


def test_acquire_within_budget(memory):
    b = BudgetBackend(memory, max_acquires=5)
    for i in range(5):
        assert b.acquire(f"job:{i}", f"owner:{i}", 60) is True
    assert b.used == 5
    assert b.remaining == 0


def test_acquire_blocked_when_budget_zero(memory):
    b = BudgetBackend(memory, max_acquires=1)
    assert b.acquire("job", "w1", 60) is True
    b.release("job", "w1")
    assert b.acquire("job", "w2", 60) is False


def test_release_does_not_restore_budget(memory):
    b = BudgetBackend(memory, max_acquires=2)
    b.acquire("job", "w1", 60)
    b.release("job", "w1")
    b.acquire("job", "w2", 60)
    b.release("job", "w2")
    assert b.remaining == 0
    assert b.acquire("job", "w3", 60) is False


def test_budget_over_namespaced(memory):
    ns = NamespacedBackend(memory, namespace="ns")
    b = BudgetBackend(ns, max_acquires=2)
    assert b.acquire("job", "w1", 60) is True
    assert b.acquire("job2", "w2", 60) is True
    assert b.acquire("job3", "w3", 60) is False


def test_remaining_decrements_correctly(memory):
    b = BudgetBackend(memory, max_acquires=3)
    assert b.remaining == 3
    b.acquire("a", "w", 60)
    assert b.remaining == 2
    b.acquire("b", "w", 60)
    assert b.remaining == 1
