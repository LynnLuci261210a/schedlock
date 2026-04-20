"""Integration tests for AgingBackend over MemoryBackend."""
import pytest

from schedlock.backends.aging_backend import AgingBackend
from schedlock.backends.memory_backend import MemoryBackend
from schedlock.backends.namespaced_backend import NamespacedBackend


@pytest.fixture
def memory():
    return MemoryBackend()


def test_full_lifecycle_ttl_grows(memory):
    b = AgingBackend(memory, growth_factor=2.0, max_ttl=1000.0)
    # First acquire — TTL should be 60
    assert b.acquire("job", "w1", 60.0) is True
    assert b.release("job", "w1") is True
    # Second acquire — TTL grows to 120 but memory doesn't expose it directly;
    # we just verify the acquire succeeds and state is consistent.
    assert b.acquire("job", "w1", 60.0) is True
    assert b.is_locked("job") is True
    assert b.release("job", "w1") is True
    assert b.is_locked("job") is False


def test_second_worker_blocked_while_first_holds(memory):
    b = AgingBackend(memory, growth_factor=2.0, max_ttl=1000.0)
    assert b.acquire("job", "w1", 60.0) is True
    assert b.acquire("job", "w2", 60.0) is False
    b.release("job", "w1")
    assert b.acquire("job", "w2", 60.0) is True


def test_release_resets_age_integration(memory):
    b = AgingBackend(memory, growth_factor=3.0, max_ttl=1000.0)
    # Acquire and release several times; each release resets count
    for _ in range(4):
        assert b.acquire("job", "w1", 10.0) is True
        assert b.release("job", "w1") is True
    # After reset, age counter is 0 each time — no unbounded growth
    assert "job" not in b._state


def test_aging_over_namespaced(memory):
    ns = NamespacedBackend(memory, namespace="ns1")
    b = AgingBackend(ns, growth_factor=2.0, max_ttl=500.0)
    assert b.acquire("task", "owner", 30.0) is True
    assert b.is_locked("task") is True
    assert b.release("task", "owner") is True
    assert b.is_locked("task") is False


def test_independent_keys_age_separately(memory):
    b = AgingBackend(memory, growth_factor=2.0, max_ttl=1000.0)
    assert b.acquire("job-a", "w", 10.0) is True
    assert b.acquire("job-b", "w", 10.0) is True
    b.release("job-a", "w")
    # job-b still held, job-a age reset
    assert "job-a" not in b._state
    assert "job-b" in b._state
