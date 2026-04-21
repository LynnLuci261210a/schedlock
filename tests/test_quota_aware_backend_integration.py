"""Integration tests for QuotaAwareBackend over MemoryBackend."""

import time

import pytest

from schedlock.backends.memory_backend import MemoryBackend
from schedlock.backends.namespaced_backend import NamespacedBackend
from schedlock.backends.quota_aware_backend import QuotaAwareBackend


@pytest.fixture
def memory():
    return MemoryBackend()


def test_full_lifecycle(memory):
    b = QuotaAwareBackend(memory, max_per_owner=3, window=60.0)
    assert b.acquire("job", "owner-a", 30) is True
    assert b.is_locked("job") is True
    assert b.release("job", "owner-a") is True
    assert b.is_locked("job") is False


def test_quota_enforced_over_memory(memory):
    b = QuotaAwareBackend(memory, max_per_owner=2, window=60.0)
    assert b.acquire("job", "owner-a", 1) is True
    memory.release("job", "owner-a")  # release via inner so quota stays consumed
    assert b.acquire("job", "owner-a", 1) is True
    memory.release("job", "owner-a")
    # third attempt should be blocked by quota
    assert b.acquire("job", "owner-a", 1) is False


def test_second_worker_independent_quota(memory):
    b = QuotaAwareBackend(memory, max_per_owner=1, window=60.0)
    assert b.acquire("job", "owner-a", 30) is True
    b.release("job", "owner-a")
    # owner-a exhausted; owner-b still has quota
    assert b.acquire("job", "owner-b", 30) is True


def test_quota_window_resets_allow_reacquire(memory):
    b = QuotaAwareBackend(memory, max_per_owner=1, window=0.05)
    b.acquire("job", "owner-a", 1)
    memory.release("job", "owner-a")
    assert b.acquire("job", "owner-a", 1) is False
    time.sleep(0.1)
    assert b.acquire("job", "owner-a", 1) is True


def test_quota_aware_over_namespaced(memory):
    ns = NamespacedBackend(memory, namespace="svc")
    b = QuotaAwareBackend(ns, max_per_owner=2, window=60.0)
    assert b.acquire("task", "owner-x", 30) is True
    assert b.quota_used("owner-x") == 1
    assert b.quota_remaining("owner-x") == 1
