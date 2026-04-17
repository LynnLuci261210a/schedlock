"""Integration tests for ExpiringBackend with MemoryBackend."""

from __future__ import annotations

import time

import pytest

from schedlock.backends.expiring_backend import ExpiringBackend
from schedlock.backends.memory_backend import MemoryBackend
from schedlock.backends.namespaced_backend import NamespacedBackend


@pytest.fixture()
def memory():
    return MemoryBackend()


def test_expiring_over_memory_basic_lifecycle(memory):
    backend = ExpiringBackend(memory, max_age=10)
    assert backend.acquire("cron:job", "w1", 30) is True
    assert backend.is_locked("cron:job") is True
    assert backend.release("cron:job", "w1") is True
    assert backend.is_locked("cron:job") is False


def test_expiring_evicts_and_allows_reacquire(memory):
    backend = ExpiringBackend(memory, max_age=0.05)
    backend.acquire("cron:job", "w1", 300)
    time.sleep(0.1)
    assert backend.acquire("cron:job", "w2", 300) is True
    assert backend._registry["cron:job"][0] == "w2"


def test_expiring_over_namespaced(memory):
    ns = NamespacedBackend(memory, namespace="prod")
    backend = ExpiringBackend(ns, max_age=0.05)
    backend.acquire("job", "w1", 60)
    time.sleep(0.1)
    assert backend.is_locked("job") is False
    assert backend.acquire("job", "w2", 60) is True


def test_multiple_keys_evict_independently(memory):
    backend = ExpiringBackend(memory, max_age=0.05)
    backend.acquire("job:a", "w1", 60)
    time.sleep(0.1)
    backend.acquire("job:b", "w2", 60)
    # job:a expired, job:b fresh
    assert backend.is_locked("job:a") is False
    assert backend.is_locked("job:b") is True
