"""Integration tests for ExpiringOwnerBackend over MemoryBackend."""
from __future__ import annotations

import time

import pytest

from schedlock.backends.memory_backend import MemoryBackend
from schedlock.backends.expiring_owner_backend import ExpiringOwnerBackend
from schedlock.backends.namespaced_backend import NamespacedBackend


@pytest.fixture()
def memory():
    return MemoryBackend()


def test_valid_owner_acquires_and_releases(memory):
    future = time.time() + 9999
    b = ExpiringOwnerBackend(memory, expiry_fn=lambda o: future)
    assert b.acquire("cron:job", "w1", 60) is True
    assert b.is_locked("cron:job") is True
    assert b.release("cron:job", "w1") is True
    assert b.is_locked("cron:job") is False


def test_expired_owner_cannot_acquire(memory):
    past = time.time() - 1
    b = ExpiringOwnerBackend(memory, expiry_fn=lambda o: past)
    assert b.acquire("cron:job", "w1", 60) is False
    assert b.is_locked("cron:job") is False


def test_expired_owner_cannot_steal_held_lock(memory):
    """A valid owner holds the lock; an expired owner must not acquire."""
    past = time.time() - 1
    future = time.time() + 9999

    def expiry_fn(owner: str):
        return past if owner == "expired" else future

    b = ExpiringOwnerBackend(memory, expiry_fn=expiry_fn)
    assert b.acquire("cron:job", "valid", 60) is True
    assert b.acquire("cron:job", "expired", 60) is False


def test_expiring_owner_over_namespaced(memory):
    ns = NamespacedBackend(memory, namespace="ns1")
    future = time.time() + 9999
    b = ExpiringOwnerBackend(ns, expiry_fn=lambda o: future)
    assert b.acquire("job", "w1", 30) is True
    assert b.is_locked("job") is True
    assert b.release("job", "w1") is True
    assert b.is_locked("job") is False
