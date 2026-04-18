"""Integration tests for ShadowBackend over real MemoryBackend instances."""

import pytest
from schedlock.backends.shadow_backend import ShadowBackend
from schedlock.backends.memory_backend import MemoryBackend
from schedlock.backends.namespaced_backend import NamespacedBackend


@pytest.fixture
def primary():
    return MemoryBackend()


@pytest.fixture
def shadow():
    return MemoryBackend()


@pytest.fixture
def backend(primary, shadow):
    return ShadowBackend(primary, shadow)


def test_full_lifecycle_mirrored(backend, primary, shadow):
    assert backend.acquire("cron.daily", "worker-1", 60) is True
    assert primary.is_locked("cron.daily") is True
    assert shadow.is_locked("cron.daily") is True
    assert backend.release("cron.daily", "worker-1") is True
    assert primary.is_locked("cron.daily") is False
    assert shadow.is_locked("cron.daily") is False


def test_primary_blocks_second_acquire(backend):
    backend.acquire("cron.daily", "worker-1", 60)
    assert backend.acquire("cron.daily", "worker-2", 60) is False


def test_shadow_divergence_does_not_affect_primary(primary, shadow):
    """If shadow is pre-locked, primary result still wins."""
    shadow.acquire("cron.daily", "ghost", 60)
    b = ShadowBackend(primary, shadow)
    # primary is free so acquire should succeed
    assert b.acquire("cron.daily", "worker-1", 60) is True
    assert primary.is_locked("cron.daily") is True


def test_shadow_over_namespaced_primary():
    ns_primary = NamespacedBackend(MemoryBackend(), namespace="prod")
    ns_shadow = NamespacedBackend(MemoryBackend(), namespace="shadow")
    b = ShadowBackend(ns_primary, ns_shadow)
    assert b.acquire("job", "owner", 30) is True
    assert ns_primary.is_locked("job") is True
    assert ns_shadow.is_locked("job") is True
    b.release("job", "owner")
    assert ns_primary.is_locked("job") is False
