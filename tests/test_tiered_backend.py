"""Tests for TieredBackend."""

import pytest

from schedlock.backends.memory_backend import MemoryBackend
from schedlock.backends.tiered_backend import TieredBackend


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def make_backend() -> MemoryBackend:
    return MemoryBackend()


@pytest.fixture()
def primary() -> MemoryBackend:
    return make_backend()


@pytest.fixture()
def secondary() -> MemoryBackend:
    return make_backend()


@pytest.fixture()
def backend(primary, secondary) -> TieredBackend:
    return TieredBackend([primary, secondary])


# ---------------------------------------------------------------------------
# Construction
# ---------------------------------------------------------------------------

def test_requires_at_least_one_backend():
    with pytest.raises(ValueError, match="at least one"):
        TieredBackend([])


def test_backends_property(primary, secondary, backend):
    assert backend.backends == [primary, secondary]


def test_backends_property_is_copy(backend, primary, secondary):
    lst = backend.backends
    lst.clear()
    assert len(backend.backends) == 2


# ---------------------------------------------------------------------------
# acquire
# ---------------------------------------------------------------------------

def test_acquire_succeeds_on_primary(primary, backend):
    assert backend.acquire("job", "owner-1", 60) is True
    assert primary.is_locked("job")


def test_acquire_falls_back_to_secondary_when_primary_blocked(primary, secondary, backend):
    # Lock primary externally so it is unavailable.
    primary.acquire("job", "other", 60)
    # TieredBackend should fall back and succeed on secondary.
    assert backend.acquire("job", "owner-1", 60) is True
    assert secondary.is_locked("job")


def test_acquire_returns_false_when_all_blocked(primary, secondary, backend):
    primary.acquire("job", "other", 60)
    secondary.acquire("job", "other", 60)
    assert backend.acquire("job", "owner-1", 60) is False


# ---------------------------------------------------------------------------
# release
# ---------------------------------------------------------------------------

def test_release_delegates_to_all_backends(primary, secondary, backend):
    primary.acquire("job", "owner-1", 60)
    secondary.acquire("job", "owner-1", 60)
    result = backend.release("job", "owner-1")
    assert result is True
    assert not primary.is_locked("job")
    assert not secondary.is_locked("job")


def test_release_returns_false_when_none_held(backend):
    assert backend.release("job", "owner-1") is False


# ---------------------------------------------------------------------------
# is_locked
# ---------------------------------------------------------------------------

def test_is_locked_true_if_any_backend_locked(primary, secondary, backend):
    secondary.acquire("job", "owner-1", 60)
    assert backend.is_locked("job") is True


def test_is_locked_false_when_none_locked(backend):
    assert backend.is_locked("job") is False


# ---------------------------------------------------------------------------
# refresh
# ---------------------------------------------------------------------------

def test_refresh_returns_true_when_any_refreshed(primary, secondary, backend):
    primary.acquire("job", "owner-1", 60)
    assert backend.refresh("job", "owner-1", 120) is True


def test_refresh_returns_false_when_none_own(backend):
    assert backend.refresh("job", "nobody", 60) is False
