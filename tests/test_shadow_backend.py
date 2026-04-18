"""Tests for ShadowBackend."""

import pytest
from unittest.mock import MagicMock
from schedlock.backends.shadow_backend import ShadowBackend
from schedlock.backends.memory_backend import MemoryBackend


def make_backend(acquire=True, release=True, is_locked=False):
    b = MagicMock()
    b.acquire.return_value = acquire
    b.release.return_value = release
    b.is_locked.return_value = is_locked
    b.refresh.return_value = True
    # satisfy isinstance check
    b.__class__ = MemoryBackend
    return b


@pytest.fixture
def primary():
    return MemoryBackend()


@pytest.fixture
def shadow_inner():
    return MemoryBackend()


@pytest.fixture
def backend(primary, shadow_inner):
    return ShadowBackend(primary, shadow_inner)


def test_requires_primary():
    with pytest.raises(TypeError, match="primary"):
        ShadowBackend("not-a-backend", MemoryBackend())


def test_requires_shadow():
    with pytest.raises(TypeError, match="shadow"):
        ShadowBackend(MemoryBackend(), "not-a-backend")


def test_inner_property(backend, primary):
    assert backend.inner is primary


def test_shadow_property(backend, shadow_inner):
    assert backend.shadow is shadow_inner


def test_acquire_delegates_to_primary(backend, primary, shadow_inner):
    result = backend.acquire("job", "owner-1", 30)
    assert result is True
    assert primary.is_locked("job") is True


def test_acquire_also_mirrors_to_shadow(backend, primary, shadow_inner):
    backend.acquire("job", "owner-1", 30)
    assert shadow_inner.is_locked("job") is True


def test_release_delegates_to_primary(backend, primary):
    backend.acquire("job", "owner-1", 30)
    result = backend.release("job", "owner-1")
    assert result is True
    assert primary.is_locked("job") is False


def test_release_mirrors_to_shadow(backend, shadow_inner):
    backend.acquire("job", "owner-1", 30)
    backend.release("job", "owner-1")
    assert shadow_inner.is_locked("job") is False


def test_is_locked_uses_primary_only(backend, primary, shadow_inner):
    primary.acquire("job", "owner-1", 30)
    # shadow not updated directly
    assert backend.is_locked("job") is True


def test_shadow_error_is_suppressed(primary):
    bad_shadow = MagicMock()
    bad_shadow.__class__ = MemoryBackend
    bad_shadow.acquire.side_effect = RuntimeError("shadow down")
    b = ShadowBackend(primary, bad_shadow)
    # should not raise
    result = b.acquire("job", "owner-1", 30)
    assert result is True


def test_shadow_error_is_logged(primary):
    import logging
    bad_shadow = MagicMock()
    bad_shadow.__class__ = MemoryBackend
    bad_shadow.acquire.side_effect = RuntimeError("shadow down")
    logger = MagicMock(spec=logging.Logger)
    b = ShadowBackend(primary, bad_shadow, logger=logger)
    b.acquire("job", "owner-1", 30)
    logger.warning.assert_called_once()
