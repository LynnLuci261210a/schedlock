"""Tests for FallbackBackend."""

import pytest
from unittest.mock import MagicMock
from schedlock.backends.fallback_backend import FallbackBackend
from schedlock.backends.base import BaseBackend


def make_backend(acquire_result=True, is_locked_result=False, release_result=True, raises=False):
    b = MagicMock(spec=BaseBackend)
    if raises:
        b.acquire.side_effect = RuntimeError("backend unavailable")
    else:
        b.acquire.return_value = acquire_result
    b.release.return_value = release_result
    b.is_locked.return_value = is_locked_result
    b.refresh.return_value = True
    return b


def test_requires_primary():
    with pytest.raises(ValueError, match="primary"):
        FallbackBackend(None, make_backend())


def test_requires_secondary():
    with pytest.raises(ValueError, match="secondary"):
        FallbackBackend(make_backend(), None)


def test_acquire_uses_primary_when_available():
    primary = make_backend(acquire_result=True)
    secondary = make_backend()
    fb = FallbackBackend(primary, secondary)
    assert fb.acquire("job", 60, "owner1") is True
    primary.acquire.assert_called_once_with("job", 60, "owner1")
    secondary.acquire.assert_not_called()


def test_acquire_falls_back_to_secondary_when_primary_fails():
    primary = make_backend(acquire_result=False)
    secondary = make_backend(acquire_result=True)
    fb = FallbackBackend(primary, secondary)
    assert fb.acquire("job", 60, "owner1") is True
    secondary.acquire.assert_called_once_with("job", 60, "owner1")


def test_acquire_falls_back_when_primary_raises():
    primary = make_backend(raises=True)
    secondary = make_backend(acquire_result=True)
    fb = FallbackBackend(primary, secondary)
    assert fb.acquire("job", 60, "owner1") is True
    secondary.acquire.assert_called_once()


def test_acquire_returns_false_when_both_fail():
    primary = make_backend(acquire_result=False)
    secondary = make_backend(acquire_result=False)
    fb = FallbackBackend(primary, secondary)
    assert fb.acquire("job", 60, "owner1") is False


def test_release_uses_correct_backend_after_primary_acquire():
    primary = make_backend(acquire_result=True)
    secondary = make_backend()
    fb = FallbackBackend(primary, secondary)
    fb.acquire("job", 60, "owner1")
    fb.release("job", "owner1")
    primary.release.assert_called_once_with("job", "owner1")
    secondary.release.assert_not_called()


def test_release_uses_secondary_after_fallback_acquire():
    primary = make_backend(acquire_result=False)
    secondary = make_backend(acquire_result=True)
    fb = FallbackBackend(primary, secondary)
    fb.acquire("job", 60, "owner1")
    fb.release("job", "owner1")
    secondary.release.assert_called_once_with("job", "owner1")
    primary.release.assert_not_called()


def test_is_locked_checks_primary_first():
    primary = make_backend(is_locked_result=True)
    secondary = make_backend(is_locked_result=False)
    fb = FallbackBackend(primary, secondary)
    assert fb.is_locked("job") is True
    secondary.is_locked.assert_not_called()


def test_is_locked_falls_back_to_secondary_when_primary_raises():
    primary = make_backend()
    primary.is_locked.side_effect = RuntimeError("unavailable")
    secondary = make_backend(is_locked_result=True)
    fb = FallbackBackend(primary, secondary)
    assert fb.is_locked("job") is True


def test_refresh_uses_active_backend():
    primary = make_backend(acquire_result=False)
    secondary = make_backend(acquire_result=True)
    fb = FallbackBackend(primary, secondary)
    fb.acquire("job", 60, "owner1")
    fb.refresh("job", "owner1", 60)
    secondary.refresh.assert_called_once_with("job", "owner1", 60)
