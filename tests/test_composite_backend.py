"""Tests for CompositeBackend."""

import pytest
from unittest.mock import MagicMock

from schedlock.backends.composite import CompositeBackend


def make_backend(acquire_ret=True, release_ret=True, is_locked_ret=False, refresh_ret=True):
    b = MagicMock()
    b.acquire.return_value = acquire_ret
    b.release.return_value = release_ret
    b.is_locked.return_value = is_locked_ret
    b.refresh.return_value = refresh_ret
    return b


def test_requires_at_least_one_backend():
    with pytest.raises(ValueError):
        CompositeBackend([])


def test_acquire_succeeds_when_all_succeed():
    b1, b2 = make_backend(), make_backend()
    comp = CompositeBackend([b1, b2])
    assert comp.acquire("job", 60, "owner") is True
    b1.acquire.assert_called_once_with("job", 60, "owner")
    b2.acquire.assert_called_once_with("job", 60, "owner")


def test_acquire_fails_and_rolls_back_when_second_fails():
    b1 = make_backend(acquire_ret=True)
    b2 = make_backend(acquire_ret=False)
    comp = CompositeBackend([b1, b2])
    result = comp.acquire("job", 60, "owner")
    assert result is False
    b1.release.assert_called_once_with("job", "owner")


def test_acquire_fails_immediately_on_first_failure():
    b1 = make_backend(acquire_ret=False)
    b2 = make_backend(acquire_ret=True)
    comp = CompositeBackend([b1, b2])
    assert comp.acquire("job", 60, "owner") is False
    b2.acquire.assert_not_called()


def test_release_returns_true_if_any_succeed():
    b1 = make_backend(release_ret=False)
    b2 = make_backend(release_ret=True)
    comp = CompositeBackend([b1, b2])
    assert comp.release("job", "owner") is True


def test_release_returns_false_when_all_fail():
    b1 = make_backend(release_ret=False)
    b2 = make_backend(release_ret=False)
    comp = CompositeBackend([b1, b2])
    assert comp.release("job", "owner") is False


def test_release_continues_despite_exception():
    b1 = make_backend()
    b1.release.side_effect = RuntimeError("fail")
    b2 = make_backend(release_ret=True)
    comp = CompositeBackend([b1, b2])
    assert comp.release("job", "owner") is True


def test_is_locked_delegates_to_primary():
    b1 = make_backend(is_locked_ret=True)
    b2 = make_backend(is_locked_ret=False)
    comp = CompositeBackend([b1, b2])
    assert comp.is_locked("job") is True
    b2.is_locked.assert_not_called()


def test_refresh_returns_true_when_all_succeed():
    b1, b2 = make_backend(), make_backend()
    comp = CompositeBackend([b1, b2])
    assert comp.refresh("job", "owner", 60) is True


def test_refresh_returns_false_when_any_fails():
    b1 = make_backend(refresh_ret=True)
    b2 = make_backend(refresh_ret=False)
    comp = CompositeBackend([b1, b2])
    assert comp.refresh("job", "owner", 60) is False


def test_refresh_calls_all_backends():
    """Ensure refresh is attempted on all backends even if one returns False."""
    b1 = make_backend(refresh_ret=False)
    b2 = make_backend(refresh_ret=True)
    comp = CompositeBackend([b1, b2])
    comp.refresh("job", "owner", 60)
    b1.refresh.assert_called_once_with("job", "owner", 60)
    b2.refresh.assert_called_once_with("job", "owner", 60)
