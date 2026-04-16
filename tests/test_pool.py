"""Tests for BackendPool."""

import pytest
from unittest.mock import MagicMock

from schedlock.pool import BackendPool


def make_backend(acquire_ret=True, release_ret=True, is_locked_ret=False):
    b = MagicMock()
    b.acquire.return_value = acquire_ret
    b.release.return_value = release_ret
    b.is_locked.return_value = is_locked_ret
    return b


def test_pool_requires_at_least_one_backend():
    with pytest.raises(ValueError):
        BackendPool([])


def test_acquire_returns_true_when_first_backend_succeeds():
    b1 = make_backend(acquire_ret=True)
    b2 = make_backend(acquire_ret=True)
    pool = BackendPool([b1, b2])
    assert pool.acquire("job", 60, "owner") is True
    b1.acquire.assert_called_once_with("job", 60, "owner")
    b2.acquire.assert_not_called()


def test_acquire_falls_back_to_second_backend():
    b1 = make_backend(acquire_ret=False)
    b2 = make_backend(acquire_ret=True)
    pool = BackendPool([b1, b2])
    assert pool.acquire("job", 60, "owner") is True
    b2.acquire.assert_called_once()


def test_acquire_returns_false_when_all_fail():
    b1 = make_backend(acquire_ret=False)
    b2 = make_backend(acquire_ret=False)
    pool = BackendPool([b1, b2])
    assert pool.acquire("job", 60, "owner") is False


def test_acquire_skips_raising_backend():
    b1 = make_backend()
    b1.acquire.side_effect = RuntimeError("connection error")
    b2 = make_backend(acquire_ret=True)
    pool = BackendPool([b1, b2])
    assert pool.acquire("job", 60, "owner") is True


def test_release_returns_true_on_first_success():
    b1 = make_backend(release_ret=True)
    pool = BackendPool([b1])
    assert pool.release("job", "owner") is True


def test_release_falls_back_on_failure():
    b1 = make_backend(release_ret=False)
    b2 = make_backend(release_ret=True)
    pool = BackendPool([b1, b2])
    assert pool.release("job", "owner") is True


def test_release_returns_false_when_all_fail():
    b1 = make_backend(release_ret=False)
    pool = BackendPool([b1])
    assert pool.release("job", "owner") is False


def test_is_locked_returns_true_if_any_locked():
    b1 = make_backend(is_locked_ret=False)
    b2 = make_backend(is_locked_ret=True)
    pool = BackendPool([b1, b2])
    assert pool.is_locked("job") is True


def test_is_locked_returns_false_when_none_locked():
    b1 = make_backend(is_locked_ret=False)
    pool = BackendPool([b1])
    assert pool.is_locked("job") is False


def test_backends_property_returns_copy():
    b1 = make_backend()
    pool = BackendPool([b1])
    result = pool.backends
    result.clear()
    assert len(pool.backends) == 1
