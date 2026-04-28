"""Tests for StickyWindowBackend."""

import pytest
from unittest.mock import MagicMock

from schedlock.backends.base import BaseBackend
from schedlock.backends.sticky_window_backend import StickyWindowBackend


@pytest.fixture
def inner():
    m = MagicMock(spec=BaseBackend)
    m.acquire.return_value = True
    m.release.return_value = True
    m.is_locked.return_value = False
    m.refresh.return_value = True
    return m


@pytest.fixture
def backend(inner):
    return StickyWindowBackend(inner, schedule_fn=lambda: True)


def test_requires_inner_backend():
    with pytest.raises(TypeError, match="inner must be a BaseBackend"):
        StickyWindowBackend(object(), schedule_fn=lambda: True)


def test_requires_callable_schedule_fn(inner):
    with pytest.raises(TypeError, match="schedule_fn must be callable"):
        StickyWindowBackend(inner, schedule_fn="not-callable")


def test_requires_non_empty_reason(inner):
    with pytest.raises(ValueError, match="reason must be a non-empty string"):
        StickyWindowBackend(inner, schedule_fn=lambda: True, reason="")


def test_inner_property(inner, backend):
    assert backend.inner is inner


def test_reason_property(inner):
    b = StickyWindowBackend(inner, schedule_fn=lambda: True, reason="closed")
    assert b.reason == "closed"


def test_acquire_succeeds_when_window_open(inner, backend):
    assert backend.acquire("job", "worker-1", 30) is True
    inner.acquire.assert_called_once_with("job", "worker-1", 30)


def test_acquire_blocked_when_window_closed(inner):
    b = StickyWindowBackend(inner, schedule_fn=lambda: False)
    assert b.acquire("job", "worker-1", 30) is False
    inner.acquire.assert_not_called()


def test_sticky_owner_can_reacquire_outside_window(inner):
    open_flag = [True]
    b = StickyWindowBackend(inner, schedule_fn=lambda: open_flag[0])
    # First acquire inside window
    b.acquire("job", "worker-1", 30)
    # Close the window
    open_flag[0] = False
    # Sticky owner should still be allowed
    assert b.acquire("job", "worker-1", 30) is True


def test_other_owner_blocked_while_sticky_owner_holds(inner):
    b = StickyWindowBackend(inner, schedule_fn=lambda: True)
    b.acquire("job", "worker-1", 30)
    # Different owner should be blocked
    result = b.acquire("job", "worker-2", 30)
    assert result is False


def test_release_clears_sticky_owner(inner, backend):
    backend.acquire("job", "worker-1", 30)
    backend.release("job", "worker-1")
    inner.release.assert_called_once_with("job", "worker-1")
    # After release, a new owner can acquire (window is open)
    backend.acquire("job", "worker-2", 30)
    assert inner.acquire.call_count == 2


def test_release_does_not_clear_sticky_on_failure(inner, backend):
    inner.release.return_value = False
    backend.acquire("job", "worker-1", 30)
    backend.release("job", "worker-1")
    # Sticky should still be set; worker-2 blocked
    assert backend.acquire("job", "worker-2", 30) is False


def test_is_locked_delegates(inner, backend):
    inner.is_locked.return_value = True
    assert backend.is_locked("job") is True
    inner.is_locked.assert_called_once_with("job")


def test_refresh_delegates(inner, backend):
    assert backend.refresh("job", "worker-1", 60) is True
    inner.refresh.assert_called_once_with("job", "worker-1", 60)


def test_independent_keys_tracked_separately(inner):
    b = StickyWindowBackend(inner, schedule_fn=lambda: True)
    b.acquire("job-a", "worker-1", 30)
    # job-b has no sticky owner; should be acquirable by anyone
    assert b.acquire("job-b", "worker-2", 30) is True
