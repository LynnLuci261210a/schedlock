"""Tests for ScheduledBackend."""

from __future__ import annotations

import datetime
from unittest.mock import MagicMock

import pytest

from schedlock.backends.scheduled_backend import ScheduledBackend
from schedlock.backends.base import BaseBackend


@pytest.fixture()
def inner():
    m = MagicMock(spec=BaseBackend)
    m.acquire.return_value = True
    m.release.return_value = True
    m.is_locked.return_value = False
    m.refresh.return_value = True
    return m


@pytest.fixture()
def backend(inner):
    return ScheduledBackend(inner, schedule_fn=lambda dt: True)


def test_requires_inner_backend():
    with pytest.raises(TypeError, match="inner must be a BaseBackend"):
        ScheduledBackend(object(), schedule_fn=lambda dt: True)


def test_requires_callable_schedule_fn(inner):
    with pytest.raises(TypeError, match="schedule_fn must be callable"):
        ScheduledBackend(inner, schedule_fn="not-callable")


def test_requires_non_empty_reason(inner):
    with pytest.raises(ValueError, match="reason must be a non-empty string"):
        ScheduledBackend(inner, schedule_fn=lambda dt: True, reason="   ")


def test_inner_property(inner, backend):
    assert backend.inner is inner


def test_reason_default(inner):
    b = ScheduledBackend(inner, schedule_fn=lambda dt: True)
    assert b.reason == "outside scheduled window"


def test_reason_custom(inner):
    b = ScheduledBackend(inner, schedule_fn=lambda dt: True, reason="off-hours")
    assert b.reason == "off-hours"


def test_acquire_delegates_when_schedule_open(inner, backend):
    result = backend.acquire("job", "worker-1", 60)
    assert result is True
    inner.acquire.assert_called_once_with("job", "worker-1", 60)


def test_acquire_blocked_when_schedule_closed(inner):
    b = ScheduledBackend(inner, schedule_fn=lambda dt: False)
    result = b.acquire("job", "worker-1", 60)
    assert result is False
    inner.acquire.assert_not_called()


def test_acquire_passes_current_utc_datetime_to_schedule_fn(inner):
    received: list[datetime.datetime] = []

    def capture(dt: datetime.datetime) -> bool:
        received.append(dt)
        return True

    b = ScheduledBackend(inner, schedule_fn=capture)
    b.acquire("job", "worker-1", 30)
    assert len(received) == 1
    assert isinstance(received[0], datetime.datetime)


def test_release_always_delegates_regardless_of_schedule(inner):
    b = ScheduledBackend(inner, schedule_fn=lambda dt: False)
    result = b.release("job", "worker-1")
    assert result is True
    inner.release.assert_called_once_with("job", "worker-1")


def test_is_locked_delegates(inner, backend):
    inner.is_locked.return_value = True
    assert backend.is_locked("job") is True
    inner.is_locked.assert_called_once_with("job")


def test_refresh_delegates(inner, backend):
    result = backend.refresh("job", "worker-1", 120)
    assert result is True
    inner.refresh.assert_called_once_with("job", "worker-1", 120)


def test_acquire_blocked_returns_false_not_none(inner):
    b = ScheduledBackend(inner, schedule_fn=lambda dt: False)
    assert b.acquire("job", "w", 10) is False
