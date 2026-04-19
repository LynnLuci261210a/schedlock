"""Tests for WindowBackend."""

from __future__ import annotations

import datetime
from unittest.mock import MagicMock, patch

import pytest

from schedlock.backends.window_backend import WindowBackend
from schedlock.backends.memory_backend import MemoryBackend


@pytest.fixture
def inner():
    return MagicMock()


@pytest.fixture
def always_open_windows():
    return [(datetime.time(0, 0), datetime.time(23, 59))]


@pytest.fixture
def backend(inner, always_open_windows):
    return WindowBackend(inner, always_open_windows)


def test_requires_inner_backend(always_open_windows):
    with pytest.raises(ValueError, match="inner"):
        WindowBackend(None, always_open_windows)


def test_requires_at_least_one_window(inner):
    with pytest.raises(ValueError, match="window"):
        WindowBackend(inner, [])


def test_window_start_must_be_before_end(inner):
    with pytest.raises(ValueError, match="before end"):
        WindowBackend(inner, [(datetime.time(12, 0), datetime.time(8, 0))])


def test_window_bounds_must_be_time_instances(inner):
    with pytest.raises(TypeError):
        WindowBackend(inner, [("08:00", "12:00")])


def test_inner_property(backend, inner):
    assert backend.inner is inner


def test_windows_property(inner, always_open_windows):
    b = WindowBackend(inner, always_open_windows)
    assert b.windows == always_open_windows


def test_acquire_delegates_when_in_window(backend, inner):
    inner.acquire.return_value = True
    result = backend.acquire("job", "owner1", 60)
    assert result is True
    inner.acquire.assert_called_once_with("job", "owner1", 60)


def test_acquire_blocked_outside_window(inner):
    window = [(datetime.time(9, 0), datetime.time(10, 0))]
    b = WindowBackend(inner, window)
    outside = datetime.time(11, 0)
    with patch("schedlock.backends.window_backend.datetime") as mock_dt:
        mock_dt.datetime.utcnow.return_value = datetime.datetime(2024, 1, 1, 11, 0)
        mock_dt.time = datetime.time
        result = b.acquire("job", "owner1", 60)
    assert result is False
    inner.acquire.assert_not_called()


def test_acquire_allowed_inside_window(inner):
    window = [(datetime.time(9, 0), datetime.time(17, 0))]
    b = WindowBackend(inner, window)
    inner.acquire.return_value = True
    with patch("schedlock.backends.window_backend.datetime") as mock_dt:
        mock_dt.datetime.utcnow.return_value = datetime.datetime(2024, 1, 1, 12, 0)
        mock_dt.time = datetime.time
        result = b.acquire("job", "owner1", 60)
    assert result is True


def test_release_always_delegates(backend, inner):
    inner.release.return_value = True
    assert backend.release("job", "owner1") is True
    inner.release.assert_called_once_with("job", "owner1")


def test_is_locked_delegates(backend, inner):
    inner.is_locked.return_value = False
    assert backend.is_locked("job") is False


def test_refresh_delegates(backend, inner):
    inner.refresh.return_value = True
    assert backend.refresh("job", "owner1", 30) is True
