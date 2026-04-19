"""Integration tests for WindowBackend over MemoryBackend."""

from __future__ import annotations

import datetime
from unittest.mock import patch

import pytest

from schedlock.backends.memory_backend import MemoryBackend
from schedlock.backends.window_backend import WindowBackend
from schedlock.backends.namespaced_backend import NamespacedBackend


@pytest.fixture
def memory():
    return MemoryBackend()


def _at(hour: int, minute: int = 0):
    return datetime.datetime(2024, 6, 1, hour, minute, 0)


def test_acquire_within_window(memory):
    b = WindowBackend(memory, [(datetime.time(8, 0), datetime.time(18, 0))])
    with patch("schedlock.backends.window_backend.datetime") as m:
        m.datetime.utcnow.return_value = _at(12)
        m.time = datetime.time
        assert b.acquire("job", "worker1", 60) is True


def test_acquire_outside_window_blocked(memory):
    b = WindowBackend(memory, [(datetime.time(8, 0), datetime.time(18, 0))])
    with patch("schedlock.backends.window_backend.datetime") as m:
        m.datetime.utcnow.return_value = _at(22)
        m.time = datetime.time
        assert b.acquire("job", "worker1", 60) is False


def test_release_after_acquire(memory):
    b = WindowBackend(memory, [(datetime.time(0, 0), datetime.time(23, 59))])
    b.acquire("job", "worker1", 60)
    assert b.release("job", "worker1") is True
    assert b.is_locked("job") is False


def test_multiple_windows(memory):
    windows = [
        (datetime.time(6, 0), datetime.time(9, 0)),
        (datetime.time(17, 0), datetime.time(20, 0)),
    ]
    b = WindowBackend(memory, windows)
    for hour in (7, 18):
        with patch("schedlock.backends.window_backend.datetime") as m:
            m.datetime.utcnow.return_value = _at(hour)
            m.time = datetime.time
            assert b.acquire(f"job{hour}", "worker", 60) is True


def test_window_over_namespaced(memory):
    ns = NamespacedBackend(memory, "cron")
    b = WindowBackend(ns, [(datetime.time(0, 0), datetime.time(23, 59))])
    assert b.acquire("task", "w1", 30) is True
    assert b.is_locked("task") is True
