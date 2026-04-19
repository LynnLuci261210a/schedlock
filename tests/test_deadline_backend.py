"""Tests for DeadlineBackend."""
from __future__ import annotations

import time
from unittest.mock import MagicMock

import pytest

from schedlock.backends.base import BaseBackend
from schedlock.backends.deadline_backend import DeadlineBackend


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
    return DeadlineBackend(inner, deadline=time.time() + 3600)


def test_requires_inner_backend():
    with pytest.raises(TypeError, match="inner must be a BaseBackend"):
        DeadlineBackend(object(), deadline=time.time() + 10)


def test_requires_numeric_deadline(inner):
    with pytest.raises(TypeError, match="deadline must be a numeric"):
        DeadlineBackend(inner, deadline="tomorrow")


def test_inner_property(inner, backend):
    assert backend.inner is inner


def test_deadline_property(inner):
    dl = time.time() + 100
    b = DeadlineBackend(inner, deadline=dl)
    assert b.deadline == pytest.approx(dl)


def test_acquire_succeeds_before_deadline(inner, backend):
    assert backend.acquire("job", "owner1", 60) is True
    inner.acquire.assert_called_once_with("job", "owner1", 60)


def test_acquire_blocked_after_deadline(inner):
    past = time.time() - 1
    b = DeadlineBackend(inner, deadline=past)
    assert b.acquire("job", "owner1", 60) is False
    inner.acquire.assert_not_called()


def test_release_delegates(inner, backend):
    assert backend.release("job", "owner1") is True
    inner.release.assert_called_once_with("job", "owner1")


def test_is_locked_delegates(inner, backend):
    backend.is_locked("job")
    inner.is_locked.assert_called_once_with("job")


def test_refresh_delegates(inner, backend):
    backend.refresh("job", "owner1", 30)
    inner.refresh.assert_called_once_with("job", "owner1", 30)


def test_acquire_at_exact_deadline_blocked(inner):
    """Deadline equal to now is treated as expired."""
    dl = time.time() - 0.001
    b = DeadlineBackend(inner, deadline=dl)
    assert b.acquire("job", "owner1", 60) is False
