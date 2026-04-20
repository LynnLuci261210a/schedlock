"""Tests for WarmupBackend."""

from __future__ import annotations

import time
from unittest.mock import MagicMock, patch

import pytest

from schedlock.backends.base import BaseBackend
from schedlock.backends.warmup_backend import WarmupBackend


@pytest.fixture()
def inner() -> MagicMock:
    m = MagicMock(spec=BaseBackend)
    m.acquire.return_value = True
    m.release.return_value = True
    m.is_locked.return_value = False
    m.refresh.return_value = True
    return m


@pytest.fixture()
def backend(inner: MagicMock) -> WarmupBackend:
    return WarmupBackend(inner, warmup_seconds=5.0)


def test_requires_inner_backend() -> None:
    with pytest.raises(TypeError, match="inner must be a BaseBackend"):
        WarmupBackend(object(), warmup_seconds=1.0)  # type: ignore[arg-type]


def test_requires_positive_warmup_seconds(inner: MagicMock) -> None:
    with pytest.raises(ValueError, match="warmup_seconds must be a positive number"):
        WarmupBackend(inner, warmup_seconds=0)


def test_requires_non_negative_warmup_seconds(inner: MagicMock) -> None:
    with pytest.raises(ValueError, match="warmup_seconds must be a positive number"):
        WarmupBackend(inner, warmup_seconds=-1)


def test_inner_property(inner: MagicMock, backend: WarmupBackend) -> None:
    assert backend.inner is inner


def test_warmup_seconds_property(inner: MagicMock) -> None:
    b = WarmupBackend(inner, warmup_seconds=3.5)
    assert b.warmup_seconds == 3.5


def test_acquire_blocked_during_warmup(inner: MagicMock, backend: WarmupBackend) -> None:
    # monotonic time hasn't advanced past 5 s yet
    result = backend.acquire("job", "owner-1", 60)
    assert result is False
    inner.acquire.assert_not_called()


def test_acquire_allowed_after_warmup(inner: MagicMock) -> None:
    b = WarmupBackend(inner, warmup_seconds=1.0)
    # Simulate that warmup has already elapsed
    with patch("schedlock.backends.warmup_backend.time.monotonic", return_value=b._started_at + 2.0):
        result = b.acquire("job", "owner-1", 60)
    assert result is True
    inner.acquire.assert_called_once_with("job", "owner-1", 60)


def test_acquire_at_exact_warmup_boundary(inner: MagicMock) -> None:
    b = WarmupBackend(inner, warmup_seconds=2.0)
    with patch("schedlock.backends.warmup_backend.time.monotonic", return_value=b._started_at + 2.0):
        result = b.acquire("job", "owner-1", 60)
    assert result is True


def test_release_delegates_to_inner(inner: MagicMock, backend: WarmupBackend) -> None:
    result = backend.release("job", "owner-1")
    assert result is True
    inner.release.assert_called_once_with("job", "owner-1")


def test_is_locked_delegates_to_inner(inner: MagicMock, backend: WarmupBackend) -> None:
    inner.is_locked.return_value = True
    assert backend.is_locked("job") is True
    inner.is_locked.assert_called_once_with("job")


def test_refresh_delegates_to_inner(inner: MagicMock, backend: WarmupBackend) -> None:
    result = backend.refresh("job", "owner-1", 30)
    assert result is True
    inner.refresh.assert_called_once_with("job", "owner-1", 30)
