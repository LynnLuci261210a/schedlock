"""Tests for LockContext context manager."""

import pytest
from unittest.mock import MagicMock

from schedlock.context import LockContext


@pytest.fixture
def mock_backend():
    backend = MagicMock()
    return backend


def test_lock_acquired_enters_and_releases(mock_backend):
    mock_backend.acquire.return_value = True

    with LockContext(mock_backend, "my-job", ttl=60, owner="test-owner") as acquired:
        assert acquired is True

    mock_backend.acquire.assert_called_once_with("my-job", ttl=60, owner="test-owner")
    mock_backend.release.assert_called_once_with("my-job", owner="test-owner")


def test_lock_not_acquired_skips_release(mock_backend):
    mock_backend.acquire.return_value = False

    with LockContext(mock_backend, "my-job", ttl=60, owner="test-owner") as acquired:
        assert acquired is False

    mock_backend.release.assert_not_called()


def test_release_called_even_on_exception(mock_backend):
    mock_backend.acquire.return_value = True

    with pytest.raises(ValueError):
        with LockContext(mock_backend, "my-job", owner="test-owner"):
            raise ValueError("boom")

    mock_backend.release.assert_called_once_with("my-job", owner="test-owner")


def test_acquired_property_reflects_state(mock_backend):
    mock_backend.acquire.return_value = True
    ctx = LockContext(mock_backend, "my-job", owner="owner-1")
    assert ctx.acquired is False  # before entering

    with ctx:
        assert ctx.acquired is True


def test_default_owner_is_set_when_none(mock_backend):
    mock_backend.acquire.return_value = True
    ctx = LockContext(mock_backend, "my-job")
    assert ctx.owner is not None
    assert isinstance(ctx.owner, str)
    assert len(ctx.owner) > 0


def test_repr_contains_job_name_and_owner(mock_backend):
    mock_backend.acquire.return_value = False
    ctx = LockContext(mock_backend, "report-job", owner="worker-1")
    r = repr(ctx)
    assert "report-job" in r
    assert "worker-1" in r


def test_exception_not_suppressed(mock_backend):
    mock_backend.acquire.return_value = True
    with pytest.raises(RuntimeError, match="expected error"):
        with LockContext(mock_backend, "job", owner="o"):
            raise RuntimeError("expected error")
