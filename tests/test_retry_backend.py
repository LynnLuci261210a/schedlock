import pytest
from unittest.mock import MagicMock
from schedlock.backends.retry_backend import RetryBackend
from schedlock.backends.memory_backend import MemoryBackend


@pytest.fixture
def inner():
    return MagicMock()


@pytest.fixture
def backend(inner):
    return RetryBackend(inner, retries=3, delay=0.0, backoff=1.0)


def test_requires_inner_backend():
    with pytest.raises(ValueError, match="inner backend is required"):
        RetryBackend(None)


def test_requires_positive_retries(inner):
    with pytest.raises(ValueError, match="retries must be at least 1"):
        RetryBackend(inner, retries=0)


def test_requires_non_negative_delay(inner):
    with pytest.raises(ValueError, match="delay must be non-negative"):
        RetryBackend(inner, delay=-1)


def test_requires_backoff_gte_one(inner):
    with pytest.raises(ValueError, match="backoff must be >= 1.0"):
        RetryBackend(inner, backoff=0.5)


def test_inner_property(inner, backend):
    assert backend.inner is inner


def test_acquire_succeeds_on_first_try(inner, backend):
    inner.acquire.return_value = True
    result = backend.acquire("job", "owner-1", 60)
    assert result is True
    inner.acquire.assert_called_once_with("job", "owner-1", 60)


def test_acquire_retries_and_succeeds(inner, backend):
    inner.acquire.side_effect = [False, False, True]
    result = backend.acquire("job", "owner-1", 60)
    assert result is True
    assert inner.acquire.call_count == 3


def test_acquire_fails_after_all_retries(inner, backend):
    inner.acquire.return_value = False
    result = backend.acquire("job", "owner-1", 60)
    assert result is False
    assert inner.acquire.call_count == 3


def test_release_delegates(inner, backend):
    inner.release.return_value = True
    result = backend.release("job", "owner-1")
    assert result is True
    inner.release.assert_called_once_with("job", "owner-1")


def test_is_locked_delegates(inner, backend):
    inner.is_locked.return_value = True
    assert backend.is_locked("job") is True
    inner.is_locked.assert_called_once_with("job")


def test_refresh_delegates(inner, backend):
    inner.refresh.return_value = True
    result = backend.refresh("job", "owner-1", 30)
    assert result is True
    inner.refresh.assert_called_once_with("job", "owner-1", 30)


def test_integration_with_memory_backend():
    mem = MemoryBackend()
    b = RetryBackend(mem, retries=3, delay=0.0, backoff=1.0)
    assert b.acquire("cron.task", "worker-1", 60) is True
    assert b.is_locked("cron.task") is True
    assert b.acquire("cron.task", "worker-2", 60) is False
    assert b.release("cron.task", "worker-1") is True
    assert b.is_locked("cron.task") is False
