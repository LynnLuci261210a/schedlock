"""Tests for schedlock.retry.retry_acquire."""

import pytest
from unittest.mock import MagicMock, call, patch

from schedlock.retry import retry_acquire


@pytest.fixture
def backend():
    return MagicMock()


def test_acquire_succeeds_on_first_attempt(backend):
    backend.acquire.return_value = True
    result = retry_acquire(backend, "my_job", ttl=60, retries=3, delay=0.0)
    assert result is True
    assert backend.acquire.call_count == 1


def test_acquire_succeeds_on_second_attempt(backend):
    backend.acquire.side_effect = [False, True]
    with patch("schedlock.retry.time.sleep") as mock_sleep:
        result = retry_acquire(backend, "my_job", ttl=60, retries=3, delay=0.5)
    assert result is True
    assert backend.acquire.call_count == 2
    mock_sleep.assert_called_once_with(0.5)


def test_acquire_fails_after_all_retries(backend):
    backend.acquire.return_value = False
    with patch("schedlock.retry.time.sleep"):
        result = retry_acquire(backend, "my_job", ttl=60, retries=3, delay=0.1)
    assert result is False
    assert backend.acquire.call_count == 3


def test_backoff_multiplier_applied(backend):
    backend.acquire.return_value = False
    with patch("schedlock.retry.time.sleep") as mock_sleep:
        retry_acquire(backend, "my_job", ttl=60, retries=4, delay=1.0, backoff=2.0)
    sleep_calls = [c.args[0] for c in mock_sleep.call_args_list]
    assert sleep_calls == [1.0, 2.0, 4.0]


def test_owner_forwarded_to_backend(backend):
    backend.acquire.return_value = True
    retry_acquire(backend, "my_job", ttl=60, owner="worker-1", retries=1)
    backend.acquire.assert_called_once_with("my_job", ttl=60, owner="worker-1")


def test_no_sleep_on_single_retry(backend):
    backend.acquire.return_value = False
    with patch("schedlock.retry.time.sleep") as mock_sleep:
        result = retry_acquire(backend, "my_job", ttl=60, retries=1, delay=1.0)
    assert result is False
    mock_sleep.assert_not_called()


def test_jitter_adds_to_sleep(backend):
    backend.acquire.side_effect = [False, True]
    with patch("schedlock.retry.time.sleep") as mock_sleep:
        with patch("schedlock.retry.random.uniform", return_value=0.3):
            retry_acquire(backend, "my_job", ttl=60, retries=3, delay=1.0, jitter=0.5)
    mock_sleep.assert_called_once_with(1.3)


def test_zero_jitter_does_not_add_random(backend):
    backend.acquire.side_effect = [False, True]
    with patch("schedlock.retry.time.sleep") as mock_sleep:
        with patch("schedlock.retry.random.uniform") as mock_rand:
            retry_acquire(backend, "my_job", ttl=60, retries=2, delay=0.5, jitter=0.0)
    mock_rand.assert_not_called()
    mock_sleep.assert_called_once_with(0.5)


def test_acquire_raises_exception_propagates(backend):
    """Exceptions from the backend should propagate immediately without retrying."""
    backend.acquire.side_effect = RuntimeError("connection lost")
    with patch("schedlock.retry.time.sleep") as mock_sleep:
        with pytest.raises(RuntimeError, match="connection lost"):
            retry_acquire(backend, "my_job", ttl=60, retries=3, delay=0.1)
    assert backend.acquire.call_count == 1
    mock_sleep.assert_not_called()
