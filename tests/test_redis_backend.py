import time
import pytest
from unittest.mock import MagicMock, patch
from schedlock.backends.redis_backend import RedisBackend


@pytest.fixture
def mock_redis():
    """Provide a mock Redis client for unit tests."""
    client = MagicMock()
    # register_script returns a callable mock by default
    client.register_script.return_value = MagicMock(return_value=1)
    return client


@pytest.fixture
def backend(mock_redis):
    return RedisBackend(client=mock_redis)


def test_acquire_lock_success(backend, mock_redis):
    mock_redis.set.return_value = True
    token = backend.acquire("daily_report", ttl=60)
    assert token is not None
    mock_redis.set.assert_called_once()
    call_kwargs = mock_redis.set.call_args
    assert call_kwargs.kwargs["nx"] is True
    assert call_kwargs.kwargs["ex"] == 60


def test_acquire_lock_blocked(backend, mock_redis):
    mock_redis.set.return_value = None
    token = backend.acquire("daily_report", ttl=60)
    assert token is None


def test_acquire_uses_provided_owner(backend, mock_redis):
    mock_redis.set.return_value = True
    token = backend.acquire("daily_report", ttl=30, owner="my-owner-id")
    assert token == "my-owner-id"
    args, kwargs = mock_redis.set.call_args
    assert args[1] == "my-owner-id"


def test_release_lock_by_owner(backend, mock_redis):
    release_script = backend._release_script
    release_script.return_value = 1
    result = backend.release("daily_report", owner="token-abc")
    assert result is True
    release_script.assert_called_once_with(
        keys=["schedlock:daily_report"], args=["token-abc"]
    )


def test_release_lock_wrong_owner(backend, mock_redis):
    release_script = backend._release_script
    release_script.return_value = 0
    result = backend.release("daily_report", owner="wrong-token")
    assert result is False


def test_is_locked_true(backend, mock_redis):
    mock_redis.exists.return_value = 1
    assert backend.is_locked("daily_report") is True


def test_is_locked_false(backend, mock_redis):
    mock_redis.exists.return_value = 0
    assert backend.is_locked("daily_report") is False


def test_ttl_returns_remaining_seconds(backend, mock_redis):
    mock_redis.ttl.return_value = 42
    assert backend.ttl("daily_report") == 42
    mock_redis.ttl.assert_called_once_with("schedlock:daily_report")


def test_key_prefix_applied(backend, mock_redis):
    mock_redis.set.return_value = True
    backend.acquire("cleanup_job", ttl=10)
    args, _ = mock_redis.set.call_args
    assert args[0] == "schedlock:cleanup_job"


def test_import_error_without_redis_and_client():
    with patch("schedlock.backends.redis_backend.redis", None):
        with pytest.raises(ImportError, match="redis-py is required"):
            RedisBackend()
