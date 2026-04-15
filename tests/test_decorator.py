import pytest
from unittest.mock import MagicMock, patch

from schedlock.decorator import schedlock


@pytest.fixture
def mock_backend():
    backend = MagicMock()
    backend.acquire.return_value = True
    backend.release.return_value = True
    return backend


def test_decorator_runs_function_when_lock_acquired(mock_backend):
    result_holder = []

    @schedlock("test_job", backend=mock_backend, ttl=30, owner="test-owner")
    def my_job():
        result_holder.append("ran")

    my_job()
    assert result_holder == ["ran"]
    mock_backend.acquire.assert_called_once_with("test_job", ttl=30, owner="test-owner")
    mock_backend.release.assert_called_once_with("test_job", owner="test-owner")


def test_decorator_skips_when_lock_not_acquired_and_skip_true(mock_backend):
    mock_backend.acquire.return_value = False
    result_holder = []

    @schedlock("test_job", backend=mock_backend, skip_if_locked=True)
    def my_job():
        result_holder.append("ran")

    result = my_job()
    assert result is None
    assert result_holder == []
    mock_backend.release.assert_not_called()


def test_decorator_raises_when_lock_not_acquired_and_skip_false(mock_backend):
    mock_backend.acquire.return_value = False

    @schedlock("test_job", backend=mock_backend, skip_if_locked=False)
    def my_job():
        pass

    with pytest.raises(RuntimeError, match="Could not acquire lock for job 'test_job'"):
        my_job()


def test_decorator_releases_lock_even_on_exception(mock_backend):
    @schedlock("test_job", backend=mock_backend, owner="owner-x")
    def my_job():
        raise ValueError("boom")

    with pytest.raises(ValueError, match="boom"):
        my_job()

    mock_backend.release.assert_called_once_with("test_job", owner="owner-x")


def test_decorator_returns_function_result(mock_backend):
    @schedlock("test_job", backend=mock_backend, owner="owner-y")
    def my_job():
        return 42

    assert my_job() == 42


def test_decorator_uses_default_owner_when_none_provided(mock_backend):
    @schedlock("test_job", backend=mock_backend)
    def my_job():
        pass

    my_job()
    call_kwargs = mock_backend.acquire.call_args
    owner_used = call_kwargs[1]["owner"]
    assert owner_used  # not empty
    import socket
    assert socket.gethostname() in owner_used
