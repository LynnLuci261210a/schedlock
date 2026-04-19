import pytest
from unittest.mock import MagicMock
from schedlock.backends.base import BaseBackend
from schedlock.backends.checkpoint_backend import CheckpointBackend


@pytest.fixture
def inner():
    m = MagicMock(spec=BaseBackend)
    return m


@pytest.fixture
def backend(inner):
    return CheckpointBackend(inner)


def test_requires_inner_backend():
    with pytest.raises(TypeError):
        CheckpointBackend("not-a-backend")


def test_inner_property(backend, inner):
    assert backend.inner is inner


def test_acquire_success_records_checkpoint(backend, inner):
    inner.acquire.return_value = True
    result = backend.acquire("job", "worker-1", 60)
    assert result is True
    cp = backend.checkpoint_for("job")
    assert cp is not None
    assert cp["event"] == "acquired"
    assert cp["owner"] == "worker-1"
    assert cp["ttl"] == 60


def test_acquire_failure_does_not_record(backend, inner):
    inner.acquire.return_value = False
    backend.acquire("job", "worker-1", 60)
    assert backend.checkpoint_for("job") is None


def test_release_success_records_checkpoint(backend, inner):
    inner.acquire.return_value = True
    inner.release.return_value = True
    backend.acquire("job", "worker-1", 60)
    result = backend.release("job", "worker-1")
    assert result is True
    cp = backend.checkpoint_for("job")
    assert cp["event"] == "released"
    assert cp["owner"] == "worker-1"


def test_release_failure_does_not_overwrite(backend, inner):
    inner.acquire.return_value = True
    inner.release.return_value = False
    backend.acquire("job", "worker-1", 60)
    backend.release("job", "worker-1")
    cp = backend.checkpoint_for("job")
    assert cp["event"] == "acquired"


def test_clear_checkpoint(backend, inner):
    inner.acquire.return_value = True
    backend.acquire("job", "worker-1", 60)
    backend.clear_checkpoint("job")
    assert backend.checkpoint_for("job") is None


def test_clear_nonexistent_checkpoint_is_safe(backend):
    backend.clear_checkpoint("no-such-key")


def test_is_locked_delegates(backend, inner):
    inner.is_locked.return_value = True
    assert backend.is_locked("job") is True
    inner.is_locked.assert_called_once_with("job")


def test_refresh_delegates(backend, inner):
    inner.refresh.return_value = True
    assert backend.refresh("job", "worker-1", 30) is True
    inner.refresh.assert_called_once_with("job", "worker-1", 30)


def test_multiple_keys_independent(backend, inner):
    inner.acquire.return_value = True
    backend.acquire("job-a", "w1", 10)
    backend.acquire("job-b", "w2", 20)
    assert backend.checkpoint_for("job-a")["owner"] == "w1"
    assert backend.checkpoint_for("job-b")["owner"] == "w2"
