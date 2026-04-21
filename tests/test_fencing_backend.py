"""Tests for FencingBackend."""
from unittest.mock import MagicMock

import pytest

from schedlock.backends.base import BaseBackend
from schedlock.backends.fencing_backend import FencingBackend
from schedlock.backends.memory_backend import MemoryBackend


@pytest.fixture()
def inner():
    return MagicMock(spec=BaseBackend)


@pytest.fixture()
def backend(inner):
    return FencingBackend(inner)


def test_requires_inner_backend():
    with pytest.raises(TypeError, match="BaseBackend"):
        FencingBackend("not-a-backend")  # type: ignore[arg-type]


def test_inner_property(backend, inner):
    assert backend.inner is inner


def test_initial_token_is_zero(backend):
    assert backend.token_for("my-job") == 0


def test_acquire_success_increments_token(backend, inner):
    inner.acquire.return_value = True
    backend.acquire("my-job", "worker-1")
    assert backend.token_for("my-job") == 1


def test_acquire_failure_does_not_increment_token(backend, inner):
    inner.acquire.return_value = False
    backend.acquire("my-job", "worker-1")
    assert backend.token_for("my-job") == 0


def test_token_increments_on_each_successful_acquire(backend, inner):
    inner.acquire.return_value = True
    backend.acquire("my-job", "worker-1")
    backend.acquire("my-job", "worker-1")
    backend.acquire("my-job", "worker-1")
    assert backend.token_for("my-job") == 3


def test_tokens_are_independent_per_key(backend, inner):
    inner.acquire.return_value = True
    backend.acquire("job-a", "worker-1")
    backend.acquire("job-a", "worker-1")
    backend.acquire("job-b", "worker-1")
    assert backend.token_for("job-a") == 2
    assert backend.token_for("job-b") == 1


def test_mixed_success_and_failure(backend, inner):
    inner.acquire.side_effect = [True, False, True]
    backend.acquire("my-job", "worker-1")
    backend.acquire("my-job", "worker-2")
    backend.acquire("my-job", "worker-1")
    assert backend.token_for("my-job") == 2


def test_release_delegates_to_inner(backend, inner):
    inner.release.return_value = True
    result = backend.release("my-job", "worker-1")
    inner.release.assert_called_once_with("my-job", "worker-1")
    assert result is True


def test_is_locked_delegates_to_inner(backend, inner):
    inner.is_locked.return_value = True
    assert backend.is_locked("my-job") is True
    inner.is_locked.assert_called_once_with("my-job")


def test_refresh_delegates_to_inner(backend, inner):
    inner.refresh.return_value = True
    result = backend.refresh("my-job", "worker-1", ttl=60)
    inner.refresh.assert_called_once_with("my-job", "worker-1", 60)
    assert result is True


def test_full_lifecycle_over_memory():
    mem = MemoryBackend()
    fb = FencingBackend(mem)

    assert fb.token_for("job") == 0
    assert fb.acquire("job", "w1", ttl=30) is True
    assert fb.token_for("job") == 1

    assert fb.acquire("job", "w2", ttl=30) is False
    assert fb.token_for("job") == 1  # no change — blocked

    fb.release("job", "w1")
    assert fb.acquire("job", "w2", ttl=30) is True
    assert fb.token_for("job") == 2
