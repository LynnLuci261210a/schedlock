import pytest
from unittest.mock import MagicMock
from schedlock.backends.watermark_backend import WatermarkBackend
from schedlock.backends.memory_backend import MemoryBackend
from schedlock.backends.base import BaseBackend


@pytest.fixture
def inner():
    return MagicMock(spec=BaseBackend)


@pytest.fixture
def backend(inner):
    return WatermarkBackend(inner)


def test_requires_inner_backend():
    with pytest.raises(TypeError):
        WatermarkBackend("not-a-backend")


def test_inner_property(backend, inner):
    assert backend.inner is inner


def test_acquire_delegates_to_inner(backend, inner):
    inner.acquire.return_value = True
    result = backend.acquire("job", "owner-1", 30)
    assert result is True
    inner.acquire.assert_called_once_with("job", "owner-1", 30)


def test_peak_starts_at_zero(backend):
    assert backend.peak_for("job") == 0


def test_current_starts_at_zero(backend):
    assert backend.current_for("job") == 0


def test_acquire_success_increments_current(backend, inner):
    inner.acquire.return_value = True
    backend.acquire("job", "owner-1", 60)
    assert backend.current_for("job") == 1


def test_acquire_failure_does_not_increment(backend, inner):
    inner.acquire.return_value = False
    backend.acquire("job", "owner-1", 60)
    assert backend.current_for("job") == 0
    assert backend.peak_for("job") == 0


def test_peak_tracks_maximum(backend, inner):
    inner.acquire.return_value = True
    backend.acquire("job", "owner-1", 60)
    backend.acquire("job", "owner-2", 60)
    assert backend.peak_for("job") == 2


def test_release_decrements_current(backend, inner):
    inner.acquire.return_value = True
    inner.release.return_value = True
    backend.acquire("job", "owner-1", 60)
    backend.release("job", "owner-1")
    assert backend.current_for("job") == 0


def test_peak_not_reduced_after_release(backend, inner):
    inner.acquire.return_value = True
    inner.release.return_value = True
    backend.acquire("job", "owner-1", 60)
    backend.acquire("job", "owner-2", 60)
    backend.release("job", "owner-1")
    assert backend.peak_for("job") == 2
    assert backend.current_for("job") == 1


def test_reset_peak(backend, inner):
    inner.acquire.return_value = True
    inner.release.return_value = True
    backend.acquire("job", "owner-1", 60)
    backend.acquire("job", "owner-2", 60)
    backend.release("job", "owner-1")
    backend.reset_peak("job")
    assert backend.peak_for("job") == 1


def test_is_locked_delegates(backend, inner):
    inner.is_locked.return_value = True
    assert backend.is_locked("job") is True


def test_refresh_delegates(backend, inner):
    inner.refresh.return_value = True
    assert backend.refresh("job", "owner-1", 60) is True
