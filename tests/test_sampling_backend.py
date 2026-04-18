import pytest
from unittest.mock import MagicMock
from schedlock.backends.sampling_backend import SamplingBackend
from schedlock.backends.base import BaseBackend


@pytest.fixture
def inner():
    m = MagicMock(spec=BaseBackend)
    m.acquire.return_value = True
    m.release.return_value = True
    m.is_locked.return_value = False
    m.refresh.return_value = True
    return m


@pytest.fixture
def backend(inner):
    return SamplingBackend(inner, sample_rate=1.0)


def test_requires_inner_backend():
    with pytest.raises(TypeError):
        SamplingBackend("not-a-backend")


def test_requires_valid_sample_rate(inner):
    with pytest.raises(ValueError):
        SamplingBackend(inner, sample_rate=0.0)
    with pytest.raises(ValueError):
        SamplingBackend(inner, sample_rate=1.5)
    with pytest.raises(ValueError):
        SamplingBackend(inner, sample_rate=-0.1)


def test_inner_property(inner, backend):
    assert backend.inner is inner


def test_sample_rate_property(inner):
    b = SamplingBackend(inner, sample_rate=0.5)
    assert b.sample_rate == 0.5


def test_acquire_delegates_when_sampled(inner, backend):
    result = backend.acquire("job", "owner1", 60)
    assert result is True
    inner.acquire.assert_called_once_with("job", "owner1", 60)


def test_acquire_skips_when_not_sampled(inner, monkeypatch):
    import random
    monkeypatch.setattr(random, "random", lambda: 0.99)
    b = SamplingBackend(inner, sample_rate=0.5)
    result = b.acquire("job", "owner1", 60)
    assert result is False
    inner.acquire.assert_not_called()


def test_acquire_proceeds_when_sampled(inner, monkeypatch):
    import random
    monkeypatch.setattr(random, "random", lambda: 0.1)
    b = SamplingBackend(inner, sample_rate=0.5)
    result = b.acquire("job", "owner1", 60)
    assert result is True
    inner.acquire.assert_called_once()


def test_release_delegates(inner, backend):
    result = backend.release("job", "owner1")
    assert result is True
    inner.release.assert_called_once_with("job", "owner1")


def test_is_locked_delegates(inner, backend):
    result = backend.is_locked("job")
    assert result is False
    inner.is_locked.assert_called_once_with("job")


def test_refresh_delegates(inner, backend):
    result = backend.refresh("job", "owner1", 60)
    assert result is True
    inner.refresh.assert_called_once_with("job", "owner1", 60)
