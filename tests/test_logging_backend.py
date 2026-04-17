import logging
import pytest
from unittest.mock import MagicMock
from schedlock.backends.base import BaseBackend
from schedlock.backends.logging_backend import LoggingBackend


@pytest.fixture
def inner():
    m = MagicMock(spec=BaseBackend)
    return m


@pytest.fixture
def backend(inner):
    return LoggingBackend(inner)


def test_requires_inner_backend():
    with pytest.raises(TypeError):
        LoggingBackend("not-a-backend")


def test_inner_property(inner, backend):
    assert backend.inner is inner


def test_acquire_success_logs(inner, backend, caplog):
    inner.acquire.return_value = True
    with caplog.at_level(logging.DEBUG, logger="schedlock.backends.logging_backend"):
        result = backend.acquire("job", "worker-1", 60)
    assert result is True
    assert "acquired lock" in caplog.text
    assert "job" in caplog.text


def test_acquire_failure_logs(inner, backend, caplog):
    inner.acquire.return_value = False
    with caplog.at_level(logging.DEBUG, logger="schedlock.backends.logging_backend"):
        result = backend.acquire("job", "worker-1", 60)
    assert result is False
    assert "failed to acquire" in caplog.text


def test_release_success_logs(inner, backend, caplog):
    inner.release.return_value = True
    with caplog.at_level(logging.DEBUG, logger="schedlock.backends.logging_backend"):
        result = backend.release("job", "worker-1")
    assert result is True
    assert "released lock" in caplog.text


def test_release_failure_logs(inner, backend, caplog):
    inner.release.return_value = False
    with caplog.at_level(logging.DEBUG, logger="schedlock.backends.logging_backend"):
        result = backend.release("job", "worker-1")
    assert result is False
    assert "release failed" in caplog.text


def test_is_locked_logs(inner, backend, caplog):
    inner.is_locked.return_value = True
    with caplog.at_level(logging.DEBUG, logger="schedlock.backends.logging_backend"):
        result = backend.is_locked("job")
    assert result is True
    assert "is_locked" in caplog.text


def test_refresh_logs(inner, backend, caplog):
    inner.refresh.return_value = True
    with caplog.at_level(logging.DEBUG, logger="schedlock.backends.logging_backend"):
        result = backend.refresh("job", "worker-1", 30)
    assert result is True
    assert "refresh" in caplog.text


def test_custom_log_level(inner, caplog):
    b = LoggingBackend(inner, level=logging.WARNING)
    inner.acquire.return_value = True
    with caplog.at_level(logging.WARNING, logger="schedlock.backends.logging_backend"):
        b.acquire("job", "worker-1", 60)
    assert "acquired lock" in caplog.text
