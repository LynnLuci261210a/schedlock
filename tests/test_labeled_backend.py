import pytest
from unittest.mock import MagicMock
from schedlock.backends.labeled_backend import LabeledBackend
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
    return LabeledBackend(inner, {"env": "prod", "team": "platform"})


def test_requires_inner_backend():
    with pytest.raises(TypeError):
        LabeledBackend("not-a-backend", {"env": "prod"})


def test_requires_non_empty_labels(inner):
    with pytest.raises(ValueError):
        LabeledBackend(inner, {})


def test_requires_dict_labels(inner):
    with pytest.raises(TypeError):
        LabeledBackend(inner, "env=prod")


def test_requires_string_label_keys(inner):
    with pytest.raises(ValueError):
        LabeledBackend(inner, {123: "prod"})


def test_requires_string_label_values(inner):
    with pytest.raises(ValueError):
        LabeledBackend(inner, {"env": 42})


def test_inner_property(inner, backend):
    assert backend.inner is inner


def test_labels_property(backend):
    assert backend.labels == {"env": "prod", "team": "platform"}


def test_labels_returns_copy(backend):
    labels = backend.labels
    labels["env"] = "staging"
    assert backend.labels["env"] == "prod"


def test_get_label_existing(backend):
    assert backend.get_label("env") == "prod"


def test_get_label_missing(backend):
    assert backend.get_label("nonexistent") is None


def test_acquire_delegates(inner, backend):
    result = backend.acquire("job", "worker-1", 30)
    assert result is True
    inner.acquire.assert_called_once_with("job", "worker-1", 30)


def test_release_delegates(inner, backend):
    result = backend.release("job", "worker-1")
    assert result is True
    inner.release.assert_called_once_with("job", "worker-1")


def test_is_locked_delegates(inner, backend):
    result = backend.is_locked("job")
    assert result is False
    inner.is_locked.assert_called_once_with("job")


def test_refresh_delegates(inner, backend):
    result = backend.refresh("job", "worker-1", 60)
    assert result is True
    inner.refresh.assert_called_once_with("job", "worker-1", 60)
