import pytest
from unittest.mock import MagicMock
from schedlock.backends.tagged_backend import TaggedBackend
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
    return TaggedBackend(inner, tags={"env": "test", "team": "platform"})


def test_requires_inner_backend():
    with pytest.raises(TypeError):
        TaggedBackend("not-a-backend")


def test_tags_property(backend):
    assert backend.tags == {"env": "test", "team": "platform"}


def test_acquire_delegates_to_inner(backend, inner):
    result = backend.acquire("job", "owner-1", 60)
    assert result is True
    inner.acquire.assert_called_once_with("job", "owner-1", 60)


def test_acquire_stores_tags_on_success(backend):
    backend.acquire("job", "owner-1", 60)
    assert backend.get_tags("job") == {"env": "test", "team": "platform"}


def test_acquire_does_not_store_tags_on_failure(inner):
    inner.acquire.return_value = False
    b = TaggedBackend(inner, tags={"env": "prod"})
    b.acquire("job", "owner-1", 60)
    assert b.get_tags("job") is None


def test_release_removes_tags(backend):
    backend.acquire("job", "owner-1", 60)
    backend.release("job", "owner-1")
    assert backend.get_tags("job") is None


def test_release_delegates_to_inner(backend, inner):
    backend.acquire("job", "owner-1", 60)
    backend.release("job", "owner-1")
    inner.release.assert_called_once_with("job", "owner-1")


def test_is_locked_delegates(backend, inner):
    inner.is_locked.return_value = True
    assert backend.is_locked("job") is True


def test_refresh_delegates(backend, inner):
    backend.refresh("job", "owner-1", 30)
    inner.refresh.assert_called_once_with("job", "owner-1", 30)


def test_locked_keys_tracks_active_locks(backend):
    backend.acquire("job-a", "owner-1", 60)
    backend.acquire("job-b", "owner-2", 60)
    assert set(backend.locked_keys()) == {"job-a", "job-b"}


def test_locked_keys_removes_after_release(backend):
    backend.acquire("job-a", "owner-1", 60)
    backend.release("job-a", "owner-1")
    assert "job-a" not in backend.locked_keys()


def test_tags_are_copied_not_shared(inner):
    tags = {"env": "staging"}
    b = TaggedBackend(inner, tags=tags)
    b.acquire("job", "owner", 60)
    tags["env"] = "mutated"
    assert b.get_tags("job")["env"] == "staging"
