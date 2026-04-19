import pytest
from unittest.mock import MagicMock
from schedlock.backends.tagging_policy_backend import TaggingPolicyBackend
from schedlock.backends.memory_backend import MemoryBackend


@pytest.fixture
def inner():
    return MagicMock(spec=["acquire", "release", "is_locked", "refresh"])


@pytest.fixture
def backend(inner):
    return TaggingPolicyBackend(inner, policy=lambda key: {"env": "test", "key": key})


def test_requires_inner_backend():
    with pytest.raises(TypeError, match="inner must be a BaseBackend"):
        TaggingPolicyBackend(object(), policy=lambda k: {})


def test_requires_callable_policy():
    mem = MemoryBackend()
    with pytest.raises(TypeError, match="policy must be callable"):
        TaggingPolicyBackend(mem, policy="not-callable")


def test_inner_property(inner, backend):
    assert backend.inner is inner


def test_acquire_success_applies_policy(inner, backend):
    inner.acquire.return_value = True
    result = backend.acquire("job", "worker-1", 30)
    assert result is True
    tags = backend.tags_for("job")
    assert tags["env"] == "test"
    assert tags["key"] == "job"


def test_acquire_failure_does_not_apply_policy(inner, backend):
    inner.acquire.return_value = False
    backend.acquire("job", "worker-1", 30)
    assert backend.tags_for("job") == {}


def test_release_clears_tags(inner, backend):
    inner.acquire.return_value = True
    inner.release.return_value = True
    backend.acquire("job", "worker-1", 30)
    assert backend.tags_for("job") != {}
    backend.release("job", "worker-1")
    assert backend.tags_for("job") == {}


def test_release_failed_does_not_clear_tags(inner, backend):
    inner.acquire.return_value = True
    inner.release.return_value = False
    backend.acquire("job", "worker-1", 30)
    backend.release("job", "worker-1")
    assert backend.tags_for("job") != {}


def test_policy_exception_is_swallowed(inner):
    def bad_policy(key):
        raise RuntimeError("boom")

    mem = MemoryBackend()
    b = TaggingPolicyBackend(mem, policy=bad_policy)
    result = b.acquire("job", "worker-1", 30)
    assert result is True
    assert b.tags_for("job") == {}


def test_is_locked_delegates(inner, backend):
    inner.is_locked.return_value = True
    assert backend.is_locked("job") is True


def test_refresh_delegates(inner, backend):
    inner.refresh.return_value = True
    assert backend.refresh("job", "worker-1", 60) is True


def test_tags_for_unknown_key_returns_empty(backend):
    assert backend.tags_for("nonexistent") == {}


def test_tags_are_copied(inner, backend):
    inner.acquire.return_value = True
    backend.acquire("job", "w", 10)
    t1 = backend.tags_for("job")
    t1["env"] = "mutated"
    assert backend.tags_for("job")["env"] == "test"
