"""Integration tests combining TaggedBackend and NamespacedBackend with MemoryBackend."""
import pytest
from schedlock.backends.memory_backend import MemoryBackend
from schedlock.backends.tagged_backend import TaggedBackend
from schedlock.backends.namespaced_backend import NamespacedBackend


@pytest.fixture
def memory():
    return MemoryBackend()


def test_namespaced_isolates_keys(memory):
    a = NamespacedBackend(memory, "app-a")
    b = NamespacedBackend(memory, "app-b")
    assert a.acquire("job", "owner-a", 60)
    assert b.acquire("job", "owner-b", 60)
    assert a.is_locked("job")
    assert b.is_locked("job")


def test_namespaced_release_does_not_affect_other_ns(memory):
    a = NamespacedBackend(memory, "app-a")
    b = NamespacedBackend(memory, "app-b")
    a.acquire("job", "owner-a", 60)
    b.acquire("job", "owner-b", 60)
    a.release("job", "owner-a")
    assert not a.is_locked("job")
    assert b.is_locked("job")


def test_tagged_over_namespaced(memory):
    ns = NamespacedBackend(memory, "svc")
    tagged = TaggedBackend(ns, tags={"region": "us-east"})
    assert tagged.acquire("task", "worker-1", 30)
    assert tagged.get_tags("task") == {"region": "us-east"}
    assert tagged.is_locked("task")


def test_tagged_over_namespaced_release_clears_tags(memory):
    ns = NamespacedBackend(memory, "svc")
    tagged = TaggedBackend(ns, tags={"region": "eu-west"})
    tagged.acquire("task", "worker-1", 30)
    tagged.release("task", "worker-1")
    assert tagged.get_tags("task") is None
    assert not tagged.is_locked("task")


def test_locked_keys_reflects_multiple_acquires(memory):
    tagged = TaggedBackend(memory, tags={"env": "test"})
    tagged.acquire("job-1", "w1", 60)
    tagged.acquire("job-2", "w2", 60)
    assert set(tagged.locked_keys()) == {"job-1", "job-2"}
