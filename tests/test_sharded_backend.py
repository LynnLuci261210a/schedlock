import pytest
from unittest.mock import MagicMock
from schedlock.backends.sharded_backend import ShardedBackend
from schedlock.backends.memory_backend import MemoryBackend


def make_backend():
    b = MagicMock()
    b.acquire.return_value = True
    b.release.return_value = True
    b.is_locked.return_value = False
    b.refresh.return_value = True
    return b


@pytest.fixture
def shards():
    return [make_backend(), make_backend(), make_backend()]


@pytest.fixture
def backend(shards):
    return ShardedBackend(shards)


def test_requires_at_least_one_backend():
    with pytest.raises(ValueError, match="at least one backend"):
        ShardedBackend([])


def test_backends_property(shards, backend):
    assert backend.backends == shards


def test_backends_property_returns_copy(shards, backend):
    result = backend.backends
    result.clear()
    assert len(backend.backends) == 3


def test_acquire_routes_to_shard(shards, backend):
    key = "job:test"
    expected_index = hash(key) % 3
    backend.acquire(key, "owner-1", 30)
    shards[expected_index].acquire.assert_called_once_with(key, "owner-1", 30)
    for i, s in enumerate(shards):
        if i != expected_index:
            s.acquire.assert_not_called()


def test_release_routes_to_same_shard(shards, backend):
    key = "job:test"
    expected_index = hash(key) % 3
    backend.release(key, "owner-1")
    shards[expected_index].release.assert_called_once_with(key, "owner-1")


def test_is_locked_routes_to_shard(shards, backend):
    key = "job:test"
    expected_index = hash(key) % 3
    shards[expected_index].is_locked.return_value = True
    assert backend.is_locked(key) is True


def test_refresh_routes_to_shard(shards, backend):
    key = "job:test"
    expected_index = hash(key) % 3
    backend.refresh(key, "owner-1", 60)
    shards[expected_index].refresh.assert_called_once_with(key, "owner-1", 60)


def test_different_keys_may_route_to_different_shards():
    b0 = make_backend()
    b1 = make_backend()
    sharded = ShardedBackend([b0, b1])
    keys = [f"job:{i}" for i in range(20)]
    used = set()
    for k in keys:
        used.add(hash(k) % 2)
    assert len(used) == 2, "Expected both shards to be used across 20 keys"


def test_single_shard_always_routes_to_it():
    b = make_backend()
    sharded = ShardedBackend([b])
    sharded.acquire("any:key", "owner", 10)
    b.acquire.assert_called_once()


def test_integration_with_memory_backends():
    shards = [MemoryBackend() for _ in range(3)]
    sharded = ShardedBackend(shards)
    key = "report:daily"
    assert sharded.acquire(key, "worker-1", 60) is True
    assert sharded.is_locked(key) is True
    assert sharded.acquire(key, "worker-2", 60) is False
    assert sharded.release(key, "worker-1") is True
    assert sharded.is_locked(key) is False
