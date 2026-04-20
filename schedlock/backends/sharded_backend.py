from __future__ import annotations
from typing import List
from schedlock.backends.base import BaseBackend


class ShardedBackend(BaseBackend):
    """Routes lock operations to a shard determined by hashing the key.

    Each key is consistently mapped to one backend shard, allowing
    horizontal scaling across multiple independent backend instances.
    """

    def __init__(self, backends: List[BaseBackend]) -> None:
        if not backends:
            raise ValueError("ShardedBackend requires at least one backend")
        self._backends = list(backends)

    @property
    def backends(self) -> List[BaseBackend]:
        return list(self._backends)

    def _shard(self, key: str) -> BaseBackend:
        index = hash(key) % len(self._backends)
        return self._backends[index]

    def acquire(self, key: str, owner: str, ttl: int) -> bool:
        return self._shard(key).acquire(key, owner, ttl)

    def release(self, key: str, owner: str) -> bool:
        return self._shard(key).release(key, owner)

    def is_locked(self, key: str) -> bool:
        return self._shard(key).is_locked(key)

    def refresh(self, key: str, owner: str, ttl: int) -> bool:
        return self._shard(key).refresh(key, owner, ttl)

    def __repr__(self) -> str:  # pragma: no cover
        return f"ShardedBackend(shards={len(self._backends)})"
