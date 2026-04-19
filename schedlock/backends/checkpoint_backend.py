from __future__ import annotations
from typing import Optional, Dict, Any
from schedlock.backends.base import BaseBackend


class CheckpointBackend(BaseBackend):
    """Wraps a backend and records the last successful acquire/release
    checkpoint per key, allowing callers to inspect resume state."""

    def __init__(self, inner: BaseBackend) -> None:
        if not isinstance(inner, BaseBackend):
            raise TypeError("inner must be a BaseBackend instance")
        self._inner = inner
        self._checkpoints: Dict[str, Dict[str, Any]] = {}

    @property
    def inner(self) -> BaseBackend:
        return self._inner

    def checkpoint_for(self, key: str) -> Optional[Dict[str, Any]]:
        """Return the last recorded checkpoint for *key*, or None."""
        return self._checkpoints.get(key)

    def clear_checkpoint(self, key: str) -> None:
        """Remove the stored checkpoint for *key*."""
        self._checkpoints.pop(key, None)

    def acquire(self, key: str, owner: str, ttl: int) -> bool:
        result = self._inner.acquire(key, owner, ttl)
        if result:
            self._checkpoints[key] = {
                "event": "acquired",
                "owner": owner,
                "ttl": ttl,
            }
        return result

    def release(self, key: str, owner: str) -> bool:
        result = self._inner.release(key, owner)
        if result:
            self._checkpoints[key] = {
                "event": "released",
                "owner": owner,
            }
        return result

    def is_locked(self, key: str) -> bool:
        return self._inner.is_locked(key)

    def refresh(self, key: str, owner: str, ttl: int) -> bool:
        return self._inner.refresh(key, owner, ttl)
