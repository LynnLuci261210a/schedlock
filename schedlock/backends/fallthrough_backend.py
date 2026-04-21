from __future__ import annotations

from typing import Optional

from schedlock.backends.base import BaseBackend


class FallthroughBackend(BaseBackend):
    """
    A backend wrapper that, on a failed acquire, transparently delegates
    the attempt to a chain of fallback backends in order.  The first
    backend in the chain that succeeds wins; release is always sent to
    the backend that originally granted the lock.

    Unlike FallbackBackend (which has exactly one fallback), this wrapper
    accepts an arbitrary ordered list of backends and tries each in turn.
    """

    def __init__(self, backends: list[BaseBackend]) -> None:
        if not backends or len(backends) < 2:
            raise ValueError("FallthroughBackend requires at least two backends.")
        for i, b in enumerate(backends):
            if not isinstance(b, BaseBackend):
                raise TypeError(f"backends[{i}] must be a BaseBackend instance.")
        self._backends = list(backends)
        # Maps (key, owner) -> index of the backend that granted the lock
        self._granted: dict[tuple[str, str], int] = {}

    @property
    def backends(self) -> list[BaseBackend]:
        return list(self._backends)

    def acquire(self, key: str, owner: str, ttl: int) -> bool:
        for idx, backend in enumerate(self._backends):
            try:
                if backend.acquire(key, owner, ttl):
                    self._granted[(key, owner)] = idx
                    return True
            except Exception:
                continue
        return False

    def release(self, key: str, owner: str) -> bool:
        idx = self._granted.pop((key, owner), None)
        if idx is not None:
            return self._backends[idx].release(key, owner)
        # Best-effort: try all backends
        released = False
        for backend in self._backends:
            try:
                if backend.release(key, owner):
                    released = True
                    break
            except Exception:
                continue
        return released

    def is_locked(self, key: str) -> bool:
        return any(
            b.is_locked(key)
            for b in self._backends
            if self._safe_is_locked(b, key)
        )

    def _safe_is_locked(self, backend: BaseBackend, key: str) -> bool:
        try:
            return backend.is_locked(key)
        except Exception:
            return False

    def refresh(self, key: str, owner: str, ttl: int) -> bool:
        idx = self._granted.get((key, owner))
        if idx is not None:
            return self._backends[idx].refresh(key, owner, ttl)
        return False
