"""ExpiryJitterBackend — adds random jitter to TTL on acquire to avoid thundering herd."""

from __future__ import annotations

import random
from typing import Optional

from schedlock.backends.base import BaseBackend


class ExpiryJitterBackend(BaseBackend):
    """Wraps an inner backend and applies random jitter to the TTL on acquire.

    When many workers compete for the same lock with identical TTLs, they may
    all expire at the same moment causing a thundering-herd burst.  This
    backend spreads expiry times by adding a uniformly-distributed random
    offset in ``[0, max_jitter]`` seconds to every requested TTL.

    Args:
        inner: The backend to delegate lock operations to.
        max_jitter: Maximum number of seconds of jitter to add.  Must be > 0.
        seed: Optional RNG seed for deterministic testing.
    """

    def __init__(
        self,
        inner: BaseBackend,
        max_jitter: float = 5.0,
        seed: Optional[int] = None,
    ) -> None:
        if not isinstance(inner, BaseBackend):
            raise TypeError("inner must be a BaseBackend instance")
        if not isinstance(max_jitter, (int, float)) or max_jitter <= 0:
            raise ValueError("max_jitter must be a positive number")
        self._inner = inner
        self._max_jitter = float(max_jitter)
        self._rng = random.Random(seed)

    @property
    def inner(self) -> BaseBackend:
        return self._inner

    @property
    def max_jitter(self) -> float:
        return self._max_jitter

    def acquire(self, key: str, owner: str, ttl: int) -> bool:
        jitter = self._rng.uniform(0, self._max_jitter)
        adjusted_ttl = max(1, int(ttl + jitter))
        return self._inner.acquire(key, owner, adjusted_ttl)

    def release(self, key: str, owner: str) -> bool:
        return self._inner.release(key, owner)

    def is_locked(self, key: str) -> bool:
        return self._inner.is_locked(key)

    def refresh(self, key: str, owner: str, ttl: int) -> bool:
        jitter = self._rng.uniform(0, self._max_jitter)
        adjusted_ttl = max(1, int(ttl + jitter))
        return self._inner.refresh(key, owner, adjusted_ttl)
