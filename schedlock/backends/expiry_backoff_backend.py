"""ExpiryBackoffBackend — increases TTL exponentially on repeated acquire failures."""

from __future__ import annotations

import time
from typing import Optional

from schedlock.backends.base import BaseBackend


class ExpiryBackoffBackend(BaseBackend):
    """Wraps an inner backend and applies exponential TTL backoff per key.

    Each time an acquire fails for a given key the next successful acquire
    will use a TTL that is multiplied by *growth_factor* (up to *max_ttl*).
    A successful release resets the backoff for that key.
    """

    def __init__(
        self,
        inner: BaseBackend,
        *,
        growth_factor: float = 2.0,
        max_ttl: float = 3600.0,
    ) -> None:
        if not isinstance(inner, BaseBackend):
            raise TypeError("inner must be a BaseBackend instance")
        if growth_factor <= 1.0:
            raise ValueError("growth_factor must be greater than 1.0")
        if max_ttl <= 0:
            raise ValueError("max_ttl must be a positive number")

        self._inner = inner
        self._growth_factor = growth_factor
        self._max_ttl = max_ttl
        # key -> (failure_count, current_multiplier)
        self._failures: dict[str, int] = {}
        self._multiplier: dict[str, float] = {}

    @property
    def inner(self) -> BaseBackend:
        return self._inner

    @property
    def growth_factor(self) -> float:
        return self._growth_factor

    @property
    def max_ttl(self) -> float:
        return self._max_ttl

    def _effective_ttl(self, key: str, ttl: Optional[float]) -> Optional[float]:
        if ttl is None:
            return ttl
        multiplier = self._multiplier.get(key, 1.0)
        return min(ttl * multiplier, self._max_ttl)

    def acquire(self, key: str, owner: str, ttl: Optional[float] = None) -> bool:
        effective = self._effective_ttl(key, ttl)
        result = self._inner.acquire(key, owner, effective)
        if result:
            # Reset backoff on success
            self._failures.pop(key, None)
            self._multiplier.pop(key, None)
        else:
            # Grow multiplier for next attempt
            failures = self._failures.get(key, 0) + 1
            self._failures[key] = failures
            current = self._multiplier.get(key, 1.0)
            self._multiplier[key] = min(current * self._growth_factor, self._max_ttl)
        return result

    def release(self, key: str, owner: str) -> bool:
        result = self._inner.release(key, owner)
        if result:
            self._failures.pop(key, None)
            self._multiplier.pop(key, None)
        return result

    def is_locked(self, key: str) -> bool:
        return self._inner.is_locked(key)

    def refresh(self, key: str, owner: str, ttl: float) -> bool:
        return self._inner.refresh(key, owner, ttl)

    def failure_count(self, key: str) -> int:
        """Return the current consecutive failure count for *key*."""
        return self._failures.get(key, 0)

    def current_multiplier(self, key: str) -> float:
        """Return the current TTL multiplier for *key*."""
        return self._multiplier.get(key, 1.0)
