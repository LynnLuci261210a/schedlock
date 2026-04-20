"""AgingBackend — increases TTL for keys that are repeatedly acquired."""
from __future__ import annotations

import time
from typing import Optional

from schedlock.backends.base import BaseBackend


class AgingBackend(BaseBackend):
    """Wrapper that multiplies TTL by a growth factor each successive acquire.

    Each time the same key is successfully acquired the stored TTL is
    multiplied by *growth_factor* up to *max_ttl*.  Releasing a key resets
    its age counter.
    """

    def __init__(
        self,
        inner: BaseBackend,
        *,
        growth_factor: float = 1.5,
        max_ttl: float = 3600.0,
    ) -> None:
        if not isinstance(inner, BaseBackend):
            raise TypeError("inner must be a BaseBackend instance")
        if growth_factor <= 1.0:
            raise ValueError("growth_factor must be greater than 1.0")
        if max_ttl <= 0:
            raise ValueError("max_ttl must be positive")
        self._inner = inner
        self._growth_factor = growth_factor
        self._max_ttl = max_ttl
        # key -> (acquire_count, last_ttl)
        self._state: dict[str, tuple[int, float]] = {}

    @property
    def inner(self) -> BaseBackend:
        return self._inner

    @property
    def growth_factor(self) -> float:
        return self._growth_factor

    @property
    def max_ttl(self) -> float:
        return self._max_ttl

    def _aged_ttl(self, key: str, ttl: float) -> float:
        count, last = self._state.get(key, (0, ttl))
        aged = last * (self._growth_factor ** count) if count > 0 else ttl
        return min(aged, self._max_ttl)

    def acquire(self, key: str, owner: str, ttl: float) -> bool:
        effective_ttl = self._aged_ttl(key, ttl)
        acquired = self._inner.acquire(key, owner, effective_ttl)
        if acquired:
            count, _ = self._state.get(key, (0, ttl))
            self._state[key] = (count + 1, effective_ttl)
        return acquired

    def release(self, key: str, owner: str) -> bool:
        released = self._inner.release(key, owner)
        if released:
            self._state.pop(key, None)
        return released

    def is_locked(self, key: str) -> bool:
        return self._inner.is_locked(key)

    def refresh(self, key: str, owner: str, ttl: float) -> bool:
        return self._inner.refresh(key, owner, ttl)
