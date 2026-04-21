"""StickyTTLBackend — extends lock TTL automatically each time the same owner re-acquires."""

from __future__ import annotations

from typing import Optional

from schedlock.backends.base import BaseBackend


class StickyTTLBackend(BaseBackend):
    """Wrapper that multiplies the TTL by a growth factor each time the same
    owner successfully re-acquires the same key, up to *max_ttl* seconds.

    This is useful for long-running jobs that keep re-scheduling themselves:
    the lock lifetime grows proportionally to how often the job runs, reducing
    churn against the underlying backend.

    Args:
        inner: The backend to delegate locking to.
        growth_factor: Multiplier applied to the TTL on each re-acquire (> 1.0).
        max_ttl: Upper bound on the effective TTL in seconds.
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
        # Maps (key, owner) -> current effective TTL
        self._ttl_map: dict[tuple[str, str], float] = {}

    @property
    def inner(self) -> BaseBackend:
        return self._inner

    @property
    def growth_factor(self) -> float:
        return self._growth_factor

    @property
    def max_ttl(self) -> float:
        return self._max_ttl

    def effective_ttl_for(self, key: str, owner: str) -> Optional[float]:
        """Return the current effective TTL recorded for *key* / *owner*, or None."""
        return self._ttl_map.get((key, owner))

    def acquire(self, key: str, owner: str, ttl: int) -> bool:
        slot = (key, owner)
        current = self._ttl_map.get(slot)
        if current is not None:
            new_ttl = min(current * self._growth_factor, self._max_ttl)
        else:
            new_ttl = float(ttl)

        effective = int(new_ttl)
        acquired = self._inner.acquire(key, owner, effective)
        if acquired:
            self._ttl_map[slot] = new_ttl
        return acquired

    def release(self, key: str, owner: str) -> bool:
        released = self._inner.release(key, owner)
        if released:
            self._ttl_map.pop((key, owner), None)
        return released

    def is_locked(self, key: str) -> bool:
        return self._inner.is_locked(key)

    def refresh(self, key: str, owner: str, ttl: int) -> bool:
        return self._inner.refresh(key, owner, ttl)
