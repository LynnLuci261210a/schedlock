from __future__ import annotations

from typing import Callable

from schedlock.backends.base import BaseBackend


class WeightBackend(BaseBackend):
    """Wraps a backend and gates acquire calls by a weight function.

    The weight function receives the lock key and owner and returns a
    numeric weight.  If the weight is below ``min_weight`` the acquire
    is rejected without touching the inner backend.
    """

    def __init__(
        self,
        inner: BaseBackend,
        weight_fn: Callable[[str, str], float],
        min_weight: float = 1.0,
    ) -> None:
        if not isinstance(inner, BaseBackend):
            raise TypeError("inner must be a BaseBackend instance")
        if not callable(weight_fn):
            raise TypeError("weight_fn must be callable")
        if not isinstance(min_weight, (int, float)):
            raise TypeError("min_weight must be numeric")
        self._inner = inner
        self._weight_fn = weight_fn
        self._min_weight = float(min_weight)

    @property
    def inner(self) -> BaseBackend:
        return self._inner

    @property
    def min_weight(self) -> float:
        return self._min_weight

    def last_weight(self, key: str) -> float | None:
        """Return the most recently computed weight for *key*, or None."""
        return self._last_weights.get(key) if hasattr(self, "_last_weights") else None

    def acquire(self, key: str, owner: str, ttl: int = 30) -> bool:
        if not hasattr(self, "_last_weights"):
            self._last_weights: dict[str, float] = {}
        weight = float(self._weight_fn(key, owner))
        self._last_weights[key] = weight
        if weight < self._min_weight:
            return False
        return self._inner.acquire(key, owner, ttl)

    def release(self, key: str, owner: str) -> bool:
        return self._inner.release(key, owner)

    def is_locked(self, key: str) -> bool:
        return self._inner.is_locked(key)

    def refresh(self, key: str, owner: str, ttl: int = 30) -> bool:
        return self._inner.refresh(key, owner, ttl)
