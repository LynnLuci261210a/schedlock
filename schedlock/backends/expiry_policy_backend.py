from __future__ import annotations
from typing import Callable, Optional
from schedlock.backends.base import BaseBackend


class ExpiryPolicyBackend(BaseBackend):
    """Wraps a backend and dynamically computes TTL via a policy callable.

    The policy receives (key, owner) and returns a TTL in seconds.
    This allows per-key or per-owner TTL strategies without changing call sites.
    """

    def __init__(
        self,
        inner: BaseBackend,
        policy: Callable[[str, str], int],
    ) -> None:
        if not isinstance(inner, BaseBackend):
            raise TypeError("inner must be a BaseBackend instance")
        if not callable(policy):
            raise TypeError("policy must be callable")
        self._inner = inner
        self._policy = policy

    @property
    def inner(self) -> BaseBackend:
        return self._inner

    @property
    def policy(self) -> Callable[[str, str], int]:
        return self._policy

    def acquire(self, key: str, owner: str, ttl: int) -> bool:
        computed_ttl = self._policy(key, owner)
        if not isinstance(computed_ttl, int) or computed_ttl <= 0:
            raise ValueError(
                f"policy must return a positive int TTL, got {computed_ttl!r}"
            )
        return self._inner.acquire(key, owner, computed_ttl)

    def release(self, key: str, owner: str) -> bool:
        return self._inner.release(key, owner)

    def is_locked(self, key: str) -> bool:
        return self._inner.is_locked(key)

    def refresh(self, key: str, owner: str, ttl: int) -> bool:
        computed_ttl = self._policy(key, owner)
        if not isinstance(computed_ttl, int) or computed_ttl <= 0:
            raise ValueError(
                f"policy must return a positive int TTL, got {computed_ttl!r}"
            )
        return self._inner.refresh(key, owner, computed_ttl)
