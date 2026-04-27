from __future__ import annotations

from typing import Optional, Set

from schedlock.backends.base import BaseBackend


class AllowlistBackend(BaseBackend):
    """Only permits acquire/release for owners present in an explicit allowlist."""

    def __init__(self, inner: BaseBackend, allowed: Set[str]) -> None:
        if not isinstance(inner, BaseBackend):
            raise TypeError("inner must be a BaseBackend instance")
        if not allowed or not isinstance(allowed, (set, frozenset, list)):
            raise ValueError("allowed must be a non-empty collection of owner strings")
        allowed_set = frozenset(allowed)
        if not all(isinstance(o, str) and o for o in allowed_set):
            raise ValueError("all entries in allowed must be non-empty strings")
        self._inner = inner
        self._allowed: frozenset = allowed_set

    @property
    def inner(self) -> BaseBackend:
        return self._inner

    @property
    def allowed(self) -> frozenset:
        return self._allowed

    def _check_owner(self, owner: str) -> None:
        if owner not in self._allowed:
            raise PermissionError(
                f"Owner '{owner}' is not in the allowlist"
            )

    def acquire(self, key: str, owner: str, ttl: int) -> bool:
        self._check_owner(owner)
        return self._inner.acquire(key, owner, ttl)

    def release(self, key: str, owner: str) -> bool:
        self._check_owner(owner)
        return self._inner.release(key, owner)

    def is_locked(self, key: str) -> bool:
        return self._inner.is_locked(key)

    def refresh(self, key: str, owner: str, ttl: int) -> bool:
        self._check_owner(owner)
        return self._inner.refresh(key, owner, ttl)
