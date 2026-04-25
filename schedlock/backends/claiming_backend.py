"""ClaimingBackend — wraps an inner backend and enforces a maximum claim
duration per owner. Once an owner has held a lock for longer than
``max_claim_seconds``, any subsequent acquire attempt by that same owner
for the same key is rejected until the existing claim is released.
"""
from __future__ import annotations

import time
from typing import Optional

from schedlock.backends.base import BaseBackend


class ClaimingBackend(BaseBackend):
    """Prevent an owner from re-acquiring a lock it already holds beyond
    the allowed claim window.

    Args:
        inner: Underlying backend to delegate to.
        max_claim_seconds: Maximum seconds an owner may hold a claim before
            a re-acquire attempt is rejected.  Must be a positive number.
    """

    def __init__(self, inner: BaseBackend, max_claim_seconds: float = 60.0) -> None:
        if not isinstance(inner, BaseBackend):
            raise TypeError("inner must be a BaseBackend instance")
        if not isinstance(max_claim_seconds, (int, float)) or isinstance(max_claim_seconds, bool):
            raise TypeError("max_claim_seconds must be a numeric value")
        if max_claim_seconds <= 0:
            raise ValueError("max_claim_seconds must be positive")

        self._inner = inner
        self._max_claim_seconds = float(max_claim_seconds)
        # {(key, owner): acquired_at}
        self._claims: dict[tuple[str, str], float] = {}

    @property
    def inner(self) -> BaseBackend:
        return self._inner

    @property
    def max_claim_seconds(self) -> float:
        return self._max_claim_seconds

    def _claim_key(self, key: str, owner: str) -> tuple[str, str]:
        return (key, owner)

    def _is_overdue(self, key: str, owner: str) -> bool:
        ck = self._claim_key(key, owner)
        acquired_at = self._claims.get(ck)
        if acquired_at is None:
            return False
        return (time.monotonic() - acquired_at) > self._max_claim_seconds

    def acquire(self, key: str, owner: str, ttl: int = 30) -> bool:
        if self._is_overdue(key, owner):
            return False
        result = self._inner.acquire(key, owner, ttl)
        if result:
            ck = self._claim_key(key, owner)
            if ck not in self._claims:
                self._claims[ck] = time.monotonic()
        return result

    def release(self, key: str, owner: str) -> bool:
        result = self._inner.release(key, owner)
        if result:
            self._claims.pop(self._claim_key(key, owner), None)
        return result

    def is_locked(self, key: str) -> bool:
        return self._inner.is_locked(key)

    def refresh(self, key: str, owner: str, ttl: int = 30) -> bool:
        return self._inner.refresh(key, owner, ttl)

    def claim_age(self, key: str, owner: str) -> Optional[float]:
        """Return how many seconds the owner has held the claim, or None."""
        acquired_at = self._claims.get(self._claim_key(key, owner))
        if acquired_at is None:
            return None
        return time.monotonic() - acquired_at
