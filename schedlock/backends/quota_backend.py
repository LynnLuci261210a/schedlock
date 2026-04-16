"""Quota-aware backend wrapper that enforces acquisition limits."""

from schedlock.backends.base import BaseBackend
from schedlock.quota import LockQuota


class QuotaBackend(BaseBackend):
    """Wraps another backend and enforces a LockQuota before acquiring."""

    def __init__(self, inner: BaseBackend, quota: LockQuota):
        self._inner = inner
        self._quota = quota

    def acquire(self, lock_name: str, owner: str, ttl: int) -> bool:
        if not self._quota.allowed(lock_name):
            return False
        acquired = self._inner.acquire(lock_name, owner, ttl)
        if acquired:
            self._quota.record(lock_name, owner)
        return acquired

    def release(self, lock_name: str, owner: str) -> bool:
        return self._inner.release(lock_name, owner)

    def is_locked(self, lock_name: str) -> bool:
        return self._inner.is_locked(lock_name)

    def refresh(self, lock_name: str, owner: str, ttl: int) -> bool:
        return self._inner.refresh(lock_name, owner, ttl)
