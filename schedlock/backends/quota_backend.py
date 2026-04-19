from schedlock.backends.base import BaseBackend
from schedlock.quota import LockQuota


class QuotaBackend(BaseBackend):
    """Wraps a backend and enforces per-key acquire quotas within a time window."""

    def __init__(self, inner: BaseBackend, max_acquires: int, window: float):
        if not isinstance(inner, BaseBackend):
            raise TypeError("inner must be a BaseBackend instance")
        self._inner = inner
        self._quotas: dict[str, LockQuota] = {}
        self._max_acquires = max_acquires
        self._window = window

    @property
    def inner(self) -> BaseBackend:
        return self._inner

    def _get_quota(self, key: str) -> LockQuota:
        if key not in self._quotas:
            self._quotas[key] = LockQuota(max_acquires=self._max_acquires, window=self._window)
        return self._quotas[key]

    def acquire(self, key: str, owner: str, ttl: float) -> bool:
        quota = self._get_quota(key)
        if not quota.allowed(key):
            return False
        acquired = self._inner.acquire(key, owner, ttl)
        if acquired:
            quota.record(key)
        return acquired

    def release(self, key: str, owner: str) -> bool:
        return self._inner.release(key, owner)

    def is_locked(self, key: str) -> bool:
        return self._inner.is_locked(key)

    def refresh(self, key: str, owner: str, ttl: float) -> bool:
        return self._inner.refresh(key, owner, ttl)
