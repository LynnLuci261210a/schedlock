"""BudgetBackend: limits total number of successful acquires over the lifetime of the backend."""
from schedlock.backends.base import BaseBackend


class BudgetBackend(BaseBackend):
    """Wraps a backend and enforces a lifetime acquire budget.

    Once ``max_acquires`` successful acquires have been made, further
    acquire attempts are rejected regardless of lock state.
    """

    def __init__(self, inner: BaseBackend, max_acquires: int) -> None:
        if not isinstance(inner, BaseBackend):
            raise TypeError("inner must be a BaseBackend instance")
        if not isinstance(max_acquires, int) or max_acquires < 1:
            raise ValueError("max_acquires must be a positive integer")
        self._inner = inner
        self._max_acquires = max_acquires
        self._count = 0

    @property
    def inner(self) -> BaseBackend:
        return self._inner

    @property
    def max_acquires(self) -> int:
        return self._max_acquires

    @property
    def used(self) -> int:
        return self._count

    @property
    def remaining(self) -> int:
        return max(0, self._max_acquires - self._count)

    def acquire(self, key: str, owner: str, ttl: int) -> bool:
        if self._count >= self._max_acquires:
            return False
        result = self._inner.acquire(key, owner, ttl)
        if result:
            self._count += 1
        return result

    def release(self, key: str, owner: str) -> bool:
        return self._inner.release(key, owner)

    def is_locked(self, key: str) -> bool:
        return self._inner.is_locked(key)

    def refresh(self, key: str, owner: str, ttl: int) -> bool:
        return self._inner.refresh(key, owner, ttl)
