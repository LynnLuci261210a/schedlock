from schedlock.backends.base import BaseBackend


class PrioritizedBackend(BaseBackend):
    """
    Wraps a backend and attaches a numeric priority to each acquire.
    Only allows acquisition if the caller's priority is >= the minimum
    required priority for the key.
    """

    def __init__(self, inner: BaseBackend, min_priority: int = 0):
        if not isinstance(inner, BaseBackend):
            raise TypeError("inner must be a BaseBackend instance")
        if not isinstance(min_priority, int) or min_priority < 0:
            raise ValueError("min_priority must be a non-negative integer")
        self._inner = inner
        self._min_priority = min_priority

    @property
    def inner(self) -> BaseBackend:
        return self._inner

    @property
    def min_priority(self) -> int:
        return self._min_priority

    def acquire(self, key: str, owner: str, ttl: int, priority: int = 0) -> bool:
        if not isinstance(priority, int) or priority < 0:
            raise ValueError("priority must be a non-negative integer")
        if priority < self._min_priority:
            return False
        return self._inner.acquire(key, owner, ttl)

    def release(self, key: str, owner: str) -> bool:
        return self._inner.release(key, owner)

    def is_locked(self, key: str) -> bool:
        return self._inner.is_locked(key)

    def refresh(self, key: str, owner: str, ttl: int) -> bool:
        return self._inner.refresh(key, owner, ttl)
