import time
import random
from schedlock.backends.base import BaseBackend


class StaggeredBackend(BaseBackend):
    """Wraps a backend and introduces a random staggered delay before acquire.

    Useful when many workers start simultaneously and you want to spread
    out lock acquisition attempts to reduce thundering-herd contention.
    """

    def __init__(self, inner: BaseBackend, max_delay: float = 1.0):
        if not isinstance(inner, BaseBackend):
            raise TypeError("inner must be a BaseBackend instance")
        if not isinstance(max_delay, (int, float)) or max_delay <= 0:
            raise ValueError("max_delay must be a positive number")
        self._inner = inner
        self._max_delay = float(max_delay)

    @property
    def inner(self) -> BaseBackend:
        return self._inner

    @property
    def max_delay(self) -> float:
        return self._max_delay

    def acquire(self, key: str, owner: str, ttl: int) -> bool:
        delay = random.uniform(0, self._max_delay)
        time.sleep(delay)
        return self._inner.acquire(key, owner, ttl)

    def release(self, key: str, owner: str) -> bool:
        return self._inner.release(key, owner)

    def is_locked(self, key: str) -> bool:
        return self._inner.is_locked(key)

    def refresh(self, key: str, owner: str, ttl: int) -> bool:
        return self._inner.refresh(key, owner, ttl)
