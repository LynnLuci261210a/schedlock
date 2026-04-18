import random
import time
from schedlock.backends.base import BaseBackend


class JitterBackend(BaseBackend):
    """Wraps a backend and adds random jitter delay before acquire attempts.

    Useful for reducing thundering-herd problems when many workers compete
    for the same lock simultaneously.
    """

    def __init__(self, inner: BaseBackend, max_jitter: float = 1.0):
        if not isinstance(inner, BaseBackend):
            raise TypeError("inner must be a BaseBackend instance")
        if not isinstance(max_jitter, (int, float)) or max_jitter <= 0:
            raise ValueError("max_jitter must be a positive number")
        self._inner = inner
        self._max_jitter = max_jitter

    @property
    def inner(self) -> BaseBackend:
        return self._inner

    @property
    def max_jitter(self) -> float:
        return self._max_jitter

    def acquire(self, key: str, owner: str, ttl: int) -> bool:
        delay = random.uniform(0, self._max_jitter)
        time.sleep(delay)
        return self._inner.acquire(key, owner, ttl)

    def release(self, key: str, owner: str) -> bool:
        return self._inner.release(key, owner)

    def is_locked(self, key: str) -> bool:
        return self._inner.is_locked(key)

    def refresh(self, key: str, owner: str, ttl: int) -> bool:
        return self._inner.refresh(key, owner, ttl)
