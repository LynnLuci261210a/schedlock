from schedlock.backends.base import BaseBackend
import random


class SamplingBackend(BaseBackend):
    """Wraps a backend and only attempts acquire with a given probability.

    Useful for gradual rollouts or load-shedding scenarios where you want
    only a fraction of workers to compete for a lock.
    """

    def __init__(self, inner: BaseBackend, sample_rate: float = 1.0):
        if not isinstance(inner, BaseBackend):
            raise TypeError("inner must be a BaseBackend instance")
        if not (0.0 < sample_rate <= 1.0):
            raise ValueError("sample_rate must be in the range (0.0, 1.0]")
        self._inner = inner
        self._sample_rate = sample_rate

    @property
    def inner(self) -> BaseBackend:
        return self._inner

    @property
    def sample_rate(self) -> float:
        return self._sample_rate

    def acquire(self, key: str, owner: str, ttl: int) -> bool:
        if random.random() > self._sample_rate:
            return False
        return self._inner.acquire(key, owner, ttl)

    def release(self, key: str, owner: str) -> bool:
        return self._inner.release(key, owner)

    def is_locked(self, key: str) -> bool:
        return self._inner.is_locked(key)

    def refresh(self, key: str, owner: str, ttl: int) -> bool:
        return self._inner.refresh(key, owner, ttl)
