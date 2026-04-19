from time import time
from schedlock.backends.base import BaseBackend


class DebounceBackend(BaseBackend):
    """
    Wraps a backend and suppresses repeated acquire attempts for the same
    lock key within a cooldown window after a successful acquire.
    """

    def __init__(self, inner: BaseBackend, cooldown: float = 5.0):
        if not isinstance(inner, BaseBackend):
            raise TypeError("inner must be a BaseBackend instance")
        if cooldown <= 0:
            raise ValueError("cooldown must be a positive number")
        self._inner = inner
        self._cooldown = cooldown
        self._last_acquired: dict[str, float] = {}

    @property
    def inner(self) -> BaseBackend:
        return self._inner

    @property
    def cooldown(self) -> float:
        return self._cooldown

    def acquire(self, key: str, owner: str, ttl: int) -> bool:
        now = time()
        last = self._last_acquired.get(key)
        if last is not None and (now - last) < self._cooldown:
            return False
        result = self._inner.acquire(key, owner, ttl)
        if result:
            self._last_acquired[key] = now
        return result

    def release(self, key: str, owner: str) -> bool:
        released = self._inner.release(key, owner)
        if released:
            self._last_acquired.pop(key, None)
        return released

    def is_locked(self, key: str) -> bool:
        return self._inner.is_locked(key)

    def refresh(self, key: str, owner: str, ttl: int) -> bool:
        return self._inner.refresh(key, owner, ttl)

    def remaining_cooldown(self, key: str) -> float:
        """Return the remaining cooldown time in seconds for the given key.

        Returns 0.0 if the key is not in the cooldown window.
        """
        last = self._last_acquired.get(key)
        if last is None:
            return 0.0
        remaining = self._cooldown - (time() - last)
        return max(0.0, remaining)
