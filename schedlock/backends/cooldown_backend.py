import time
from schedlock.backends.base import BaseBackend


class CooldownBackend(BaseBackend):
    """
    A backend wrapper that enforces a per-key cooldown period after a lock
    is released. During the cooldown window, no new acquires are allowed.
    """

    def __init__(self, inner: BaseBackend, cooldown: float):
        if not isinstance(inner, BaseBackend):
            raise TypeError("inner must be a BaseBackend instance")
        if not isinstance(cooldown, (int, float)) or cooldown <= 0:
            raise ValueError("cooldown must be a positive number")
        self._inner = inner
        self._cooldown = cooldown
        self._released_at: dict[str, float] = {}

    @property
    def inner(self) -> BaseBackend:
        return self._inner

    @property
    def cooldown(self) -> float:
        return self._cooldown

    def _in_cooldown(self, key: str) -> bool:
        released = self._released_at.get(key)
        if released is None:
            return False
        return (time.monotonic() - released) < self._cooldown

    def acquire(self, key: str, owner: str, ttl: int) -> bool:
        if self._in_cooldown(key):
            return False
        return self._inner.acquire(key, owner, ttl)

    def release(self, key: str, owner: str) -> bool:
        result = self._inner.release(key, owner)
        if result:
            self._released_at[key] = time.monotonic()
        return result

    def is_locked(self, key: str) -> bool:
        return self._inner.is_locked(key)

    def refresh(self, key: str, owner: str, ttl: int) -> bool:
        return self._inner.refresh(key, owner, ttl)
