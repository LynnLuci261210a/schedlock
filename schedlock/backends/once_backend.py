"""OnceBackend — allows a lock to be acquired at most once per key, ever."""

from schedlock.backends.base import BaseBackend


class OnceBackend(BaseBackend):
    """Wraps a backend and ensures each key can only ever be acquired once.

    Once a key has been successfully acquired and then released, any future
    acquire attempt for that key will be permanently rejected.

    Useful for one-shot migration jobs or initialisation tasks.
    """

    def __init__(self, inner: BaseBackend) -> None:
        if not isinstance(inner, BaseBackend):
            raise TypeError("inner must be a BaseBackend instance")
        self._inner = inner
        self._used: set[str] = set()

    @property
    def inner(self) -> BaseBackend:
        return self._inner

    def acquire(self, key: str, owner: str, ttl: int) -> bool:
        if key in self._used:
            return False
        return self._inner.acquire(key, owner, ttl)

    def release(self, key: str, owner: str) -> bool:
        released = self._inner.release(key, owner)
        if released:
            self._used.add(key)
        return released

    def is_locked(self, key: str) -> bool:
        return self._inner.is_locked(key)

    def refresh(self, key: str, owner: str, ttl: int) -> bool:
        return self._inner.refresh(key, owner, ttl)

    def ever_acquired(self, key: str) -> bool:
        """Return True if the key has been acquired and released at least once."""
        return key in self._used
