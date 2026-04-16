"""Abstract base class for schedlock backends."""

from abc import ABC, abstractmethod
from contextlib import contextmanager
from typing import Optional


class BaseBackend(ABC):
    """Base class all backends must inherit from."""

    @abstractmethod
    def acquire(self, lock_key: str, owner: str, ttl: int) -> bool:
        """Attempt to acquire a lock. Returns True if successful."""
        raise NotImplementedError

    @abstractmethod
    def release(self, lock_key: str, owner: str) -> bool:
        """Release a lock owned by owner. Returns True if released."""
        raise NotImplementedError

    @abstractmethod
    def is_locked(self, lock_key: str) -> bool:
        """Return True if the lock is currently held."""
        raise NotImplementedError

    def refresh(self, lock_key: str, owner: str, ttl: int) -> bool:
        """Refresh the TTL of an existing lock owned by owner.

        Default implementation: release and re-acquire (not atomic).
        Backends should override this with an atomic version where possible.

        Returns True if the lock was successfully refreshed.
        """
        if not self.is_locked(lock_key):
            return False
        released = self.release(lock_key, owner)
        if not released:
            return False
        return self.acquire(lock_key, owner, ttl)

    def get_owner(self, lock_key: str) -> Optional[str]:
        """Return the current owner of the lock, or None if not locked.

        Backends may override this to provide owner introspection.
        The default implementation returns None unconditionally.
        """
        return None

    @contextmanager
    def lock(self, lock_key: str, owner: str, ttl: int, blocking: bool = True):
        """Context manager that acquires and releases a lock.

        Args:
            lock_key: The key identifying the lock.
            owner: A unique identifier for the lock owner.
            ttl: Time-to-live for the lock in seconds.
            blocking: If False, do not retry acquisition; yield False immediately
                if the lock cannot be acquired.

        Yields True if acquired, False otherwise.
        """
        acquired = self.acquire(lock_key, owner, ttl)
        try:
            yield acquired
        finally:
            if acquired:
                self.release(lock_key, owner)
