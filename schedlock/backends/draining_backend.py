"""DrainingBackend: prevents new acquires once draining mode is activated."""

from __future__ import annotations

from schedlock.backends.base import BaseBackend


class DrainingBackend(BaseBackend):
    """Wraps a backend and blocks new acquire calls once drain() is invoked.

    Useful for graceful shutdown scenarios where in-flight jobs should be
    allowed to finish but no new locks should be granted.

    Args:
        inner: The underlying backend to delegate to.

    Raises:
        TypeError: If *inner* is not a BaseBackend instance.
    """

    def __init__(self, inner: BaseBackend) -> None:
        if not isinstance(inner, BaseBackend):
            raise TypeError("inner must be a BaseBackend instance")
        self._inner = inner
        self._draining = False

    @property
    def inner(self) -> BaseBackend:
        """The wrapped backend."""
        return self._inner

    @property
    def is_draining(self) -> bool:
        """True once drain() has been called."""
        return self._draining

    def drain(self) -> None:
        """Activate draining mode.  New acquire calls will be rejected."""
        self._draining = True

    def resume(self) -> None:
        """Deactivate draining mode, allowing new acquire calls again."""
        self._draining = False

    def acquire(self, key: str, owner: str, ttl: int) -> bool:
        """Acquire the lock unless draining mode is active.

        Returns:
            False immediately if draining; otherwise delegates to *inner*.
        """
        if self._draining:
            return False
        return self._inner.acquire(key, owner, ttl)

    def release(self, key: str, owner: str) -> bool:
        """Delegate release to the inner backend unconditionally."""
        return self._inner.release(key, owner)

    def is_locked(self, key: str) -> bool:
        """Delegate is_locked to the inner backend."""
        return self._inner.is_locked(key)

    def refresh(self, key: str, owner: str, ttl: int) -> bool:
        """Delegate refresh to the inner backend."""
        return self._inner.refresh(key, owner, ttl)
