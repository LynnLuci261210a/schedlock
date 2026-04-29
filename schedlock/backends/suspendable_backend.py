"""SuspendableBackend — wraps an inner backend and allows temporary suspension.

While suspended, acquire() is blocked (returns False) but release() and
is_locked() still delegate to the inner backend so held locks can be
cleaned up gracefully.
"""
from __future__ import annotations

from schedlock.backends.base import BaseBackend


class SuspendableBackend(BaseBackend):
    """Decorator that blocks new acquisitions while the backend is suspended.

    Args:
        inner: The backend to wrap.
        reason: Human-readable reason shown when acquisition is blocked.
    """

    def __init__(self, inner: BaseBackend, *, reason: str = "backend suspended") -> None:
        if not isinstance(inner, BaseBackend):
            raise TypeError("inner must be a BaseBackend instance")
        if not isinstance(reason, str) or not reason.strip():
            raise ValueError("reason must be a non-empty string")
        self._inner = inner
        self._reason = reason
        self._suspended = False

    @property
    def inner(self) -> BaseBackend:
        return self._inner

    @property
    def reason(self) -> str:
        return self._reason

    @property
    def is_suspended(self) -> bool:
        """Return True if the backend is currently suspended."""
        return self._suspended

    def suspend(self) -> None:
        """Suspend the backend, blocking future acquire() calls."""
        self._suspended = True

    def resume(self) -> None:
        """Resume the backend, allowing acquire() calls again."""
        self._suspended = False

    def acquire(self, key: str, owner: str, ttl: int) -> bool:
        if self._suspended:
            return False
        return self._inner.acquire(key, owner, ttl)

    def release(self, key: str, owner: str) -> bool:
        return self._inner.release(key, owner)

    def is_locked(self, key: str) -> bool:
        return self._inner.is_locked(key)

    def refresh(self, key: str, owner: str, ttl: int) -> bool:
        if self._suspended:
            return False
        return self._inner.refresh(key, owner, ttl)

    def __repr__(self) -> str:  # pragma: no cover
        state = "suspended" if self._suspended else "active"
        return f"SuspendableBackend(inner={self._inner!r}, state={state!r})"
