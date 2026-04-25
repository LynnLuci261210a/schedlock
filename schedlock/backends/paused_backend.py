"""PausedBackend — wraps an inner backend and blocks all acquires while paused."""

from __future__ import annotations

from schedlock.backends.base import BaseBackend


class PausedBackend(BaseBackend):
    """A backend wrapper that can be paused to block all new acquire attempts.

    While paused, ``acquire`` always returns ``False`` without consulting the
    inner backend.  ``release`` and ``is_locked`` are always delegated so that
    existing lock holders can still clean up.

    Args:
        inner: The backend to delegate real operations to.
        paused: Initial paused state (default ``False``).
    """

    def __init__(self, inner: BaseBackend, *, paused: bool = False) -> None:
        if not isinstance(inner, BaseBackend):
            raise TypeError("inner must be a BaseBackend instance")
        self._inner = inner
        self._paused = paused

    # ------------------------------------------------------------------
    # Properties
    # ------------------------------------------------------------------

    @property
    def inner(self) -> BaseBackend:
        """The wrapped backend."""
        return self._inner

    @property
    def is_paused(self) -> bool:
        """Return ``True`` when the backend is currently paused."""
        return self._paused

    # ------------------------------------------------------------------
    # Control
    # ------------------------------------------------------------------

    def pause(self) -> None:
        """Pause the backend — future ``acquire`` calls will return ``False``."""
        self._paused = True

    def resume(self) -> None:
        """Resume the backend — ``acquire`` calls are forwarded to *inner* again."""
        self._paused = False

    # ------------------------------------------------------------------
    # BaseBackend interface
    # ------------------------------------------------------------------

    def acquire(self, key: str, owner: str, ttl: int) -> bool:
        if self._paused:
            return False
        return self._inner.acquire(key, owner, ttl)

    def release(self, key: str, owner: str) -> bool:
        return self._inner.release(key, owner)

    def is_locked(self, key: str) -> bool:
        return self._inner.is_locked(key)

    def refresh(self, key: str, owner: str, ttl: int) -> bool:
        return self._inner.refresh(key, owner, ttl)
