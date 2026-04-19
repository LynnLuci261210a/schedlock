"""ImmutableBackend — a wrapper that prevents releases once a lock is acquired.

Useful for one-shot jobs where the lock must persist until TTL expiry.
"""

from __future__ import annotations

from schedlock.backends.base import BaseBackend


class ImmutableBackend(BaseBackend):
    """Wraps an inner backend and disallows explicit releases.

    Once a lock is acquired it can only be freed by TTL expiry on the
    underlying backend.  Calls to ``release`` raise ``PermissionError``.
    """

    def __init__(self, inner: BaseBackend) -> None:
        if not isinstance(inner, BaseBackend):
            raise TypeError("inner must be a BaseBackend instance")
        self._inner = inner

    # ------------------------------------------------------------------
    # Properties
    # ------------------------------------------------------------------

    @property
    def inner(self) -> BaseBackend:
        """The wrapped backend."""
        return self._inner

    # ------------------------------------------------------------------
    # BaseBackend interface
    # ------------------------------------------------------------------

    def acquire(self, key: str, owner: str, ttl: int) -> bool:
        return self._inner.acquire(key, owner, ttl)

    def release(self, key: str, owner: str) -> bool:  # type: ignore[override]
        raise PermissionError(
            f"ImmutableBackend: release is not permitted for key '{key}'. "
            "The lock will expire automatically via TTL."
        )

    def is_locked(self, key: str) -> bool:
        return self._inner.is_locked(key)

    def refresh(self, key: str, owner: str, ttl: int) -> bool:
        return self._inner.refresh(key, owner, ttl)
