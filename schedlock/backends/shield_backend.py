"""ShieldBackend — prevents lock acquisition during a protected window.

If the shield is active, acquire calls are rejected immediately.
Release and is_locked always delegate to the inner backend.
"""
from __future__ import annotations

from schedlock.backends.base import BaseBackend


class ShieldBackend(BaseBackend):
    """Wraps an inner backend and blocks acquire while the shield is raised."""

    def __init__(self, inner: BaseBackend, *, reason: str = "shielded") -> None:
        if not isinstance(inner, BaseBackend):
            raise TypeError("inner must be a BaseBackend instance")
        if not reason or not isinstance(reason, str):
            raise ValueError("reason must be a non-empty string")
        self._inner = inner
        self._reason = reason
        self._shielded: bool = False

    # ------------------------------------------------------------------
    # Properties
    # ------------------------------------------------------------------

    @property
    def inner(self) -> BaseBackend:
        return self._inner

    @property
    def reason(self) -> str:
        return self._reason

    @property
    def is_shielded(self) -> bool:
        return self._shielded

    # ------------------------------------------------------------------
    # Shield control
    # ------------------------------------------------------------------

    def raise_shield(self) -> None:
        """Activate the shield — subsequent acquire calls will be blocked."""
        self._shielded = True

    def lower_shield(self) -> None:
        """Deactivate the shield — acquire calls will be forwarded normally."""
        self._shielded = False

    # ------------------------------------------------------------------
    # BaseBackend interface
    # ------------------------------------------------------------------

    def acquire(self, key: str, owner: str, ttl: int) -> bool:
        if self._shielded:
            return False
        return self._inner.acquire(key, owner, ttl)

    def release(self, key: str, owner: str) -> bool:
        return self._inner.release(key, owner)

    def is_locked(self, key: str) -> bool:
        return self._inner.is_locked(key)

    def refresh(self, key: str, owner: str, ttl: int) -> bool:
        return self._inner.refresh(key, owner, ttl)

    def __repr__(self) -> str:  # pragma: no cover
        return (
            f"ShieldBackend(inner={self._inner!r}, "
            f"reason={self._reason!r}, shielded={self._shielded})"
        )
