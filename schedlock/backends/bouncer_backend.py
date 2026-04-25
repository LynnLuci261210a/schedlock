from __future__ import annotations

from typing import Callable, Optional

from schedlock.backends.base import BaseBackend


class BouncerBackend(BaseBackend):
    """Delegates acquire/release to inner, but calls a bouncer function
    before each acquire.  If the bouncer returns False the acquire is
    denied without touching the inner backend.

    Args:
        inner:    Wrapped backend.
        bouncer:  ``(key, owner) -> bool``.  Return True to allow.
        reason:   Human-readable explanation used in repr / logs.
    """

    def __init__(
        self,
        inner: BaseBackend,
        bouncer: Callable[[str, str], bool],
        reason: str = "bouncer denied",
    ) -> None:
        if not isinstance(inner, BaseBackend):
            raise TypeError("inner must be a BaseBackend instance")
        if not callable(bouncer):
            raise TypeError("bouncer must be callable")
        if not isinstance(reason, str) or not reason.strip():
            raise ValueError("reason must be a non-empty string")

        self._inner = inner
        self._bouncer = bouncer
        self._reason = reason

    # ------------------------------------------------------------------
    # Properties
    # ------------------------------------------------------------------

    @property
    def inner(self) -> BaseBackend:
        return self._inner

    @property
    def reason(self) -> str:
        return self._reason

    # ------------------------------------------------------------------
    # BaseBackend interface
    # ------------------------------------------------------------------

    def acquire(self, key: str, owner: str, ttl: Optional[int] = None) -> bool:
        if not self._bouncer(key, owner):
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
            f"BouncerBackend(inner={self._inner!r}, reason={self._reason!r})"
        )
