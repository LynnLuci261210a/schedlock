"""ExpiringOwnerBackend — rejects acquire attempts from owners whose
credentials have passed a caller-supplied expiry time."""
from __future__ import annotations

import time
from typing import Callable, Optional

from schedlock.backends.base import BaseBackend


class ExpiringOwnerBackend(BaseBackend):
    """Wraps an inner backend and refuses to acquire when the owner's
    credential has expired according to *expiry_fn*.

    Args:
        inner:     Delegate backend.
        expiry_fn: Callable(owner) -> float | None.  Returns a UNIX
                   timestamp after which the owner is considered expired,
                   or None to allow unconditionally.
    """

    def __init__(self, inner: BaseBackend, expiry_fn: Callable[[str], Optional[float]]) -> None:
        if not isinstance(inner, BaseBackend):
            raise TypeError("inner must be a BaseBackend instance")
        if not callable(expiry_fn):
            raise TypeError("expiry_fn must be callable")
        self._inner = inner
        self._expiry_fn = expiry_fn

    # ------------------------------------------------------------------
    # Properties
    # ------------------------------------------------------------------

    @property
    def inner(self) -> BaseBackend:
        return self._inner

    # ------------------------------------------------------------------
    # BaseBackend interface
    # ------------------------------------------------------------------

    def acquire(self, key: str, owner: str, ttl: int) -> bool:
        expiry = self._expiry_fn(owner)
        if expiry is not None and time.time() > expiry:
            return False
        return self._inner.acquire(key, owner, ttl)

    def release(self, key: str, owner: str) -> bool:
        return self._inner.release(key, owner)

    def is_locked(self, key: str) -> bool:
        return self._inner.is_locked(key)

    def refresh(self, key: str, owner: str, ttl: int) -> bool:
        return self._inner.refresh(key, owner, ttl)

    def __repr__(self) -> str:  # pragma: no cover
        return f"ExpiringOwnerBackend(inner={self._inner!r})"
