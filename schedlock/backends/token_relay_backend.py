"""TokenRelayBackend — forwards a resolved token to a downstream backend.

The token is computed by a user-supplied callable and injected as the
owner string on every acquire call.  Release and is_locked delegate
transparently so the resolved token is also used there.
"""
from __future__ import annotations

from typing import Callable, Optional

from schedlock.backends.base import BaseBackend


class TokenRelayBackend(BaseBackend):
    """Resolves an owner token via a factory and relays it to *inner*.

    Parameters
    ----------
    inner:
        The wrapped backend that performs the actual locking.
    token_fn:
        A zero-argument callable that returns the owner string to use.
        Called on every :meth:`acquire` invocation so tokens can rotate.
    """

    def __init__(self, inner: BaseBackend, token_fn: Callable[[], str]) -> None:
        if not isinstance(inner, BaseBackend):
            raise TypeError("inner must be a BaseBackend instance")
        if not callable(token_fn):
            raise TypeError("token_fn must be callable")
        self._inner = inner
        self._token_fn = token_fn

    # ------------------------------------------------------------------
    # Properties
    # ------------------------------------------------------------------

    @property
    def inner(self) -> BaseBackend:
        """The wrapped backend."""
        return self._inner

    @property
    def token_fn(self) -> Callable[[], str]:
        """The token factory callable."""
        return self._token_fn

    # ------------------------------------------------------------------
    # BaseBackend interface
    # ------------------------------------------------------------------

    def acquire(self, key: str, owner: str, ttl: int) -> bool:
        """Resolve the token and forward to *inner*."""
        resolved = self._token_fn()
        if not isinstance(resolved, str) or not resolved:
            raise ValueError("token_fn must return a non-empty string")
        return self._inner.acquire(key, resolved, ttl)

    def release(self, key: str, owner: str) -> bool:
        """Resolve the token and forward to *inner*."""
        resolved = self._token_fn()
        if not isinstance(resolved, str) or not resolved:
            raise ValueError("token_fn must return a non-empty string")
        return self._inner.release(key, resolved)

    def is_locked(self, key: str) -> bool:
        """Delegate to *inner* unchanged."""
        return self._inner.is_locked(key)

    def refresh(self, key: str, owner: str, ttl: int) -> bool:
        """Resolve the token and forward to *inner*."""
        resolved = self._token_fn()
        if not isinstance(resolved, str) or not resolved:
            raise ValueError("token_fn must return a non-empty string")
        return self._inner.refresh(key, resolved, ttl)
