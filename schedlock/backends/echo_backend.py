"""EchoBackend — a decorator backend that calls a callback on every operation.

Useful for debugging, monitoring, or testing pipeline integrations.
"""
from __future__ import annotations

from typing import Callable, Optional

from schedlock.backends.base import BaseBackend


class EchoBackend(BaseBackend):
    """Wraps an inner backend and echoes every acquire/release/is_locked call
    to a user-supplied callback.

    The callback receives a single string describing the operation and its
    outcome, e.g.::

        def my_echo(msg: str) -> None:
            print(msg)

        backend = EchoBackend(inner, callback=my_echo)
    """

    def __init__(
        self,
        inner: BaseBackend,
        *,
        callback: Callable[[str], None],
    ) -> None:
        if not isinstance(inner, BaseBackend):
            raise TypeError("inner must be a BaseBackend instance")
        if not callable(callback):
            raise TypeError("callback must be callable")
        self._inner = inner
        self._callback = callback

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
        result = self._inner.acquire(key, owner, ttl)
        status = "acquired" if result else "blocked"
        self._callback(f"acquire key={key!r} owner={owner!r} ttl={ttl} -> {status}")
        return result

    def release(self, key: str, owner: str) -> bool:
        result = self._inner.release(key, owner)
        status = "released" if result else "not_held"
        self._callback(f"release key={key!r} owner={owner!r} -> {status}")
        return result

    def is_locked(self, key: str) -> bool:
        result = self._inner.is_locked(key)
        self._callback(f"is_locked key={key!r} -> {result}")
        return result

    def refresh(self, key: str, owner: str, ttl: int) -> bool:
        result = self._inner.refresh(key, owner, ttl)
        status = "refreshed" if result else "not_held"
        self._callback(f"refresh key={key!r} owner={owner!r} ttl={ttl} -> {status}")
        return result
