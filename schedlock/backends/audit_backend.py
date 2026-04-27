"""AuditBackend — wraps any backend and records every acquire/release event
to an external sink (callable), enabling real-time streaming audit trails."""
from __future__ import annotations

from typing import Callable, Optional

from schedlock.backends.base import BaseBackend


class AuditBackend(BaseBackend):
    """Delegates all operations to *inner* and calls *sink* with a plain-dict
    event record after every acquire or release attempt.

    The sink receives a dict with keys:
        ``event``   – ``"acquire"`` or ``"release"``
        ``key``     – lock key
        ``owner``   – owner string
        ``success`` – bool
        ``ttl``     – ttl passed to acquire (None for release events)
    """

    def __init__(
        self,
        inner: BaseBackend,
        sink: Callable[[dict], None],
    ) -> None:
        if not isinstance(inner, BaseBackend):
            raise TypeError("inner must be a BaseBackend instance")
        if not callable(sink):
            raise TypeError("sink must be callable")
        self._inner = inner
        self._sink = sink

    @property
    def inner(self) -> BaseBackend:
        return self._inner

    def acquire(self, key: str, owner: str, ttl: int) -> bool:
        result = self._inner.acquire(key, owner, ttl)
        self._sink({
            "event": "acquire",
            "key": key,
            "owner": owner,
            "ttl": ttl,
            "success": result,
        })
        return result

    def release(self, key: str, owner: str) -> bool:
        result = self._inner.release(key, owner)
        self._sink({
            "event": "release",
            "key": key,
            "owner": owner,
            "ttl": None,
            "success": result,
        })
        return result

    def is_locked(self, key: str) -> bool:
        return self._inner.is_locked(key)

    def refresh(self, key: str, owner: str, ttl: int) -> bool:
        return self._inner.refresh(key, owner, ttl)
