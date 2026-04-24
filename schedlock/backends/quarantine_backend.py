from __future__ import annotations

import time
from typing import Optional

from schedlock.backends.base import BaseBackend


class QuarantineBackend(BaseBackend):
    """Wraps a backend and temporarily quarantines (blocks) owners that fail
    to release a lock within a grace window after expiry.

    Once an owner is quarantined, all their acquire attempts are rejected for
    ``quarantine_seconds``.  Quarantine is lifted automatically when the
    penalty period elapses.
    """

    def __init__(
        self,
        inner: BaseBackend,
        *,
        quarantine_seconds: float = 60.0,
    ) -> None:
        if not isinstance(inner, BaseBackend):
            raise TypeError("inner must be a BaseBackend instance")
        if not isinstance(quarantine_seconds, (int, float)) or quarantine_seconds <= 0:
            raise ValueError("quarantine_seconds must be a positive number")

        self._inner = inner
        self._quarantine_seconds = quarantine_seconds
        # owner -> expiry timestamp of quarantine
        self._quarantined: dict[str, float] = {}

    @property
    def inner(self) -> BaseBackend:
        return self._inner

    @property
    def quarantine_seconds(self) -> float:
        return self._quarantine_seconds

    def _is_quarantined(self, owner: str) -> bool:
        expiry = self._quarantined.get(owner)
        if expiry is None:
            return False
        if time.monotonic() >= expiry:
            del self._quarantined[owner]
            return False
        return True

    def quarantine(self, owner: str) -> None:
        """Manually quarantine *owner* for ``quarantine_seconds``."""
        self._quarantined[owner] = time.monotonic() + self._quarantine_seconds

    def is_quarantined(self, owner: str) -> bool:
        """Return True if *owner* is currently quarantined."""
        return self._is_quarantined(owner)

    def acquire(self, key: str, owner: str, ttl: int) -> bool:
        if self._is_quarantined(owner):
            return False
        return self._inner.acquire(key, owner, ttl)

    def release(self, key: str, owner: str) -> bool:
        return self._inner.release(key, owner)

    def is_locked(self, key: str) -> bool:
        return self._inner.is_locked(key)

    def refresh(self, key: str, owner: str, ttl: int) -> bool:
        return self._inner.refresh(key, owner, ttl)
