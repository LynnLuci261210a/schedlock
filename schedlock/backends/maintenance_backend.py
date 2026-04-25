"""MaintenanceBackend — blocks all acquire operations during a maintenance window."""
from __future__ import annotations

import threading
from typing import Optional

from schedlock.backends.base import BaseBackend


class MaintenanceBackend(BaseBackend):
    """Wraps an inner backend and rejects acquires while maintenance mode is active.

    Release and is_locked still delegate to the inner backend so that operators
    can inspect and clean up held locks even during maintenance.
    """

    def __init__(self, inner: BaseBackend, *, reason: str = "maintenance") -> None:
        if not isinstance(inner, BaseBackend):
            raise TypeError("inner must be a BaseBackend instance")
        if not isinstance(reason, str) or not reason.strip():
            raise ValueError("reason must be a non-empty string")
        self._inner = inner
        self._reason = reason
        self._lock = threading.Lock()
        self._maintenance = False

    @property
    def inner(self) -> BaseBackend:
        return self._inner

    @property
    def reason(self) -> str:
        return self._reason

    @property
    def is_in_maintenance(self) -> bool:
        with self._lock:
            return self._maintenance

    def enter_maintenance(self) -> None:
        """Activate maintenance mode — all subsequent acquire calls will be rejected."""
        with self._lock:
            self._maintenance = True

    def exit_maintenance(self) -> None:
        """Deactivate maintenance mode — acquire calls delegate to inner again."""
        with self._lock:
            self._maintenance = False

    def acquire(self, key: str, owner: str, ttl: int) -> bool:
        with self._lock:
            if self._maintenance:
                return False
        return self._inner.acquire(key, owner, ttl)

    def release(self, key: str, owner: str) -> bool:
        return self._inner.release(key, owner)

    def is_locked(self, key: str) -> bool:
        return self._inner.is_locked(key)

    def refresh(self, key: str, owner: str, ttl: int) -> bool:
        with self._lock:
            if self._maintenance:
                return False
        return self._inner.refresh(key, owner, ttl)

    def __repr__(self) -> str:  # pragma: no cover
        return (
            f"MaintenanceBackend(inner={self._inner!r}, "
            f"reason={self._reason!r}, active={self._maintenance})"
        )
