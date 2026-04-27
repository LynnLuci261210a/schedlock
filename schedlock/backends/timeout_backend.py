"""TimeoutBackend — wraps an inner backend and enforces a maximum wait
time when attempting to acquire a lock. If the inner backend does not
acquire within *timeout_seconds* the attempt is abandoned and False is
returned.
"""
from __future__ import annotations

import threading
from typing import Optional

from schedlock.backends.base import BaseBackend


class TimeoutBackend(BaseBackend):
    """Acquire with a wall-clock timeout.

    The acquire call is executed in a background thread.  If it has not
    completed within *timeout_seconds* the thread is abandoned (Python
    threads cannot be forcibly killed, but the caller receives False
    immediately) and the result is discarded.

    Args:
        inner: Delegate backend.
        timeout_seconds: Maximum seconds to wait for ``inner.acquire``.
    """

    def __init__(self, inner: BaseBackend, timeout_seconds: float = 5.0) -> None:
        if not isinstance(inner, BaseBackend):
            raise TypeError("inner must be a BaseBackend instance")
        if not isinstance(timeout_seconds, (int, float)) or timeout_seconds <= 0:
            raise ValueError("timeout_seconds must be a positive number")
        self._inner = inner
        self._timeout = float(timeout_seconds)

    # ------------------------------------------------------------------
    # Properties
    # ------------------------------------------------------------------

    @property
    def inner(self) -> BaseBackend:
        return self._inner

    @property
    def timeout_seconds(self) -> float:
        return self._timeout

    # ------------------------------------------------------------------
    # BaseBackend interface
    # ------------------------------------------------------------------

    def acquire(self, key: str, owner: str, ttl: int) -> bool:
        result: list[bool] = []

        def _attempt() -> None:
            result.append(self._inner.acquire(key, owner, ttl))

        t = threading.Thread(target=_attempt, daemon=True)
        t.start()
        t.join(timeout=self._timeout)

        if t.is_alive():
            # Timed out — thread is still running in the background.
            return False
        return bool(result and result[0])

    def release(self, key: str, owner: str) -> bool:
        return self._inner.release(key, owner)

    def is_locked(self, key: str) -> bool:
        return self._inner.is_locked(key)

    def refresh(self, key: str, owner: str, ttl: int) -> bool:
        return self._inner.refresh(key, owner, ttl)

    def __repr__(self) -> str:  # pragma: no cover
        return (
            f"TimeoutBackend(inner={self._inner!r}, "
            f"timeout_seconds={self._timeout})"
        )
