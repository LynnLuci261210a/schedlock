"""BurstBackend — allows a short burst of extra acquires above a baseline rate.

During a burst window, up to `max_burst` acquires are permitted beyond the
normal per-window limit.  Once the burst allowance is exhausted it resets
only after `burst_window` seconds have elapsed with no further acquires.
"""

from __future__ import annotations

import time
from typing import Optional

from schedlock.backends.base import BaseBackend


class BurstBackend(BaseBackend):
    """Wraps an inner backend and permits short-lived acquire bursts."""

    def __init__(
        self,
        inner: BaseBackend,
        *,
        max_burst: int,
        burst_window: float,
    ) -> None:
        if not isinstance(inner, BaseBackend):
            raise TypeError("inner must be a BaseBackend instance")
        if not isinstance(max_burst, int) or max_burst < 1:
            raise ValueError("max_burst must be a positive integer")
        if not isinstance(burst_window, (int, float)) or burst_window <= 0:
            raise ValueError("burst_window must be a positive number")

        self._inner = inner
        self._max_burst = max_burst
        self._burst_window = float(burst_window)
        # per-key state: (burst_used, window_start)
        self._state: dict[str, tuple[int, float]] = {}

    @property
    def inner(self) -> BaseBackend:
        return self._inner

    @property
    def max_burst(self) -> int:
        return self._max_burst

    @property
    def burst_window(self) -> float:
        return self._burst_window

    def _burst_used(self, key: str) -> int:
        """Return current burst usage for *key*, resetting if window expired."""
        now = time.monotonic()
        used, start = self._state.get(key, (0, now))
        if now - start >= self._burst_window:
            # Window has elapsed — reset
            self._state[key] = (0, now)
            return 0
        return used

    def acquire(self, key: str, owner: str, ttl: int) -> bool:
        used = self._burst_used(key)
        if used >= self._max_burst:
            return False
        acquired = self._inner.acquire(key, owner, ttl)
        if acquired:
            current_used, start = self._state.get(key, (0, time.monotonic()))
            self._state[key] = (current_used + 1, start)
        return acquired

    def release(self, key: str, owner: str) -> bool:
        return self._inner.release(key, owner)

    def is_locked(self, key: str) -> bool:
        return self._inner.is_locked(key)

    def refresh(self, key: str, owner: str, ttl: int) -> bool:
        return self._inner.refresh(key, owner, ttl)
