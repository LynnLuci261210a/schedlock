from __future__ import annotations

from typing import Optional

from schedlock.backends.base import BaseBackend


class SpilloverBackend(BaseBackend):
    """
    A backend wrapper that automatically spills over to a secondary backend
    when the primary backend's active lock count reaches a configured threshold.

    Useful for overflow scenarios where a fast/cheap backend handles the common
    case and a larger/slower backend absorbs excess load.
    """

    def __init__(
        self,
        primary: BaseBackend,
        secondary: BaseBackend,
        threshold: int,
    ) -> None:
        if not isinstance(primary, BaseBackend):
            raise TypeError("primary must be a BaseBackend instance")
        if not isinstance(secondary, BaseBackend):
            raise TypeError("secondary must be a BaseBackend instance")
        if not isinstance(threshold, int) or threshold < 1:
            raise ValueError("threshold must be a positive integer")
        self._primary = primary
        self._secondary = secondary
        self._threshold = threshold
        self._primary_count: int = 0
        self._active: dict[str, str] = {}  # key -> "primary" | "secondary"

    @property
    def primary(self) -> BaseBackend:
        return self._primary

    @property
    def secondary(self) -> BaseBackend:
        return self._secondary

    @property
    def threshold(self) -> int:
        return self._threshold

    def acquire(self, key: str, owner: str, ttl: int) -> bool:
        if self._primary_count < self._threshold:
            acquired = self._primary.acquire(key, owner, ttl)
            if acquired:
                self._primary_count += 1
                self._active[f"{key}:{owner}"] = "primary"
            return acquired
        # Primary is at capacity — spill to secondary
        acquired = self._secondary.acquire(key, owner, ttl)
        if acquired:
            self._active[f"{key}:{owner}"] = "secondary"
        return acquired

    def release(self, key: str, owner: str) -> bool:
        slot = self._active.get(f"{key}:{owner}")
        if slot == "primary":
            released = self._primary.release(key, owner)
            if released:
                self._primary_count = max(0, self._primary_count - 1)
                del self._active[f"{key}:{owner}"]
            return released
        if slot == "secondary":
            released = self._secondary.release(key, owner)
            if released:
                del self._active[f"{key}:{owner}"]
            return released
        # Unknown slot — try both
        return self._primary.release(key, owner) or self._secondary.release(key, owner)

    def is_locked(self, key: str) -> bool:
        return self._primary.is_locked(key) or self._secondary.is_locked(key)

    def refresh(self, key: str, owner: str, ttl: int) -> bool:
        slot = self._active.get(f"{key}:{owner}")
        if slot == "primary":
            return self._primary.refresh(key, owner, ttl)
        if slot == "secondary":
            return self._secondary.refresh(key, owner, ttl)
        return False
