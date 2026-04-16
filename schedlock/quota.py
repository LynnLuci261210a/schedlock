"""Lock acquisition quota enforcement — limit how many times a lock can be acquired within a window."""

import time
from dataclasses import dataclass, field
from typing import Dict, List


@dataclass
class QuotaEntry:
    owner: str
    timestamp: float = field(default_factory=time.time)


class LockQuota:
    """Tracks and enforces per-lock acquisition quotas within a rolling time window."""

    def __init__(self, max_acquisitions: int, window_seconds: float):
        if max_acquisitions < 1:
            raise ValueError("max_acquisitions must be >= 1")
        if window_seconds <= 0:
            raise ValueError("window_seconds must be > 0")
        self.max_acquisitions = max_acquisitions
        self.window_seconds = window_seconds
        self._entries: Dict[str, List[QuotaEntry]] = {}

    def _prune(self, lock_name: str) -> None:
        cutoff = time.time() - self.window_seconds
        self._entries[lock_name] = [
            e for e in self._entries.get(lock_name, []) if e.timestamp >= cutoff
        ]

    def allowed(self, lock_name: str) -> bool:
        """Return True if another acquisition is allowed under the quota."""
        self._prune(lock_name)
        return len(self._entries.get(lock_name, [])) < self.max_acquisitions

    def record(self, lock_name: str, owner: str) -> None:
        """Record a successful acquisition."""
        self._prune(lock_name)
        self._entries.setdefault(lock_name, []).append(QuotaEntry(owner=owner))

    def count(self, lock_name: str) -> int:
        """Return the number of acquisitions in the current window."""
        self._prune(lock_name)
        return len(self._entries.get(lock_name, []))

    def reset(self, lock_name: str) -> None:
        """Clear all recorded acquisitions for a lock."""
        self._entries.pop(lock_name, None)
