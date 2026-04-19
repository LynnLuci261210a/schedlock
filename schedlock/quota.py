import time
from dataclasses import dataclass, field
from typing import List


@dataclass
class QuotaEntry:
    timestamp: float = field(default_factory=time.monotonic)


class LockQuota:
    """Tracks acquire attempts per key and enforces a rolling window quota."""

    def __init__(self, max_acquires: int, window: float):
        if max_acquires < 1:
            raise ValueError("max_acquires must be >= 1")
        if window <= 0:
            raise ValueError("window must be positive")
        self._max = max_acquires
        self._window = window
        self._entries: dict[str, List[QuotaEntry]] = {}

    def _prune(self, key: str) -> None:
        now = time.monotonic()
        self._entries[key] = [
            e for e in self._entries.get(key, []) if now - e.timestamp < self._window
        ]

    def allowed(self, key: str) -> bool:
        self._prune(key)
        return len(self._entries.get(key, [])) < self._max

    def record(self, key: str) -> None:
        self._prune(key)
        self._entries.setdefault(key, []).append(QuotaEntry())

    def count(self, key: str) -> int:
        self._prune(key)
        return len(self._entries.get(key, []))

    def reset(self, key: str) -> None:
        self._entries.pop(key, None)
