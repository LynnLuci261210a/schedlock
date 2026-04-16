"""Rate limiting support for lock acquisition attempts."""
from dataclasses import dataclass, field
from threading import Lock
from time import time
from typing import List


@dataclass
class RateLimitEntry:
    timestamp: float


class RateLimiter:
    """Sliding window rate limiter for lock acquisition attempts."""

    def __init__(self, max_attempts: int, window: float):
        if max_attempts < 1:
            raise ValueError("max_attempts must be >= 1")
        if window <= 0:
            raise ValueError("window must be > 0")
        self.max_attempts = max_attempts
        self.window = window
        self._entries: List[RateLimitEntry] = []
        self._lock = Lock()

    def _prune(self, now: float) -> None:
        cutoff = now - self.window
        self._entries = [e for e in self._entries if e.timestamp > cutoff]

    def allowed(self) -> bool:
        """Return True if a new attempt is within the rate limit."""
        with self._lock:
            now = time()
            self._prune(now)
            return len(self._entries) < self.max_attempts

    def record(self) -> None:
        """Record an attempt."""
        with self._lock:
            self._entries.append(RateLimitEntry(timestamp=time()))

    def attempt(self) -> bool:
        """Check and record in one step. Returns True if allowed."""
        with self._lock:
            now = time()
            self._prune(now)
            if len(self._entries) < self.max_attempts:
                self._entries.append(RateLimitEntry(timestamp=now))
                return True
            return False

    @property
    def current_count(self) -> int:
        with self._lock:
            self._prune(time())
            return len(self._entries)
