from schedlock.backends.base import BaseBackend
from collections import deque
import time


class ThrottledBackend(BaseBackend):
    """
    Wraps a backend and throttles acquire calls per job key.
    Limits how many acquires can succeed within a given time window.
    """

    def __init__(self, inner: BaseBackend, max_acquires: int, window: float):
        if not isinstance(inner, BaseBackend):
            raise TypeError("inner must be a BaseBackend instance")
        if max_acquires < 1:
            raise ValueError("max_acquires must be at least 1")
        if window <= 0:
            raise ValueError("window must be positive")
        self._inner = inner
        self._max_acquires = max_acquires
        self._window = window
        self._history: dict[str, deque] = {}

    @property
    def inner(self) -> BaseBackend:
        return self._inner

    @property
    def max_acquires(self) -> int:
        return self._max_acquires

    @property
    def window(self) -> float:
        return self._window

    def _prune(self, key: str) -> None:
        now = time.monotonic()
        if key in self._history:
            dq = self._history[key]
            while dq and now - dq[0] > self._window:
                dq.popleft()

    def _throttled(self, key: str) -> bool:
        self._prune(key)
        return len(self._history.get(key, [])) >= self._max_acquires

    def _record(self, key: str) -> None:
        if key not in self._history:
            self._history[key] = deque()
        self._history[key].append(time.monotonic())

    def acquire(self, key: str, owner: str, ttl: int) -> bool:
        if self._throttled(key):
            return False
        result = self._inner.acquire(key, owner, ttl)
        if result:
            self._record(key)
        return result

    def release(self, key: str, owner: str) -> bool:
        return self._inner.release(key, owner)

    def is_locked(self, key: str) -> bool:
        return self._inner.is_locked(key)

    def refresh(self, key: str, owner: str, ttl: int) -> bool:
        return self._inner.refresh(key, owner, ttl)
