from __future__ import annotations
from dataclasses import dataclass, field
from typing import List, Optional
from schedlock.backends.base import BaseBackend


@dataclass
class ReplayEntry:
    operation: str  # 'acquire' or 'release'
    key: str
    owner: str
    ttl: Optional[int]
    result: bool

    def __str__(self) -> str:
        return f"ReplayEntry({self.operation} key={self.key!r} owner={self.owner!r} result={self.result})"


class ReplayBackend(BaseBackend):
    """Records all acquire/release calls and can replay them against another backend."""

    def __init__(self, inner: BaseBackend) -> None:
        if not isinstance(inner, BaseBackend):
            raise TypeError("inner must be a BaseBackend instance")
        self._inner = inner
        self._log: List[ReplayEntry] = []

    @property
    def inner(self) -> BaseBackend:
        return self._inner

    @property
    def log(self) -> List[ReplayEntry]:
        return list(self._log)

    def acquire(self, key: str, owner: str, ttl: int) -> bool:
        result = self._inner.acquire(key, owner, ttl)
        self._log.append(ReplayEntry("acquire", key, owner, ttl, result))
        return result

    def release(self, key: str, owner: str) -> bool:
        result = self._inner.release(key, owner)
        self._log.append(ReplayEntry("release", key, owner, None, result))
        return result

    def is_locked(self, key: str) -> bool:
        return self._inner.is_locked(key)

    def refresh(self, key: str, owner: str, ttl: int) -> bool:
        return self._inner.refresh(key, owner, ttl)

    def replay_onto(self, target: BaseBackend) -> List[ReplayEntry]:
        """Replay all recorded operations onto a target backend."""
        results = []
        for entry in self._log:
            if entry.operation == "acquire":
                r = target.acquire(entry.key, entry.owner, entry.ttl)
            else:
                r = target.release(entry.key, entry.owner)
            results.append(ReplayEntry(entry.operation, entry.key, entry.owner, entry.ttl, r))
        return results

    def clear_log(self) -> None:
        self._log.clear()
