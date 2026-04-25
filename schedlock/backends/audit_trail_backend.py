"""AuditTrailBackend — wraps any backend and records a timestamped trail of
every acquire/release attempt, keyed by lock name and owner."""

from __future__ import annotations

import time
from dataclasses import dataclass, field
from typing import Dict, List, Optional

from schedlock.backends.base import BaseBackend


@dataclass
class TrailEntry:
    """A single record in the audit trail."""

    action: str          # 'acquire_ok', 'acquire_fail', 'release_ok', 'release_fail'
    key: str
    owner: str
    ttl: Optional[int]
    timestamp: float = field(default_factory=time.time)

    def __str__(self) -> str:  # pragma: no cover
        return (
            f"[{self.timestamp:.3f}] {self.action} key={self.key!r} "
            f"owner={self.owner!r} ttl={self.ttl}"
        )


class AuditTrailBackend(BaseBackend):
    """Decorator backend that maintains a full audit trail of lock operations.

    Parameters
    ----------
    inner:
        The underlying backend to delegate to.
    max_entries:
        Maximum number of trail entries to retain (oldest are dropped).
        Defaults to 1000.
    """

    def __init__(self, inner: BaseBackend, *, max_entries: int = 1000) -> None:
        if not isinstance(inner, BaseBackend):
            raise TypeError("inner must be a BaseBackend instance")
        if not isinstance(max_entries, int) or max_entries < 1:
            raise ValueError("max_entries must be a positive integer")
        self._inner = inner
        self._max_entries = max_entries
        self._trail: List[TrailEntry] = []

    # ------------------------------------------------------------------
    # Properties
    # ------------------------------------------------------------------

    @property
    def inner(self) -> BaseBackend:
        return self._inner

    @property
    def max_entries(self) -> int:
        return self._max_entries

    @property
    def trail(self) -> List[TrailEntry]:
        """Return a snapshot of the current audit trail (oldest-first)."""
        return list(self._trail)

    def trail_for(self, key: str) -> List[TrailEntry]:
        """Return trail entries for a specific lock key."""
        return [e for e in self._trail if e.key == key]

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _record(self, action: str, key: str, owner: str, ttl: Optional[int]) -> None:
        entry = TrailEntry(action=action, key=key, owner=owner, ttl=ttl)
        self._trail.append(entry)
        if len(self._trail) > self._max_entries:
            self._trail = self._trail[-self._max_entries :]

    # ------------------------------------------------------------------
    # BaseBackend interface
    # ------------------------------------------------------------------

    def acquire(self, key: str, owner: str, ttl: int) -> bool:
        ok = self._inner.acquire(key, owner, ttl)
        self._record("acquire_ok" if ok else "acquire_fail", key, owner, ttl)
        return ok

    def release(self, key: str, owner: str) -> bool:
        ok = self._inner.release(key, owner)
        self._record("release_ok" if ok else "release_fail", key, owner, None)
        return ok

    def is_locked(self, key: str) -> bool:
        return self._inner.is_locked(key)

    def refresh(self, key: str, owner: str, ttl: int) -> bool:
        return self._inner.refresh(key, owner, ttl)
