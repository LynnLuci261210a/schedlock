"""SnapshotBackend — wraps an inner backend and records lock state snapshots."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Dict, List, Optional

from schedlock.backends.base import BaseBackend


@dataclass
class Snapshot:
    job_name: str
    owner: str
    action: str  # 'acquired', 'released', 'checked'
    result: bool
    ttl: Optional[int]
    timestamp: datetime = field(default_factory=lambda: datetime.now(timezone.utc))

    def __str__(self) -> str:
        ts = self.timestamp.isoformat()
        return f"[{ts}] {self.action} job={self.job_name} owner={self.owner} result={self.result}"


class SnapshotBackend(BaseBackend):
    """Records snapshots of lock operations for inspection and debugging."""

    def __init__(self, inner: BaseBackend, max_snapshots: int = 256) -> None:
        if not isinstance(inner, BaseBackend):
            raise TypeError("inner must be a BaseBackend instance")
        if not isinstance(max_snapshots, int) or max_snapshots < 1:
            raise ValueError("max_snapshots must be a positive integer")
        self._inner = inner
        self._max_snapshots = max_snapshots
        self._snapshots: List[Snapshot] = []

    @property
    def inner(self) -> BaseBackend:
        return self._inner

    def _record(self, action: str, job_name: str, owner: str, result: bool, ttl: Optional[int]) -> None:
        snap = Snapshot(job_name=job_name, owner=owner, action=action, result=result, ttl=ttl)
        self._snapshots.append(snap)
        if len(self._snapshots) > self._max_snapshots:
            self._snapshots.pop(0)

    def snapshots(self, job_name: Optional[str] = None) -> List[Snapshot]:
        if job_name is None:
            return list(self._snapshots)
        return [s for s in self._snapshots if s.job_name == job_name]

    def clear_snapshots(self) -> None:
        self._snapshots.clear()

    def acquire(self, job_name: str, owner: str, ttl: int) -> bool:
        result = self._inner.acquire(job_name, owner, ttl)
        self._record("acquired" if result else "blocked", job_name, owner, result, ttl)
        return result

    def release(self, job_name: str, owner: str) -> bool:
        result = self._inner.release(job_name, owner)
        self._record("released", job_name, owner, result, None)
        return result

    def is_locked(self, job_name: str) -> bool:
        result = self._inner.is_locked(job_name)
        self._record("checked", job_name, "", result, None)
        return result

    def refresh(self, job_name: str, owner: str, ttl: int) -> bool:
        return self._inner.refresh(job_name, owner, ttl)
