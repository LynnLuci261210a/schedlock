"""TimedBackend — wraps a backend and records acquire/release durations."""

from __future__ import annotations

import time
from dataclasses import dataclass, field
from typing import List, Optional

from schedlock.backends.base import BaseBackend


@dataclass
class TimingRecord:
    job_name: str
    operation: str  # 'acquire' or 'release'
    duration_ms: float
    success: bool
    timestamp: float = field(default_factory=time.time)

    def __str__(self) -> str:
        return (
            f"TimingRecord(op={self.operation}, job={self.job_name}, "
            f"duration={self.duration_ms:.2f}ms, success={self.success})"
        )


class TimedBackend(BaseBackend):
    """Wraps a backend and records timing for acquire and release operations."""

    def __init__(self, inner: BaseBackend, max_records: int = 200) -> None:
        if not isinstance(inner, BaseBackend):
            raise TypeError("inner must be a BaseBackend instance")
        if not isinstance(max_records, int) or max_records < 1:
            raise ValueError("max_records must be a positive integer")
        self._inner = inner
        self._max_records = max_records
        self._records: List[TimingRecord] = []

    @property
    def inner(self) -> BaseBackend:
        return self._inner

    @property
    def records(self) -> List[TimingRecord]:
        return list(self._records)

    def _record(self, job_name: str, operation: str, duration_ms: float, success: bool) -> None:
        entry = TimingRecord(job_name=job_name, operation=operation, duration_ms=duration_ms, success=success)
        self._records.append(entry)
        if len(self._records) > self._max_records:
            self._records = self._records[-self._max_records:]

    def acquire(self, job_name: str, owner: str, ttl: int) -> bool:
        start = time.perf_counter()
        result = self._inner.acquire(job_name, owner, ttl)
        elapsed = (time.perf_counter() - start) * 1000
        self._record(job_name, "acquire", elapsed, result)
        return result

    def release(self, job_name: str, owner: str) -> bool:
        start = time.perf_counter()
        result = self._inner.release(job_name, owner)
        elapsed = (time.perf_counter() - start) * 1000
        self._record(job_name, "release", elapsed, result)
        return result

    def is_locked(self, job_name: str) -> bool:
        return self._inner.is_locked(job_name)

    def refresh(self, job_name: str, owner: str, ttl: int) -> bool:
        return self._inner.refresh(job_name, owner, ttl)

    def average_duration_ms(self, operation: Optional[str] = None) -> float:
        records = [r for r in self._records if operation is None or r.operation == operation]
        if not records:
            return 0.0
        return sum(r.duration_ms for r in records) / len(records)
