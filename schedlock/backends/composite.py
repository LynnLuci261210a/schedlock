"""CompositeBackend: writes to ALL backends, reads from primary."""

from __future__ import annotations

import logging
from typing import List, Optional

from schedlock.backends.base import BaseBackend

logger = logging.getLogger(__name__)


class CompositeBackend(BaseBackend):
    """Mirrors lock operations across multiple backends.

    Acquire succeeds only if ALL backends succeed.
    Release is attempted on all backends regardless.
    is_locked uses the primary (first) backend.
    """

    def __init__(self, backends: List[BaseBackend]) -> None:
        if not backends:
            raise ValueError("CompositeBackend requires at least one backend.")
        self._backends = backends

    def acquire(self, job_name: str, ttl: int, owner: Optional[str] = None) -> bool:
        acquired: List[BaseBackend] = []
        for backend in self._backends:
            if backend.acquire(job_name, ttl, owner):
                acquired.append(backend)
            else:
                # Roll back already acquired locks
                for done in acquired:
                    try:
                        done.release(job_name, owner or "")
                    except Exception as exc:  # noqa: BLE001
                        logger.warning("Rollback failed on %r: %s", done, exc)
                return False
        return True

    def release(self, job_name: str, owner: str) -> bool:
        results = []
        for backend in self._backends:
            try:
                results.append(backend.release(job_name, owner))
            except Exception as exc:  # noqa: BLE001
                logger.warning("Release failed on %r: %s", backend, exc)
                results.append(False)
        return any(results)

    def is_locked(self, job_name: str) -> bool:
        return self._backends[0].is_locked(job_name)

    def refresh(self, job_name: str, owner: str, ttl: int) -> bool:
        return all(
            b.refresh(job_name, owner, ttl) for b in self._backends
        )
