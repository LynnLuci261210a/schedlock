"""Backend pool for managing multiple lock backends with fallback support."""

from __future__ import annotations

import logging
from typing import List, Optional

from schedlock.backends.base import BaseBackend

logger = logging.getLogger(__name__)


class BackendPool:
    """Tries each backend in order; first successful acquire wins."""

    def __init__(self, backends: List[BaseBackend]) -> None:
        if not backends:
            raise ValueError("BackendPool requires at least one backend.")
        self._backends = backends

    @property
    def backends(self) -> List[BaseBackend]:
        return list(self._backends)

    def acquire(self, job_name: str, ttl: int, owner: Optional[str] = None) -> bool:
        for backend in self._backends:
            try:
                if backend.acquire(job_name, ttl, owner):
                    return True
            except Exception as exc:  # noqa: BLE001
                logger.warning("Backend %r failed during acquire: %s", backend, exc)
        return False

    def release(self, job_name: str, owner: str) -> bool:
        for backend in self._backends:
            try:
                if backend.release(job_name, owner):
                    return True
            except Exception as exc:  # noqa: BLE001
                logger.warning("Backend %r failed during release: %s", backend, exc)
        return False

    def is_locked(self, job_name: str) -> bool:
        for backend in self._backends:
            try:
                if backend.is_locked(job_name):
                    return True
            except Exception as exc:  # noqa: BLE001
                logger.warning("Backend %r failed during is_locked: %s", backend, exc)
        return False

    def __repr__(self) -> str:  # pragma: no cover
        return f"BackendPool(backends={self._backends!r})"
