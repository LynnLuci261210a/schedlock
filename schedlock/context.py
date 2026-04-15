"""Context manager support for schedlock backends."""

from __future__ import annotations

from typing import Optional

from schedlock.backends.base import BaseBackend
from schedlock.utils import default_owner


class LockContext:
    """Context manager that acquires and releases a distributed lock.

    Usage::

        with LockContext(backend, "my-job", ttl=60) as acquired:
            if acquired:
                do_work()
    """

    def __init__(
        self,
        backend: BaseBackend,
        job_name: str,
        ttl: int = 300,
        owner: Optional[str] = None,
        skip_on_locked: bool = True,
    ) -> None:
        self.backend = backend
        self.job_name = job_name
        self.ttl = ttl
        self.owner = owner or default_owner()
        self.skip_on_locked = skip_on_locked
        self._acquired = False

    def __enter__(self) -> bool:
        self._acquired = self.backend.acquire(
            self.job_name, ttl=self.ttl, owner=self.owner
        )
        return self._acquired

    def __exit__(self, exc_type, exc_val, exc_tb) -> bool:
        if self._acquired:
            self.backend.release(self.job_name, owner=self.owner)
        # Do not suppress exceptions
        return False

    @property
    def acquired(self) -> bool:
        """Whether the lock was successfully acquired."""
        return self._acquired

    def __repr__(self) -> str:
        return (
            f"LockContext(job_name={self.job_name!r}, "
            f"owner={self.owner!r}, acquired={self._acquired})"
        )
