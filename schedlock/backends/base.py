"""Abstract base class for schedlock backends."""

from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Optional

from schedlock.utils import default_owner


class BaseBackend(ABC):
    """Base class that all schedlock backends must implement."""

    @abstractmethod
    def acquire(self, job_name: str, ttl: int = 300, owner: Optional[str] = None) -> bool:
        """Attempt to acquire the lock for *job_name*.

        Returns True if the lock was acquired, False if it is already held.
        """

    @abstractmethod
    def release(self, job_name: str, owner: Optional[str] = None) -> bool:
        """Release the lock for *job_name*.

        Returns True if the lock was released, False if it was not held by *owner*.
        """

    @abstractmethod
    def is_locked(self, job_name: str) -> bool:
        """Return True if *job_name* is currently locked."""

    def lock(self, job_name: str, ttl: int = 300, owner: Optional[str] = None, skip_on_locked: bool = True):
        """Return a :class:`~schedlock.context.LockContext` for *job_name*.

        This is a convenience method so backends can be used directly as
        context managers::

            with backend.lock("my-job", ttl=60) as acquired:
                if acquired:
                    do_work()
        """
        from schedlock.context import LockContext

        return LockContext(
            self,
            job_name,
            ttl=ttl,
            owner=owner or default_owner(),
            skip_on_locked=skip_on_locked,
        )

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}()"
