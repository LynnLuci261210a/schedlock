"""Abstract base class for schedlock backends."""

from abc import ABC, abstractmethod
from typing import Optional


class BaseBackend(ABC):
    """Abstract base class that all schedlock backends must implement.

    A backend is responsible for acquiring and releasing distributed locks,
    as well as checking the current lock status for a given job name.
    """

    @abstractmethod
    def acquire(self, job_name: str, ttl: int, owner: Optional[str] = None) -> bool:
        """Attempt to acquire a lock for the given job.

        Args:
            job_name: Unique identifier for the job being locked.
            ttl: Time-to-live in seconds for the lock.
            owner: Optional identifier for the lock owner. If not provided,
                   the backend should generate one.

        Returns:
            True if the lock was successfully acquired, False otherwise.
        """
        ...

    @abstractmethod
    def release(self, job_name: str, owner: str) -> bool:
        """Release a lock held by the specified owner.

        Args:
            job_name: Unique identifier for the job whose lock should be released.
            owner: Identifier of the owner attempting to release the lock.
                   Only the owner that acquired the lock may release it.

        Returns:
            True if the lock was successfully released, False if the lock
            did not exist or was owned by a different owner.
        """
        ...

    @abstractmethod
    def is_locked(self, job_name: str) -> bool:
        """Check whether a lock currently exists for the given job.

        Args:
            job_name: Unique identifier for the job to check.

        Returns:
            True if the job is currently locked, False otherwise.
        """
        ...

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}()"
