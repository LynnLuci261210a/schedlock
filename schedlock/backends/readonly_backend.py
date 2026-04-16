"""Read-only backend wrapper that prevents acquire/release operations."""

from schedlock.backends.base import BaseBackend


class ReadOnlyBackend(BaseBackend):
    """Wraps a backend and disables write operations.

    Useful for inspection, monitoring, or dry-run scenarios where
    you want to check lock state without modifying it.
    """

    def __init__(self, inner: BaseBackend):
        if inner is None:
            raise ValueError("inner backend must not be None")
        self._inner = inner

    def acquire(self, job_name: str, ttl: int, owner: str = None) -> bool:
        raise PermissionError("ReadOnlyBackend does not allow acquire operations")

    def release(self, job_name: str, owner: str) -> bool:
        raise PermissionError("ReadOnlyBackend does not allow release operations")

    def is_locked(self, job_name: str) -> bool:
        return self._inner.is_locked(job_name)

    def refresh(self, job_name: str, owner: str, ttl: int) -> bool:
        raise PermissionError("ReadOnlyBackend does not allow refresh operations")

    def get_owner(self, job_name: str):
        """Return current owner if the inner backend supports it."""
        if hasattr(self._inner, "get_owner"):
            return self._inner.get_owner(job_name)
        return None
