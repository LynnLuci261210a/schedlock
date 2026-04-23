"""In-memory backend for distributed job locking (useful for testing)."""

import time
import threading
from typing import Optional

from schedlock.backends.base import BaseBackend


class MemoryBackend(BaseBackend):
    """A thread-safe in-memory lock backend.

    Intended primarily for testing and local development.
    Not suitable for distributed environments.
    """

    _store: dict = {}
    _lock: threading.Lock = threading.Lock()

    def __init__(self):
        self._store = {}
        self._lock = threading.Lock()

    def acquire(self, job_name: str, ttl: int, owner: Optional[str] = None) -> bool:
        """Attempt to acquire a lock for the given job.

        Args:
            job_name: Unique identifier for the job.
            ttl: Time-to-live in seconds for the lock.
            owner: Optional identifier for the lock owner.

        Returns:
            True if the lock was acquired, False otherwise.
        """
        owner = owner or self._default_owner()
        now = time.time()

        with self._lock:
            entry = self._store.get(job_name)
            if entry is not None:
                if now < entry["expires_at"]:
                    return False
            self._store[job_name] = {
                "owner": owner,
                "expires_at": now + ttl,
            }
            return True

    def release(self, job_name: str, owner: Optional[str] = None) -> bool:
        """Release a lock if owned by the given owner.

        Args:
            job_name: Unique identifier for the job.
            owner: Optional identifier for the lock owner.

        Returns:
            True if the lock was released, False otherwise.
        """
        owner = owner or self._default_owner()

        with self._lock:
            entry = self._store.get(job_name)
            if entry is None:
                return False
            if entry["owner"] != owner:
                return False
            del self._store[job_name]
            return True

    def is_locked(self, job_name: str) -> bool:
        """Check whether a job is currently locked.

        Args:
            job_name: Unique identifier for the job.

        Returns:
            True if the lock exists and has not expired.
        """
        now = time.time()
        with self._lock:
            entry = self._store.get(job_name)
            return entry is not None and now < entry["expires_at"]

    def get_lock_info(self, job_name: str) -> Optional[dict]:
        """Return metadata about an active lock, or None if not locked.

        Args:
            job_name: Unique identifier for the job.

        Returns:
            A dict with 'owner' and 'expires_at' keys if the lock is active,
            or None if the lock does not exist or has expired.
        """
        now = time.time()
        with self._lock:
            entry = self._store.get(job_name)
            if entry is None or now >= entry["expires_at"]:
                return None
            return {
                "owner": entry["owner"],
                "expires_at": entry["expires_at"],
                "ttl_remaining": entry["expires_at"] - now,
            }

    def _default_owner(self) -> str:
        import socket
        import os
        return f"{socket.gethostname()}-{os.getpid()}"

    def __repr__(self) -> str:
        return f"MemoryBackend(locks={list(self._store.keys())})"
