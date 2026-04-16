"""A backend wrapper that records audit entries for lock operations."""

from typing import Optional
from schedlock.backends.base import BaseBackend
from schedlock.audit import LockAuditLog


class AuditedBackend(BaseBackend):
    """Wraps any BaseBackend and records audit entries for each operation."""

    def __init__(self, backend: BaseBackend, audit_log: Optional[LockAuditLog] = None):
        self._backend = backend
        self.audit_log = audit_log or LockAuditLog()

    def acquire(self, lock_key: str, owner: str, ttl: int) -> bool:
        acquired = self._backend.acquire(lock_key, owner, ttl)
        event = "acquired" if acquired else "failed"
        self.audit_log.record(event, lock_key, owner, ttl=ttl)
        return acquired

    def release(self, lock_key: str, owner: str) -> bool:
        released = self._backend.release(lock_key, owner)
        if released:
            self.audit_log.record("released", lock_key, owner)
        return released

    def is_locked(self, lock_key: str) -> bool:
        return self._backend.is_locked(lock_key)

    def refresh(self, lock_key: str, owner: str, ttl: int) -> bool:
        return self._backend.refresh(lock_key, owner, ttl)

    def get_owner(self, lock_key: str) -> Optional[str]:
        return self._backend.get_owner(lock_key)
