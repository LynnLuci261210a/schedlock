"""Audit log for lock acquisition and release events."""

import logging
import time
from dataclasses import dataclass, field
from typing import List, Optional

logger = logging.getLogger(__name__)


@dataclass
class AuditEntry:
    event: str  # 'acquired', 'released', 'failed', 'expired'
    lock_key: str
    owner: str
    timestamp: float = field(default_factory=time.time)
    ttl: Optional[int] = None
    note: Optional[str] = None

    def __str__(self) -> str:
        return (
            f"[{self.event.upper()}] key={self.lock_key} owner={self.owner} "
            f"at={self.timestamp:.3f}"
            + (f" ttl={self.ttl}" if self.ttl else "")
            + (f" note={self.note}" if self.note else "")
        )


class LockAuditLog:
    """In-memory audit log with optional logging integration."""

    def __init__(self, max_entries: int = 500, log_to_logger: bool = True):
        self._entries: List[AuditEntry] = []
        self._max_entries = max_entries
        self._log_to_logger = log_to_logger

    def record(self, event: str, lock_key: str, owner: str,
               ttl: Optional[int] = None, note: Optional[str] = None) -> AuditEntry:
        entry = AuditEntry(event=event, lock_key=lock_key, owner=owner, ttl=ttl, note=note)
        self._entries.append(entry)
        if len(self._entries) > self._max_entries:
            self._entries.pop(0)
        if self._log_to_logger:
            logger.debug(str(entry))
        return entry

    def entries(self, lock_key: Optional[str] = None, event: Optional[str] = None) -> List[AuditEntry]:
        result = self._entries
        if lock_key:
            result = [e for e in result if e.lock_key == lock_key]
        if event:
            result = [e for e in result if e.event == event]
        return list(result)

    def clear(self) -> None:
        self._entries.clear()

    def __len__(self) -> int:
        return len(self._entries)
