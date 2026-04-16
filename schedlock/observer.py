"""Lock event observer for monitoring lock lifecycle events."""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import Callable, List, Optional

logger = logging.getLogger(__name__)


@dataclass
class LockEvent:
    event_type: str  # 'acquired', 'released', 'failed', 'expired'
    lock_name: str
    owner: str
    backend: str = "unknown"
    extra: dict = field(default_factory=dict)


EventHandler = Callable[[LockEvent], None]


class LockObserver:
    """Collects and dispatches lock lifecycle events to registered handlers."""

    def __init__(self) -> None:
        self._handlers: List[EventHandler] = []

    def subscribe(self, handler: EventHandler) -> None:
        """Register a callable to receive LockEvent notifications."""
        self._handlers.append(handler)

    def unsubscribe(self, handler: EventHandler) -> None:
        """Remove a previously registered handler."""
        self._handlers = [h for h in self._handlers if h is not handler]

    def notify(self, event: LockEvent) -> None:
        """Dispatch event to all registered handlers."""
        for handler in self._handlers:
            try:
                handler(event)
            except Exception as exc:  # pragma: no cover
                logger.warning("LockObserver handler raised an error: %s", exc)

    def emit(self, event_type: str, lock_name: str, owner: str,
             backend: str = "unknown", **extra) -> None:
        """Convenience method to build and dispatch a LockEvent."""
        event = LockEvent(
            event_type=event_type,
            lock_name=lock_name,
            owner=owner,
            backend=backend,
            extra=extra,
        )
        self.notify(event)


# Module-level default observer instance
default_observer = LockObserver()
