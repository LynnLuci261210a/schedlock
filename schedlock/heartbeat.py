"""Heartbeat module for refreshing lock TTL while a job is running."""

import threading
import time
import logging
from typing import Optional

logger = logging.getLogger(__name__)


class LockHeartbeat:
    """Periodically refreshes a lock's TTL to prevent expiry during long-running jobs.

    Usage:
        with LockHeartbeat(backend, lock_key, owner, ttl=60, interval=20):
            do_long_running_work()
    """

    def __init__(self, backend, lock_key: str, owner: str, ttl: int = 60, interval: Optional[int] = None):
        """
        Args:
            backend: A backend instance implementing `refresh(key, owner, ttl)`.
            lock_key: The lock key to refresh.
            owner: The owner identifier for the lock.
            ttl: TTL in seconds to renew the lock to on each heartbeat.
            interval: How often (in seconds) to send a heartbeat. Defaults to ttl // 3.
        """
        self.backend = backend
        self.lock_key = lock_key
        self.owner = owner
        self.ttl = ttl
        self.interval = interval if interval is not None else max(1, ttl // 3)
        self._stop_event = threading.Event()
        self._thread: Optional[threading.Thread] = None

    def _run(self):
        while not self._stop_event.wait(timeout=self.interval):
            try:
                refreshed = self.backend.refresh(self.lock_key, self.owner, self.ttl)
                if refreshed:
                    logger.debug("Heartbeat refreshed lock '%s' for owner '%s'", self.lock_key, self.owner)
                else:
                    logger.warning("Heartbeat failed to refresh lock '%s' — lock may have expired", self.lock_key)
            except Exception as exc:  # pragma: no cover
                logger.error("Heartbeat error for lock '%s': %s", self.lock_key, exc)

    def start(self):
        """Start the heartbeat background thread."""
        self._stop_event.clear()
        self._thread = threading.Thread(target=self._run, daemon=True, name=f"heartbeat-{self.lock_key}")
        self._thread.start()
        logger.debug("Heartbeat started for lock '%s' (interval=%ds, ttl=%ds)", self.lock_key, self.interval, self.ttl)

    def stop(self):
        """Stop the heartbeat background thread."""
        self._stop_event.set()
        if self._thread is not None:
            self._thread.join(timeout=self.interval + 1)
        logger.debug("Heartbeat stopped for lock '%s'", self.lock_key)

    def __enter__(self):
        self.start()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.stop()
        return False
