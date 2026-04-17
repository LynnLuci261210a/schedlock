import logging
from schedlock.backends.base import BaseBackend

logger = logging.getLogger(__name__)


class LoggingBackend(BaseBackend):
    """Wraps a backend and logs all lock operations."""

    def __init__(self, inner: BaseBackend, level: int = logging.DEBUG):
        if not isinstance(inner, BaseBackend):
            raise TypeError("inner must be a BaseBackend instance")
        self._inner = inner
        self._level = level

    @property
    def inner(self) -> BaseBackend:
        return self._inner

    def acquire(self, key: str, owner: str, ttl: int) -> bool:
        result = self._inner.acquire(key, owner, ttl)
        if result:
            logger.log(self._level, "[schedlock] acquired lock key=%s owner=%s ttl=%s", key, owner, ttl)
        else:
            logger.log(self._level, "[schedlock] failed to acquire lock key=%s owner=%s", key, owner)
        return result

    def release(self, key: str, owner: str) -> bool:
        result = self._inner.release(key, owner)
        if result:
            logger.log(self._level, "[schedlock] released lock key=%s owner=%s", key, owner)
        else:
            logger.log(self._level, "[schedlock] release failed (not owner?) key=%s owner=%s", key, owner)
        return result

    def is_locked(self, key: str) -> bool:
        result = self._inner.is_locked(key)
        logger.log(self._level, "[schedlock] is_locked key=%s -> %s", key, result)
        return result

    def refresh(self, key: str, owner: str, ttl: int) -> bool:
        result = self._inner.refresh(key, owner, ttl)
        logger.log(self._level, "[schedlock] refresh key=%s owner=%s ttl=%s -> %s", key, owner, ttl, result)
        return result
