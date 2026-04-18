"""ObservableBackend — a wrapper that emits LockEvents via a LockObserver."""

from schedlock.backends.base import BaseBackend
from schedlock.observer import LockObserver, LockEvent


class ObservableBackend(BaseBackend):
    """Wraps any backend and fires observer events on acquire/release/is_locked.

    Example::

        observer = LockObserver()
        observer.subscribe(lambda e: print(e))

        backend = ObservableBackend(MemoryBackend(), observer=observer)
        backend.acquire("my-job", "worker-1", ttl=60)
    """

    def __init__(self, inner: BaseBackend, *, observer: LockObserver) -> None:
        if not isinstance(inner, BaseBackend):
            raise TypeError("inner must be a BaseBackend instance")
        if not isinstance(observer, LockObserver):
            raise TypeError("observer must be a LockObserver instance")
        self._inner = inner
        self._observer = observer

    @property
    def inner(self) -> BaseBackend:
        """The wrapped backend."""
        return self._inner

    @property
    def observer(self) -> LockObserver:
        """The attached observer."""
        return self._observer

    def acquire(self, key: str, owner: str, ttl: int) -> bool:
        """Acquire the lock and emit an 'acquired' or 'failed' event."""
        result = self._inner.acquire(key, owner, ttl)
        event_type = "acquired" if result else "failed"
        self._observer.notify(LockEvent(type=event_type, key=key, owner=owner, ttl=ttl))
        return result

    def release(self, key: str, owner: str) -> bool:
        """Release the lock and emit a 'released' or 'release_failed' event."""
        result = self._inner.release(key, owner)
        event_type = "released" if result else "release_failed"
        self._observer.notify(LockEvent(type=event_type, key=key, owner=owner, ttl=None))
        return result

    def is_locked(self, key: str) -> bool:
        """Check lock state and emit a 'checked' event."""
        result = self._inner.is_locked(key)
        self._observer.notify(LockEvent(type="checked", key=key, owner=None, ttl=None))
        return result

    def refresh(self, key: str, owner: str, ttl: int) -> bool:
        """Refresh the lock TTL and emit a 'refreshed' or 'refresh_failed' event."""
        result = self._inner.refresh(key, owner, ttl)
        event_type = "refreshed" if result else "refresh_failed"
        self._observer.notify(LockEvent(type=event_type, key=key, owner=owner, ttl=ttl))
        return result
