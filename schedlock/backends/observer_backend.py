from __future__ import annotations

from schedlock.backends.base import BaseBackend
from schedlock.observer import LockObserver, LockEvent


class ObservedBackend(BaseBackend):
    """A backend wrapper that emits LockEvents to a LockObserver on acquire/release.

    Useful for monitoring and auditing lock activity without coupling
    business logic to observation concerns.
    """

    def __init__(self, inner: BaseBackend, observer: LockObserver) -> None:
        if not isinstance(inner, BaseBackend):
            raise TypeError("inner must be a BaseBackend instance")
        if not isinstance(observer, LockObserver):
            raise TypeError("observer must be a LockObserver instance")
        self._inner = inner
        self._observer = observer

    @property
    def inner(self) -> BaseBackend:
        return self._inner

    @property
    def observer(self) -> LockObserver:
        return self._observer

    def acquire(self, key: str, owner: str, ttl: int) -> bool:
        acquired = self._inner.acquire(key, owner, ttl)
        event_type = "acquired" if acquired else "blocked"
        self._observer.notify(LockEvent(event=event_type, key=key, owner=owner, ttl=ttl))
        return acquired

    def release(self, key: str, owner: str) -> bool:
        released = self._inner.release(key, owner)
        if released:
            self._observer.notify(LockEvent(event="released", key=key, owner=owner, ttl=None))
        return released

    def is_locked(self, key: str) -> bool:
        return self._inner.is_locked(key)

    def refresh(self, key: str, owner: str, ttl: int) -> bool:
        return self._inner.refresh(key, owner, ttl)
