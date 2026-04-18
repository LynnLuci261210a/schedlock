"""ShadowBackend — mirrors acquire/release to a secondary backend for testing/comparison."""

from schedlock.backends.base import BaseBackend


class ShadowBackend(BaseBackend):
    """Delegates all operations to the primary backend, but also silently mirrors
    them to a secondary (shadow) backend.  Errors from the shadow are suppressed
    and optionally logged so they never affect the primary result."""

    def __init__(self, primary: BaseBackend, shadow: BaseBackend, *, logger=None):
        if not isinstance(primary, BaseBackend):
            raise TypeError("primary must be a BaseBackend instance")
        if not isinstance(shadow, BaseBackend):
            raise TypeError("shadow must be a BaseBackend instance")
        self._primary = primary
        self._shadow = shadow
        self._logger = logger

    @property
    def inner(self) -> BaseBackend:
        return self._primary

    @property
    def shadow(self) -> BaseBackend:
        return self._shadow

    def _mirror(self, method: str, *args, **kwargs) -> None:
        try:
            getattr(self._shadow, method)(*args, **kwargs)
        except Exception as exc:  # noqa: BLE001
            if self._logger:
                self._logger.warning("ShadowBackend mirror error [%s]: %s", method, exc)

    def acquire(self, key: str, owner: str, ttl: int) -> bool:
        result = self._primary.acquire(key, owner, ttl)
        self._mirror("acquire", key, owner, ttl)
        return result

    def release(self, key: str, owner: str) -> bool:
        result = self._primary.release(key, owner)
        self._mirror("release", key, owner)
        return result

    def is_locked(self, key: str) -> bool:
        return self._primary.is_locked(key)

    def refresh(self, key: str, owner: str, ttl: int) -> bool:
        result = self._primary.refresh(key, owner, ttl)
        self._mirror("refresh", key, owner, ttl)
        return result
