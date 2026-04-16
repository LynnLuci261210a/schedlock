from schedlock.backends.base import BaseBackend


class NamespacedBackend(BaseBackend):
    """Wraps a backend and prepends a namespace to all lock keys."""

    def __init__(self, inner: BaseBackend, namespace: str):
        if not isinstance(inner, BaseBackend):
            raise TypeError("inner must be a BaseBackend instance")
        if not namespace or not namespace.strip():
            raise ValueError("namespace must be a non-empty string")
        self._inner = inner
        self._namespace = namespace.strip()

    @property
    def namespace(self) -> str:
        return self._namespace

    def _ns(self, key: str) -> str:
        return f"{self._namespace}:{key}"

    def acquire(self, key: str, owner: str, ttl: int) -> bool:
        return self._inner.acquire(self._ns(key), owner, ttl)

    def release(self, key: str, owner: str) -> bool:
        return self._inner.release(self._ns(key), owner)

    def is_locked(self, key: str) -> bool:
        return self._inner.is_locked(self._ns(key))

    def refresh(self, key: str, owner: str, ttl: int) -> bool:
        return self._inner.refresh(self._ns(key), owner, ttl)
