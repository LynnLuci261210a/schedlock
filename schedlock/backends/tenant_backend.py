from schedlock.backends.base import BaseBackend


class TenantBackend(BaseBackend):
    """
    A backend wrapper that scopes all lock keys to a specific tenant ID.
    Useful for multi-tenant applications where lock isolation per tenant is required.
    """

    def __init__(self, inner: BaseBackend, tenant_id: str) -> None:
        if not isinstance(inner, BaseBackend):
            raise TypeError("inner must be a BaseBackend instance")
        if not isinstance(tenant_id, str) or not tenant_id.strip():
            raise ValueError("tenant_id must be a non-empty string")
        self._inner = inner
        self._tenant_id = tenant_id.strip()

    @property
    def inner(self) -> BaseBackend:
        return self._inner

    @property
    def tenant_id(self) -> str:
        return self._tenant_id

    def _scoped(self, key: str) -> str:
        return f"tenant:{self._tenant_id}:{key}"

    def acquire(self, key: str, owner: str, ttl: int) -> bool:
        return self._inner.acquire(self._scoped(key), owner, ttl)

    def release(self, key: str, owner: str) -> bool:
        return self._inner.release(self._scoped(key), owner)

    def is_locked(self, key: str) -> bool:
        return self._inner.is_locked(self._scoped(key))

    def refresh(self, key: str, owner: str, ttl: int) -> bool:
        return self._inner.refresh(self._scoped(key), owner, ttl)
