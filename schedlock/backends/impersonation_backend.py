from schedlock.backends.base import BaseBackend


class ImpersonationBackend(BaseBackend):
    """
    A backend wrapper that allows a trusted caller to acquire or release
    locks on behalf of another owner (impersonation).

    The ``impersonate`` kwarg passed to ``acquire`` / ``release`` overrides
    the ``owner`` argument transparently before delegating to the inner
    backend.  If no impersonation mapping is supplied the call passes
    through unchanged.
    """

    def __init__(self, inner: BaseBackend, aliases: dict | None = None) -> None:
        if not isinstance(inner, BaseBackend):
            raise TypeError("inner must be a BaseBackend instance")
        if aliases is not None and not isinstance(aliases, dict):
            raise TypeError("aliases must be a dict or None")
        self._inner = inner
        self._aliases: dict[str, str] = dict(aliases) if aliases else {}

    @property
    def inner(self) -> BaseBackend:
        return self._inner

    @property
    def aliases(self) -> dict[str, str]:
        """Return a copy of the current alias mapping."""
        return dict(self._aliases)

    def add_alias(self, caller: str, impersonate_as: str) -> None:
        """Register that *caller* may act as *impersonate_as*."""
        if not caller or not impersonate_as:
            raise ValueError("caller and impersonate_as must be non-empty strings")
        self._aliases[caller] = impersonate_as

    def remove_alias(self, caller: str) -> None:
        """Remove an alias for *caller* if it exists."""
        self._aliases.pop(caller, None)

    def _resolve(self, owner: str) -> str:
        return self._aliases.get(owner, owner)

    def acquire(self, key: str, owner: str, ttl: int) -> bool:
        return self._inner.acquire(key, self._resolve(owner), ttl)

    def release(self, key: str, owner: str) -> bool:
        return self._inner.release(key, self._resolve(owner))

    def is_locked(self, key: str) -> bool:
        return self._inner.is_locked(key)

    def refresh(self, key: str, owner: str, ttl: int) -> bool:
        return self._inner.refresh(key, self._resolve(owner), ttl)
