from schedlock.backends.base import BaseBackend


class ValidatingBackend(BaseBackend):
    """Wraps a backend and validates lock keys and owner strings before delegating."""

    def __init__(self, inner: BaseBackend, max_key_length: int = 128, max_owner_length: int = 256):
        if not isinstance(inner, BaseBackend):
            raise TypeError("inner must be a BaseBackend instance")
        if not isinstance(max_key_length, int) or max_key_length <= 0:
            raise ValueError("max_key_length must be a positive integer")
        if not isinstance(max_owner_length, int) or max_owner_length <= 0:
            raise ValueError("max_owner_length must be a positive integer")
        self._inner = inner
        self._max_key_length = max_key_length
        self._max_owner_length = max_owner_length

    @property
    def inner(self) -> BaseBackend:
        return self._inner

    @property
    def max_key_length(self) -> int:
        return self._max_key_length

    @property
    def max_owner_length(self) -> int:
        return self._max_owner_length

    def _validate(self, key: str, owner: str) -> None:
        if not key or not isinstance(key, str):
            raise ValueError("lock key must be a non-empty string")
        if len(key) > self._max_key_length:
            raise ValueError(f"lock key exceeds max length of {self._max_key_length}")
        if not owner or not isinstance(owner, str):
            raise ValueError("owner must be a non-empty string")
        if len(owner) > self._max_owner_length:
            raise ValueError(f"owner exceeds max length of {self._max_owner_length}")

    def acquire(self, key: str, owner: str, ttl: int) -> bool:
        self._validate(key, owner)
        if not isinstance(ttl, int) or ttl <= 0:
            raise ValueError("ttl must be a positive integer")
        return self._inner.acquire(key, owner, ttl)

    def release(self, key: str, owner: str) -> bool:
        self._validate(key, owner)
        return self._inner.release(key, owner)

    def is_locked(self, key: str) -> bool:
        if not key or not isinstance(key, str):
            raise ValueError("lock key must be a non-empty string")
        return self._inner.is_locked(key)

    def refresh(self, key: str, owner: str, ttl: int) -> bool:
        self._validate(key, owner)
        if not isinstance(ttl, int) or ttl <= 0:
            raise ValueError("ttl must be a positive integer")
        return self._inner.refresh(key, owner, ttl)
