"""EncryptedBackend — wraps an inner backend and encrypts lock owner values."""

from __future__ import annotations

import base64
import hashlib
from typing import Optional

from schedlock.backends.base import BaseBackend


class EncryptedBackend(BaseBackend):
    """A backend wrapper that obfuscates owner identifiers using a secret key.

    The owner string is HMAC-hashed with the secret so that raw owner values
    are never stored in the underlying backend (useful for privacy-sensitive
    deployments).
    """

    def __init__(self, inner: BaseBackend, secret: str) -> None:
        if not isinstance(inner, BaseBackend):
            raise TypeError("inner must be a BaseBackend instance")
        if not secret or not isinstance(secret, str):
            raise ValueError("secret must be a non-empty string")
        self._inner = inner
        self._secret = secret.encode()

    @property
    def inner(self) -> BaseBackend:
        return self._inner

    def _encrypt_owner(self, owner: str) -> str:
        digest = hashlib.blake2b(
            owner.encode(), key=self._secret[:64], digest_size=16
        ).digest()
        return base64.urlsafe_b64encode(digest).decode()

    def acquire(self, key: str, owner: str, ttl: int) -> bool:
        return self._inner.acquire(key, self._encrypt_owner(owner), ttl)

    def release(self, key: str, owner: str) -> bool:
        return self._inner.release(key, self._encrypt_owner(owner))

    def is_locked(self, key: str) -> bool:
        return self._inner.is_locked(key)

    def refresh(self, key: str, owner: str, ttl: int) -> bool:
        return self._inner.refresh(key, self._encrypt_owner(owner), ttl)

    def get_owner(self, key: str) -> Optional[str]:
        """Returns the encrypted owner stored in the inner backend (opaque)."""
        if hasattr(self._inner, "get_owner"):
            return self._inner.get_owner(key)
        return None
