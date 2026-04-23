"""TrustingBackend — delegates acquire/release only when the owner matches a trusted pattern."""
from __future__ import annotations

import re
from typing import Callable, Optional, Union

from schedlock.backends.base import BaseBackend


class TrustingBackend(BaseBackend):
    """Wraps an inner backend and only allows operations from trusted owners.

    An owner is considered trusted when it matches *any* of the supplied
    patterns (plain strings are matched with equality; compiled patterns or
    callables are also supported).
    """

    def __init__(
        self,
        inner: BaseBackend,
        trusted: list[Union[str, re.Pattern, Callable[[str], bool]]],
    ) -> None:
        if not isinstance(inner, BaseBackend):
            raise TypeError("inner must be a BaseBackend instance")
        if not trusted:
            raise ValueError("trusted must be a non-empty list")
        self._inner = inner
        self._trusted = list(trusted)

    @property
    def inner(self) -> BaseBackend:
        return self._inner

    def _is_trusted(self, owner: str) -> bool:
        for rule in self._trusted:
            if callable(rule) and not isinstance(rule, re.Pattern):
                if rule(owner):
                    return True
            elif isinstance(rule, re.Pattern):
                if rule.search(owner):
                    return True
            else:
                if owner == rule:
                    return True
        return False

    def acquire(self, key: str, owner: str, ttl: int) -> bool:
        if not self._is_trusted(owner):
            return False
        return self._inner.acquire(key, owner, ttl)

    def release(self, key: str, owner: str) -> bool:
        if not self._is_trusted(owner):
            return False
        return self._inner.release(key, owner)

    def is_locked(self, key: str) -> bool:
        return self._inner.is_locked(key)

    def refresh(self, key: str, owner: str, ttl: int) -> bool:
        if not self._is_trusted(owner):
            return False
        return self._inner.refresh(key, owner, ttl)
