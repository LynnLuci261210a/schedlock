from typing import Optional, Dict, Any, List
from schedlock.backends.base import BaseBackend


class TaggedBackend(BaseBackend):
    """Wraps a backend and attaches metadata tags to all lock operations."""

    def __init__(self, inner: BaseBackend, tags: Optional[Dict[str, str]] = None):
        if not isinstance(inner, BaseBackend):
            raise TypeError("inner must be a BaseBackend instance")
        self._inner = inner
        self._tags: Dict[str, str] = tags or {}
        self._lock_tags: Dict[str, Dict[str, str]] = {}

    @property
    def tags(self) -> Dict[str, str]:
        return dict(self._tags)

    def acquire(self, key: str, owner: str, ttl: int) -> bool:
        result = self._inner.acquire(key, owner, ttl)
        if result:
            self._lock_tags[key] = dict(self._tags)
        return result

    def release(self, key: str, owner: str) -> bool:
        result = self._inner.release(key, owner)
        if result:
            self._lock_tags.pop(key, None)
        return result

    def is_locked(self, key: str) -> bool:
        return self._inner.is_locked(key)

    def refresh(self, key: str, owner: str, ttl: int) -> bool:
        return self._inner.refresh(key, owner, ttl)

    def get_tags(self, key: str) -> Optional[Dict[str, str]]:
        """Return tags associated with a currently held lock."""
        return dict(self._lock_tags[key]) if key in self._lock_tags else None

    def locked_keys(self) -> List[str]:
        """Return all keys currently tracked as locked."""
        return list(self._lock_tags.keys())

    def set_tag(self, key: str, tag_key: str, tag_value: str) -> None:
        """Set or update a tag on an existing held lock.

        Args:
            key: The lock key to update.
            tag_key: The tag name to set.
            tag_value: The tag value to assign.

        Raises:
            KeyError: If the lock key is not currently tracked.
        """
        if key not in self._lock_tags:
            raise KeyError(f"No active lock tracked for key: {key!r}")
        self._lock_tags[key][tag_key] = tag_value
