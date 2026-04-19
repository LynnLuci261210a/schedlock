from schedlock.backends.base import BaseBackend


class TaggingPolicyBackend(BaseBackend):
    """Wraps a backend and enforces a tagging policy on lock keys.

    A tagging policy is a callable that receives the lock key and returns
    a dict of tags (str -> str).  The tags are stored per-key and can be
    inspected via ``tags_for``.  The policy is called on every successful
    acquire so that tags always reflect the latest acquisition.
    """

    def __init__(self, inner: BaseBackend, policy):
        if not isinstance(inner, BaseBackend):
            raise TypeError("inner must be a BaseBackend instance")
        if not callable(policy):
            raise TypeError("policy must be callable")
        self._inner = inner
        self._policy = policy
        self._tags: dict[str, dict[str, str]] = {}

    @property
    def inner(self) -> BaseBackend:
        return self._inner

    def tags_for(self, key: str) -> dict[str, str]:
        """Return the most-recently applied tags for *key*, or {}."""
        return dict(self._tags.get(key, {}))

    def acquire(self, key: str, owner: str, ttl: int) -> bool:
        acquired = self._inner.acquire(key, owner, ttl)
        if acquired:
            try:
                result = self._policy(key)
                if isinstance(result, dict):
                    self._tags[key] = {str(k): str(v) for k, v in result.items()}
            except Exception:
                pass
        return acquired

    def release(self, key: str, owner: str) -> bool:
        released = self._inner.release(key, owner)
        if released:
            self._tags.pop(key, None)
        return released

    def is_locked(self, key: str) -> bool:
        return self._inner.is_locked(key)

    def refresh(self, key: str, owner: str, ttl: int) -> bool:
        return self._inner.refresh(key, owner, ttl)
