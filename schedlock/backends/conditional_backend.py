from schedlock.backends.base import BaseBackend


class ConditionalBackend(BaseBackend):
    """
    A backend wrapper that only allows acquire/release when a condition
    callable returns True. Useful for maintenance windows, feature flags, etc.
    """

    def __init__(self, inner: BaseBackend, condition, reason: str = "condition not met"):
        if not isinstance(inner, BaseBackend):
            raise TypeError("inner must be a BaseBackend instance")
        if not callable(condition):
            raise TypeError("condition must be callable")
        if not isinstance(reason, str) or not reason.strip():
            raise ValueError("reason must be a non-empty string")
        self._inner = inner
        self._condition = condition
        self._reason = reason

    @property
    def inner(self) -> BaseBackend:
        return self._inner

    @property
    def reason(self) -> str:
        return self._reason

    def acquire(self, key: str, owner: str, ttl: int) -> bool:
        if not self._condition():
            return False
        return self._inner.acquire(key, owner, ttl)

    def release(self, key: str, owner: str) -> bool:
        if not self._condition():
            return False
        return self._inner.release(key, owner)

    def is_locked(self, key: str) -> bool:
        return self._inner.is_locked(key)

    def refresh(self, key: str, owner: str, ttl: int) -> bool:
        if not self._condition():
            return False
        return self._inner.refresh(key, owner, ttl)
