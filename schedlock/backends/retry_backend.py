from schedlock.backends.base import BaseBackend
from schedlock.retry import retry_acquire


class RetryBackend(BaseBackend):
    """
    A backend wrapper that automatically retries acquire() using
    the retry_acquire helper before giving up.
    """

    def __init__(self, inner: BaseBackend, retries: int = 3, delay: float = 0.5, backoff: float = 2.0):
        if inner is None:
            raise ValueError("inner backend is required")
        if retries < 1:
            raise ValueError("retries must be at least 1")
        if delay < 0:
            raise ValueError("delay must be non-negative")
        if backoff < 1.0:
            raise ValueError("backoff must be >= 1.0")
        self._inner = inner
        self._retries = retries
        self._delay = delay
        self._backoff = backoff

    @property
    def inner(self) -> BaseBackend:
        return self._inner

    def acquire(self, key: str, owner: str, ttl: int) -> bool:
        return retry_acquire(
            self._inner,
            key=key,
            owner=owner,
            ttl=ttl,
            retries=self._retries,
            delay=self._delay,
            backoff=self._backoff,
        )

    def release(self, key: str, owner: str) -> bool:
        return self._inner.release(key, owner)

    def is_locked(self, key: str) -> bool:
        return self._inner.is_locked(key)

    def refresh(self, key: str, owner: str, ttl: int) -> bool:
        return self._inner.refresh(key, owner, ttl)

    def __repr__(self) -> str:
        return (
            f"RetryBackend(inner={self._inner!r}, retries={self._retries}, "
            f"delay={self._delay}, backoff={self._backoff})"
        )
