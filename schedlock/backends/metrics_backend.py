from schedlock.backends.base import BaseBackend


class MetricsBackend(BaseBackend):
    """Wraps a backend and tracks acquire/release counts and timings."""

    def __init__(self, inner: BaseBackend):
        if not isinstance(inner, BaseBackend):
            raise TypeError("inner must be a BaseBackend instance")
        self._inner = inner
        self._acquire_attempts = 0
        self._acquire_successes = 0
        self._acquire_failures = 0
        self._release_attempts = 0
        self._release_successes = 0

    @property
    def inner(self) -> BaseBackend:
        return self._inner

    def acquire(self, key: str, owner: str, ttl: int) -> bool:
        self._acquire_attempts += 1
        result = self._inner.acquire(key, owner, ttl)
        if result:
            self._acquire_successes += 1
        else:
            self._acquire_failures += 1
        return result

    def release(self, key: str, owner: str) -> bool:
        self._release_attempts += 1
        result = self._inner.release(key, owner)
        if result:
            self._release_successes += 1
        return result

    def is_locked(self, key: str) -> bool:
        return self._inner.is_locked(key)

    def refresh(self, key: str, owner: str, ttl: int) -> bool:
        return self._inner.refresh(key, owner, ttl)

    def stats(self) -> dict:
        return {
            "acquire_attempts": self._acquire_attempts,
            "acquire_successes": self._acquire_successes,
            "acquire_failures": self._acquire_failures,
            "release_attempts": self._release_attempts,
            "release_successes": self._release_successes,
        }

    def reset_stats(self) -> None:
        self._acquire_attempts = 0
        self._acquire_successes = 0
        self._acquire_failures = 0
        self._release_attempts = 0
        self._release_successes = 0
