"""CircuitRetryBackend — combines circuit-breaker semantics with automatic
retry so transient failures are retried but a persistently-broken inner
backend trips the circuit and stops hammering it.
"""
from __future__ import annotations

import time
from collections import deque
from typing import Optional

from schedlock.backends.base import BaseBackend


class CircuitRetryBackend(BaseBackend):
    """Wraps an inner backend with retry-on-failure and a circuit breaker.

    Parameters
    ----------
    inner:            Underlying backend.
    retries:          Maximum per-call retry attempts (>= 1).
    delay:            Seconds to wait between retries (>= 0).
    failure_threshold: Consecutive failures before the circuit opens.
    reset_timeout:    Seconds after which an open circuit becomes half-open.
    """

    def __init__(
        self,
        inner: BaseBackend,
        *,
        retries: int = 3,
        delay: float = 0.1,
        failure_threshold: int = 5,
        reset_timeout: float = 30.0,
    ) -> None:
        if not isinstance(inner, BaseBackend):
            raise TypeError("inner must be a BaseBackend instance")
        if retries < 1:
            raise ValueError("retries must be >= 1")
        if delay < 0:
            raise ValueError("delay must be >= 0")
        if failure_threshold < 1:
            raise ValueError("failure_threshold must be >= 1")
        if reset_timeout <= 0:
            raise ValueError("reset_timeout must be > 0")

        self._inner = inner
        self._retries = retries
        self._delay = delay
        self._failure_threshold = failure_threshold
        self._reset_timeout = reset_timeout

        self._failures: deque[float] = deque()
        self._opened_at: Optional[float] = None

    # ------------------------------------------------------------------
    # Properties
    # ------------------------------------------------------------------

    @property
    def inner(self) -> BaseBackend:
        return self._inner

    @property
    def is_open(self) -> bool:
        """True when the circuit is open (requests are being blocked)."""
        if self._opened_at is None:
            return False
        if time.monotonic() - self._opened_at >= self._reset_timeout:
            # half-open: allow one probe through
            return False
        return True

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _record_failure(self) -> None:
        now = time.monotonic()
        self._failures.append(now)
        if len(self._failures) >= self._failure_threshold:
            self._opened_at = now

    def _record_success(self) -> None:
        self._failures.clear()
        self._opened_at = None

    def _call_with_retry(self, fn, *args, **kwargs):
        """Attempt *fn* up to *retries* times, recording circuit state."""
        last_exc: Optional[Exception] = None
        for attempt in range(self._retries):
            try:
                result = fn(*args, **kwargs)
                self._record_success()
                return result
            except Exception as exc:  # noqa: BLE001
                last_exc = exc
                self._record_failure()
                if attempt < self._retries - 1 and self._delay > 0:
                    time.sleep(self._delay)
        raise last_exc  # type: ignore[misc]

    # ------------------------------------------------------------------
    # BaseBackend interface
    # ------------------------------------------------------------------

    def acquire(self, key: str, owner: str, ttl: int) -> bool:
        if self.is_open:
            return False
        try:
            return self._call_with_retry(self._inner.acquire, key, owner, ttl)
        except Exception:
            return False

    def release(self, key: str, owner: str) -> bool:
        try:
            return self._call_with_retry(self._inner.release, key, owner)
        except Exception:
            return False

    def is_locked(self, key: str) -> bool:
        if self.is_open:
            return False
        try:
            return self._call_with_retry(self._inner.is_locked, key)
        except Exception:
            return False

    def refresh(self, key: str, owner: str, ttl: int) -> bool:
        if self.is_open:
            return False
        try:
            return self._call_with_retry(self._inner.refresh, key, owner, ttl)
        except Exception:
            return False
