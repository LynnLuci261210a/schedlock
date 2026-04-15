"""schedlock decorator — wraps a function with distributed lock acquisition."""

import logging
from functools import wraps
from typing import Optional

from schedlock.retry import retry_acquire

logger = logging.getLogger(__name__)


def schedlock(
    backend,
    job_name: Optional[str] = None,
    ttl: int = 60,
    owner: Optional[str] = None,
    skip_if_locked: bool = True,
    retries: int = 1,
    retry_delay: float = 1.0,
    retry_backoff: float = 2.0,
    retry_jitter: float = 0.0,
):
    """
    Decorator that acquires a distributed lock before running the wrapped function.

    Args:
        backend:        A BaseBackend instance.
        job_name:       Lock name; defaults to the wrapped function's qualified name.
        ttl:            Lock TTL in seconds.
        owner:          Optional owner string.
        skip_if_locked: If True, silently skip execution when lock is unavailable.
                        If False, raise RuntimeError.
        retries:        Number of acquisition attempts (passed to retry_acquire).
        retry_delay:    Initial delay between retries in seconds.
        retry_backoff:  Backoff multiplier for retry delay.
        retry_jitter:   Max jitter added to each retry sleep.
    """

    def decorator(func):
        lock_name = job_name or f"{func.__module__}.{func.__qualname__}"

        @wraps(func)
        def wrapper(*args, **kwargs):
            acquired = retry_acquire(
                backend,
                lock_name,
                ttl=ttl,
                owner=owner,
                retries=retries,
                delay=retry_delay,
                backoff=retry_backoff,
                jitter=retry_jitter,
            )

            if not acquired:
                if skip_if_locked:
                    logger.info(
                        "schedlock: skipping '%s' — lock held by another process.",
                        lock_name,
                    )
                    return None
                raise RuntimeError(
                    f"schedlock: could not acquire lock '{lock_name}' after {retries} attempt(s)."
                )

            try:
                return func(*args, **kwargs)
            finally:
                backend.release(lock_name, owner=owner)

        return wrapper

    return decorator
