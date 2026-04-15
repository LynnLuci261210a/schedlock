"""Retry utilities for schedlock — poll-based lock acquisition with backoff."""

import time
import logging
from typing import Optional

logger = logging.getLogger(__name__)


def retry_acquire(
    backend,
    job_name: str,
    ttl: int,
    owner: Optional[str] = None,
    retries: int = 3,
    delay: float = 1.0,
    backoff: float = 2.0,
    jitter: float = 0.0,
) -> bool:
    """
    Attempt to acquire a lock with retry logic and optional exponential backoff.

    Args:
        backend:   A BaseBackend instance.
        job_name:  The lock/job name.
        ttl:       Lock time-to-live in seconds.
        owner:     Optional owner identifier.
        retries:   Total number of attempts (including the first).
        delay:     Initial delay between attempts in seconds.
        backoff:   Multiplier applied to delay after each failure.
        jitter:    Max random jitter added to each delay (0 disables).

    Returns:
        True if the lock was acquired within the allowed attempts, False otherwise.
    """
    import random

    attempt = 0
    current_delay = delay

    while attempt < retries:
        acquired = backend.acquire(job_name, ttl=ttl, owner=owner)
        if acquired:
            logger.debug(
                "schedlock: lock '%s' acquired on attempt %d", job_name, attempt + 1
            )
            return True

        attempt += 1
        if attempt < retries:
            sleep_time = current_delay
            if jitter > 0:
                sleep_time += random.uniform(0, jitter)
            logger.debug(
                "schedlock: lock '%s' not acquired, retrying in %.2fs (attempt %d/%d)",
                job_name,
                sleep_time,
                attempt + 1,
                retries,
            )
            time.sleep(sleep_time)
            current_delay *= backoff

    logger.debug(
        "schedlock: lock '%s' could not be acquired after %d attempts", job_name, retries
    )
    return False
