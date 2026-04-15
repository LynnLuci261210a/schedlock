"""Utility helpers for schedlock."""

import socket
import os
import hashlib
import time
from typing import Optional


def default_owner() -> str:
    """Generate a default owner identifier using hostname and PID.

    Returns a string in the format ``<hostname>:<pid>`` which is
    reasonably unique across distributed workers.
    """
    hostname = socket.gethostname()
    pid = os.getpid()
    return f"{hostname}:{pid}"


def make_lock_key(job_name: str, prefix: str = "schedlock") -> str:
    """Build a canonical lock key for the given job name.

    Args:
        job_name: The name of the job (e.g. function name or custom label).
        prefix: Optional namespace prefix for the key.

    Returns:
        A colon-separated string suitable for use as a Redis key or
        file-system-safe lock identifier.
    """
    safe_name = job_name.replace(" ", "_")
    return f"{prefix}:{safe_name}"


def ttl_to_expiry(ttl: int) -> float:
    """Convert a TTL in seconds to an absolute expiry timestamp.

    Args:
        ttl: Time-to-live in seconds.

    Returns:
        Unix timestamp (float) representing when the lock should expire.
    """
    return time.time() + ttl


def is_expired(expiry: float) -> bool:
    """Check whether an expiry timestamp has passed.

    Args:
        expiry: Unix timestamp to check against the current time.

    Returns:
        ``True`` if the current time is past *expiry*, ``False`` otherwise.
    """
    return time.time() >= expiry


def fingerprint(value: str) -> str:
    """Return a short SHA-256 hex digest of *value*.

    Useful for generating compact, deterministic identifiers.

    Args:
        value: Arbitrary string to hash.

    Returns:
        First 16 hex characters of the SHA-256 digest.
    """
    return hashlib.sha256(value.encode()).hexdigest()[:16]
