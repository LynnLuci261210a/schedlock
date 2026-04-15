import functools
import socket
import uuid
from typing import Optional, Union

from schedlock.backends.file_backend import FileBackend
from schedlock.backends.redis_backend import RedisBackend

Backend = Union[FileBackend, RedisBackend]


def schedlock(
    job_name: str,
    backend: Backend,
    ttl: int = 60,
    owner: Optional[str] = None,
    skip_if_locked: bool = True,
):
    """
    Decorator that wraps a function with distributed locking.

    Args:
        job_name: Unique name for the job / lock key.
        backend: A FileBackend or RedisBackend instance.
        ttl: Lock time-to-live in seconds.
        owner: Optional owner identifier; defaults to hostname + uuid.
        skip_if_locked: If True, silently skip execution when lock is held.
                        If False, raise RuntimeError instead.
    """

    def decorator(func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            resolved_owner = owner or f"{socket.gethostname()}-{uuid.uuid4().hex}"
            acquired = backend.acquire(job_name, ttl=ttl, owner=resolved_owner)

            if not acquired:
                if skip_if_locked:
                    return None
                raise RuntimeError(
 for job '{job_name}'. "
                    "Another instance may be running."
                )

            try:
                return func(*args, **kwargs)
            finally:
                backend.release(job_name, owner=resolved_owner)

        return wrapper

    return decorator
