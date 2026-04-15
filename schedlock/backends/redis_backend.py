import time
import uuid
from typing import Optional

try:
    import redis
except ImportError:
    redis = None  # type: ignore


class RedisBackend:
    """
    Distributed lock backend using Redis.

    Uses SET NX PX for atomic lock acquisition and Lua scripts
    for safe release (only owner can release).
    """

    RELEASE_SCRIPT = """
    if redis.call('get', KEYS[1]) == ARGV[1] then
        return redis.call('del', KEYS[1])
    else
        return 0
    end
    """

    def __init__(self, host: str = "localhost", port: int = 6379, db: int = 0,
                 key_prefix: str = "schedlock:", client=None):
        if redis is None and client is None:
            raise ImportError(
                "redis-py is required for RedisBackend. "
                "Install it with: pip install redis"
            )
        self.key_prefix = key_prefix
        self._client = client or redis.Redis(host=host, port=port, db=db,
                                             decode_responses=True)
        self._release_script = self._client.register_script(self.RELEASE_SCRIPT)

    def _lock_key(self, job_name: str) -> str:
        return f"{self.key_prefix}{job_name}"

    def acquire(self, job_name: str, ttl: int, owner: Optional[str] = None) -> Optional[str]:
        """
        Attempt to acquire the lock for job_name.

        :param job_name: Unique identifier for the job.
        :param ttl: Lock time-to-live in seconds.
        :param owner: Optional owner token; generated if not provided.
        :return: Owner token string if acquired, None otherwise.
        """
        owner = owner or str(uuid.uuid4())
        key = self._lock_key(job_name)
        acquired = self._client.set(key, owner, nx=True, ex=ttl)
        return owner if acquired else None

    def release(self, job_name: str, owner: str) -> bool:
        """
        Release the lock only if the caller is the owner.

        :param job_name: Unique identifier for the job.
        :param owner: Owner token returned by acquire().
        :return: True if released, False if not owner or not found.
        """
        key = self._lock_key(job_name)
        result = self._release_script(keys=[key], args=[owner])
        return bool(result)

    def is_locked(self, job_name: str) -> bool:
        """Return True if the lock currently exists."""
        return self._client.exists(self._lock_key(job_name)) == 1

    def ttl(self, job_name: str) -> int:
        """Return remaining TTL in seconds, or -2 if key does not exist."""
        return self._client.ttl(self._lock_key(job_name))
