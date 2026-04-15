import os
import time
import json
import fcntl
from typing import Optional
from schedlock.backends.base import BaseBackend


class FileBackend(BaseBackend):
    """
    File-based locking backend using exclusive file locks.
    Suitable for single-host deployments or testing.
    """

    def __init__(self, lock_dir: str = "/tmp/schedlock"):
        self.lock_dir = lock_dir
        os.makedirs(self.lock_dir, exist_ok=True)

    def _lock_path(self, job_name: str) -> str:
        safe_name = job_name.replace("/", "_").replace(" ", "_")
        return os.path.join(self.lock_dir, f"{safe_name}.lock")

    def acquire(self, job_name: str, ttl: int, owner: str) -> bool:
        """
        Attempt to acquire a lock for the given job.

        :param job_name: Unique name of the cron job.
        :param ttl: Time-to-live in seconds for the lock.
        :param owner: Identifier for the process acquiring the lock.
        :return: True if lock was acquired, False otherwise.
        """
        lock_path = self._lock_path(job_name)
        try:
            fd = open(lock_path, "a+")
            fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
            fd.seek(0)
            content = fd.read().strip()
            if content:
                try:
                    data = json.loads(content)
                    if time.time() < data.get("expires_at", 0):
                        fcntl.flock(fd, fcntl.LOCK_UN)
                        fd.close()
                        return False
                except (json.JSONDecodeError, KeyError):
                    pass

            fd.seek(0)
            fd.truncate()
            payload = {"owner": owner, "acquired_at": time.time(), "expires_at": time.time() + ttl}
            fd.write(json.dumps(payload))
            fd.flush()
            fcntl.flock(fd, fcntl.LOCK_UN)
            fd.close()
            return True
        except BlockingIOError:
            return False

    def release(self, job_name: str, owner: str) -> bool:
        """
        Release the lock if owned by the given owner.

        :param job_name: Unique name of the cron job.
        :param owner: Identifier for the process releasing the lock.
        :return: True if lock was released, False if not owner or not found.
        """
        lock_path = self._lock_path(job_name)
        if not os.path.exists(lock_path):
            return False
        try:
            with open(lock_path, "r+") as fd:
                fcntl.flock(fd, fcntl.LOCK_EX)
                content = fd.read().strip()
                if not content:
                    fcntl.flock(fd, fcntl.LOCK_UN)
                    return False
                data = json.loads(content)
                if data.get("owner") != owner:
                    fcntl.flock(fd, fcntl.LOCK_UN)
                    return False
                fd.seek(0)
                fd.truncate()
                fcntl.flock(fd, fcntl.LOCK_UN)
            return True
        except (json.JSONDecodeError, OSError):
            return False

    def is_locked(self, job_name: str) -> bool:
        lock_path = self._lock_path(job_name)
        if not os.path.exists(lock_path):
            return False
        try:
            with open(lock_path, "r") as fd:
                content = fd.read().strip()
                if not content:
                    return False
                data = json.loads(content)
                return time.time() < data.get("expires_at", 0)
        except (json.JSONDecodeError, OSError):
            return False
