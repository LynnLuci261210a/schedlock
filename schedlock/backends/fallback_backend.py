"""FallbackBackend: tries a primary backend, falls back to secondary on failure."""

from schedlock.backends.base import BaseBackend


class FallbackBackend(BaseBackend):
    """Wraps two backends: uses primary, falls back to secondary on acquire failure."""

    def __init__(self, primary: BaseBackend, secondary: BaseBackend):
        if primary is None:
            raise ValueError("primary backend is required")
        if secondary is None:
            raise ValueError("secondary backend is required")
        self._primary = primary
        self._secondary = secondary
        self._active: dict[str, str] = {}  # job_name -> which backend is active

    @property
    def primary(self) -> BaseBackend:
        return self._primary

    @property
    def secondary(self) -> BaseBackend:
        return self._secondary

    def acquire(self, job_name: str, ttl: int, owner: str) -> bool:
        try:
            if self._primary.acquire(job_name, ttl, owner):
                self._active[job_name] = "primary"
                return True
        except Exception:
            pass

        try:
            if self._secondary.acquire(job_name, ttl, owner):
                self._active[job_name] = "secondary"
                return True
        except Exception:
            pass

        return False

    def release(self, job_name: str, owner: str) -> bool:
        backend_name = self._active.get(job_name)
        if backend_name == "primary":
            result = self._primary.release(job_name, owner)
        elif backend_name == "secondary":
            result = self._secondary.release(job_name, owner)
        else:
            # Try both
            result = self._primary.release(job_name, owner) or self._secondary.release(job_name, owner)
        self._active.pop(job_name, None)
        return result

    def is_locked(self, job_name: str) -> bool:
        try:
            if self._primary.is_locked(job_name):
                return True
        except Exception:
            pass
        return self._secondary.is_locked(job_name)

    def refresh(self, job_name: str, owner: str, ttl: int) -> bool:
        backend_name = self._active.get(job_name)
        if backend_name == "secondary":
            return self._secondary.refresh(job_name, owner, ttl)
        return self._primary.refresh(job_name, owner, ttl)
