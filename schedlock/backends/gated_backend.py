from __future__ import annotations

from typing import Callable, Optional

from schedlock.backends.base import BaseBackend


class GatedBackend(BaseBackend):
    """A backend wrapper that delegates acquire/release only when a gate
    function returns True.  When the gate is closed, acquire returns False
    and release is a no-op.

    Args:
        inner: The wrapped backend.
        gate_fn: A zero-argument callable that returns a bool.  When it
            returns False the gate is considered *closed*.
        reason: Human-readable explanation shown in the ValueError when the
            gate_fn is invalid.
    """

    def __init__(
        self,
        inner: BaseBackend,
        gate_fn: Callable[[], bool],
        reason: str = "gate closed",
    ) -> None:
        if not isinstance(inner, BaseBackend):
            raise ValueError("inner must be a BaseBackend instance")
        if not callable(gate_fn):
            raise ValueError("gate_fn must be callable")
        reason = reason.strip() if isinstance(reason, str) else ""
        if not reason:
            raise ValueError("reason must be a non-empty string")
        self._inner = inner
        self._gate_fn = gate_fn
        self._reason = reason

    # ------------------------------------------------------------------
    # Properties
    # ------------------------------------------------------------------

    @property
    def inner(self) -> BaseBackend:
        return self._inner

    @property
    def reason(self) -> str:
        return self._reason

    def is_open(self) -> bool:
        """Return True when the gate currently allows operations."""
        return bool(self._gate_fn())

    # ------------------------------------------------------------------
    # BaseBackend interface
    # ------------------------------------------------------------------

    def acquire(self, key: str, owner: str, ttl: int) -> bool:
        if not self._gate_fn():
            return False
        return self._inner.acquire(key, owner, ttl)

    def release(self, key: str, owner: str) -> bool:
        if not self._gate_fn():
            return False
        return self._inner.release(key, owner)

    def is_locked(self, key: str) -> bool:
        return self._inner.is_locked(key)

    def refresh(self, key: str, owner: str, ttl: int) -> bool:
        if not self._gate_fn():
            return False
        return self._inner.refresh(key, owner, ttl)
