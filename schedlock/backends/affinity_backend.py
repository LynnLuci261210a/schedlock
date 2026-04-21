"""Affinity backend — routes acquire/release to a preferred backend based on owner.

The AffinityBackend maintains a mapping from owner identifiers to specific
backend instances. When a lock is acquired, the backend associated with the
owner (if any) is tried first; if no affinity is registered or the preferred
backend fails, the request falls through to the default inner backend.

This is useful in multi-region or multi-shard deployments where certain
workers should preferentially use a local backend for latency reasons.
"""

from __future__ import annotations

from typing import Dict, Optional

from schedlock.backends.base import BaseBackend


class AffinityBackend(BaseBackend):
    """Routes lock operations to a preferred backend based on owner affinity.

    Parameters
    ----------
    inner:
        Default backend used when no affinity is registered for an owner.
    affinities:
        Optional initial mapping of ``owner -> backend``. Each value must be
        a :class:`~schedlock.backends.base.BaseBackend` instance.
    """

    def __init__(
        self,
        inner: BaseBackend,
        affinities: Optional[Dict[str, BaseBackend]] = None,
    ) -> None:
        if not isinstance(inner, BaseBackend):
            raise TypeError("inner must be a BaseBackend instance")
        if affinities is not None:
            if not isinstance(affinities, dict):
                raise TypeError("affinities must be a dict mapping owner to BaseBackend")
            for owner, backend in affinities.items():
                if not isinstance(owner, str) or not owner:
                    raise ValueError("affinity keys must be non-empty strings")
                if not isinstance(backend, BaseBackend):
                    raise TypeError(
                        f"affinity value for owner '{owner}' must be a BaseBackend instance"
                    )
        self._inner = inner
        self._affinities: Dict[str, BaseBackend] = dict(affinities) if affinities else {}

    # ------------------------------------------------------------------
    # Properties
    # ------------------------------------------------------------------

    @property
    def inner(self) -> BaseBackend:
        """The default backend used when no affinity is registered."""
        return self._inner

    @property
    def affinities(self) -> Dict[str, BaseBackend]:
        """A shallow copy of the current owner-to-backend affinity map."""
        return dict(self._affinities)

    # ------------------------------------------------------------------
    # Affinity management
    # ------------------------------------------------------------------

    def set_affinity(self, owner: str, backend: BaseBackend) -> None:
        """Register or update the preferred backend for *owner*."""
        if not isinstance(owner, str) or not owner:
            raise ValueError("owner must be a non-empty string")
        if not isinstance(backend, BaseBackend):
            raise TypeError("backend must be a BaseBackend instance")
        self._affinities[owner] = backend

    def clear_affinity(self, owner: str) -> None:
        """Remove any registered affinity for *owner* (no-op if absent)."""
        self._affinities.pop(owner, None)

    def _backend_for(self, owner: str) -> BaseBackend:
        """Return the preferred backend for *owner*, falling back to inner."""
        return self._affinities.get(owner, self._inner)

    # ------------------------------------------------------------------
    # BaseBackend interface
    # ------------------------------------------------------------------

    def acquire(self, key: str, owner: str, ttl: int) -> bool:
        """Acquire *key* using the backend preferred for *owner*."""
        return self._backend_for(owner).acquire(key, owner, ttl)

    def release(self, key: str, owner: str) -> bool:
        """Release *key* on the backend preferred for *owner*."""
        return self._backend_for(owner).release(key, owner)

    def is_locked(self, key: str) -> bool:
        """Check whether *key* is locked on the default inner backend.

        Affinity is owner-scoped; without an owner we check the default
        backend only.
        """
        return self._inner.is_locked(key)

    def refresh(self, key: str, owner: str, ttl: int) -> bool:
        """Refresh the TTL for *key* on the backend preferred for *owner*."""
        return self._backend_for(owner).refresh(key, owner, ttl)
