"""Smoke tests: WarmupBackend is exported from schedlock.backends."""

from __future__ import annotations

from schedlock.backends import WarmupBackend
from schedlock.backends.base import BaseBackend


def test_warmup_backend_importable() -> None:
    assert WarmupBackend is not None


def test_warmup_backend_is_base_subclass() -> None:
    assert issubclass(WarmupBackend, BaseBackend)


def test_warmup_backend_in_all() -> None:
    import schedlock.backends as pkg
    assert "WarmupBackend" in pkg.__all__


def test_warmup_backend_instantiable() -> None:
    """Verify WarmupBackend can be instantiated without errors."""
    backend = WarmupBackend()
    assert isinstance(backend, WarmupBackend)
    assert isinstance(backend, BaseBackend)
