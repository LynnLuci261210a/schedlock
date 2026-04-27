"""Verify AuditBackend is importable from the backends package."""
import pytest

from schedlock.backends.audit_backend import AuditBackend
from schedlock.backends.base import BaseBackend


def test_audit_backend_importable():
    assert AuditBackend is not None


def test_audit_backend_is_base_subclass():
    assert issubclass(AuditBackend, BaseBackend)


def test_audit_backend_has_expected_attrs():
    for attr in ("acquire", "release", "is_locked", "refresh", "inner"):
        assert hasattr(AuditBackend, attr), f"Missing attribute: {attr}"


def test_audit_backend_instantiable():
    from schedlock.backends.memory_backend import MemoryBackend
    b = AuditBackend(MemoryBackend(), sink=lambda e: None)
    assert isinstance(b, BaseBackend)
