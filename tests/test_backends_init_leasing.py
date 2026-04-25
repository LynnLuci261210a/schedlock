import pytest
from schedlock.backends import LeasingBackend
from schedlock.backends.base import BaseBackend
from schedlock.backends.leasing_backend import LeasingBackend as DirectImport


def test_leasing_backend_importable():
    assert LeasingBackend is not None


def test_leasing_backend_is_base_subclass():
    assert issubclass(LeasingBackend, BaseBackend)


def test_leasing_backend_in_all():
    import schedlock.backends as pkg
    assert "LeasingBackend" in pkg.__all__


def test_direct_import_matches_package_import():
    assert LeasingBackend is DirectImport


def test_leasing_backend_instantiable():
    from schedlock.backends.memory_backend import MemoryBackend
    b = LeasingBackend(MemoryBackend(), lease_seconds=5)
    assert b.lease_seconds == 5.0


def test_leasing_backend_has_renew_method():
    assert callable(getattr(LeasingBackend, "renew", None))


def test_leasing_backend_has_lease_expires_at_method():
    assert callable(getattr(LeasingBackend, "lease_expires_at", None))
