"""Verify MaintenanceBackend is properly exported from the backends package."""
import pytest

from schedlock.backends import MaintenanceBackend
from schedlock.backends.base import BaseBackend
from schedlock.backends.maintenance_backend import MaintenanceBackend as DirectImport


def test_maintenance_backend_importable():
    assert MaintenanceBackend is not None


def test_maintenance_backend_is_base_subclass():
    assert issubclass(MaintenanceBackend, BaseBackend)


def test_maintenance_backend_in_all():
    import schedlock.backends as pkg
    assert "MaintenanceBackend" in pkg.__all__


def test_direct_import_matches_package_import():
    assert MaintenanceBackend is DirectImport


def test_maintenance_backend_has_expected_attrs():
    from schedlock.backends.memory_backend import MemoryBackend
    b = MaintenanceBackend(MemoryBackend())
    assert hasattr(b, "enter_maintenance")
    assert hasattr(b, "exit_maintenance")
    assert hasattr(b, "is_in_maintenance")
    assert hasattr(b, "reason")
    assert hasattr(b, "inner")
