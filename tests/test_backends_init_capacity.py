from schedlock.backends import CapacityBackend
from schedlock.backends.base import BaseBackend
from schedlock.backends.capacity_backend import CapacityBackend as DirectImport


def test_capacity_backend_importable():
    assert CapacityBackend is not None


def test_capacity_backend_is_base_subclass():
    assert issubclass(CapacityBackend, BaseBackend)


def test_capacity_backend_in_all():
    import schedlock.backends as pkg
    assert "CapacityBackend" in pkg.__all__


def test_direct_import_matches_package_import():
    assert CapacityBackend is DirectImport
