"""Verify AgingBackend is importable from schedlock.backends."""
from schedlock.backends import AgingBackend
from schedlock.backends.base import BaseBackend


def test_aging_backend_importable():
    assert AgingBackend is not None


def test_aging_backend_is_base_subclass():
    assert issubclass(AgingBackend, BaseBackend)


def test_aging_backend_in_all():
    import schedlock.backends as pkg
    assert "AgingBackend" in pkg.__all__
