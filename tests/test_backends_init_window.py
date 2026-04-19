"""Ensure WindowBackend is exported from schedlock.backends."""

from schedlock.backends import WindowBackend
from schedlock.backends.base import BaseBackend


def test_window_backend_importable():
    assert WindowBackend is not None


def test_window_backend_is_base_subclass():
    assert issubclass(WindowBackend, BaseBackend)


def test_window_backend_in_all():
    import schedlock.backends as pkg
    assert "WindowBackend" in pkg.__all__
