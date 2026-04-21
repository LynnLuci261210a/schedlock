"""Verify FencingBackend is exported from the backends package."""
import pytest

from schedlock.backends import FencingBackend
from schedlock.backends.base import BaseBackend


def test_fencing_backend_importable():
    assert FencingBackend is not None


def test_fencing_backend_is_base_subclass():
    assert issubclass(FencingBackend, BaseBackend)


def test_fencing_backend_in_all():
    import schedlock.backends as pkg
    assert "FencingBackend" in pkg.__all__
