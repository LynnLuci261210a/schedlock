import pytest

from schedlock.backends import AllowlistBackend
from schedlock.backends.base import BaseBackend
from schedlock.backends.allowlist_backend import AllowlistBackend as DirectImport


def test_allowlist_backend_importable():
    assert AllowlistBackend is not None


def test_allowlist_backend_is_base_subclass():
    assert issubclass(AllowlistBackend, BaseBackend)


def test_allowlist_backend_in_all():
    import schedlock.backends as pkg
    assert "AllowlistBackend" in dir(pkg)


def test_direct_import_matches_package_import():
    assert AllowlistBackend is DirectImport


def test_allowlist_backend_has_expected_attrs():
    from schedlock.backends.memory_backend import MemoryBackend
    b = AllowlistBackend(MemoryBackend(), allowed={"owner-a"})
    assert hasattr(b, "inner")
    assert hasattr(b, "allowed")
    assert hasattr(b, "acquire")
    assert hasattr(b, "release")
    assert hasattr(b, "is_locked")
    assert hasattr(b, "refresh")
