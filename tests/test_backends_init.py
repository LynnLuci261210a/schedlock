"""Tests for schedlock.backends package exports."""

import pytest
from schedlock import backends
from schedlock.backends.base import BaseBackend


def test_base_backend_importable():
    from schedlock.backends import BaseBackend
    assert BaseBackend is not None


def test_file_backend_importable():
    from schedlock.backends import FileBackend
    assert FileBackend is not None


def test_memory_backend_importable():
    from schedlock.backends import MemoryBackend
    assert MemoryBackend is not None


def test_memory_backend_is_base_subclass():
    from schedlock.backends import MemoryBackend
    assert issubclass(MemoryBackend, BaseBackend)


def test_file_backend_is_base_subclass():
    from schedlock.backends import FileBackend
    assert issubclass(FileBackend, BaseBackend)


def test_composite_backend_importable():
    from schedlock.backends import CompositeBackend
    assert CompositeBackend is not None


def test_audited_backend_importable():
    from schedlock.backends import AuditedBackend
    assert AuditedBackend is not None


def test_readonly_backend_importable():
    from schedlock.backends import ReadOnlyBackend
    assert ReadOnlyBackend is not None


def test_readonly_backend_is_base_subclass():
    from schedlock.backends import ReadOnlyBackend
    assert issubclass(ReadOnlyBackend, BaseBackend)


def test_all_exports_list_contains_readonly():
    from schedlock.backends import __all__
    assert "ReadOnlyBackend" in __all__
