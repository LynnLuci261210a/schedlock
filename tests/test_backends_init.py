"""Tests for schedlock.backends package exports."""

import pytest
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


def test_fallback_backend_importable():
    from schedlock.backends import FallbackBackend
    assert FallbackBackend is not None


def test_fallback_backend_is_base_subclass():
    from schedlock.backends import FallbackBackend
    assert issubclass(FallbackBackend, BaseBackend)


def test_namespaced_backend_importable():
    from schedlock.backends import NamespacedBackend
    assert NamespacedBackend is not None


def test_tagged_backend_importable():
    from schedlock.backends import TaggedBackend
    assert TaggedBackend is not None


def test_ttl_cap_backend_importable():
    from schedlock.backends import TTLCapBackend
    assert TTLCapBackend is not None


def test_readonly_backend_importable():
    from schedlock.backends import ReadOnlyBackend
    assert ReadOnlyBackend is not None


def test_all_exports_are_base_subclasses():
    from schedlock.backends import (
        MemoryBackend, FileBackend, CompositeBackend,
        AuditedBackend, ReadOnlyBackend, TTLCapBackend,
        TaggedBackend, NamespacedBackend, FallbackBackend,
    )
    for cls in [MemoryBackend, FileBackend, CompositeBackend,
                AuditedBackend, ReadOnlyBackend, TTLCapBackend,
                TaggedBackend, NamespacedBackend, FallbackBackend]:
        assert issubclass(cls, BaseBackend), f"{cls.__name__} is not a BaseBackend subclass"
