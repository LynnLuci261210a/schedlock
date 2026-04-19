"""Tests for schedlock.backends package-level imports."""
from __future__ import annotations

import pytest


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
    from schedlock.backends import BaseBackend, MemoryBackend
    assert issubclass(MemoryBackend, BaseBackend)


def test_file_backend_is_base_subclass():
    from schedlock.backends import BaseBackend, FileBackend
    assert issubclass(FileBackend, BaseBackend)


def test_redis_backend_importable():
    from schedlock.backends import RedisBackend
    assert RedisBackend is not None


def test_deadline_backend_importable():
    from schedlock.backends import DeadlineBackend
    assert DeadlineBackend is not None


def test_deadline_backend_is_base_subclass():
    from schedlock.backends import BaseBackend, DeadlineBackend
    assert issubclass(DeadlineBackend, BaseBackend)


def test_all_exports_present():
    import schedlock.backends as pkg
    for name in pkg.__all__:
        assert hasattr(pkg, name), f"{name} missing from package"
