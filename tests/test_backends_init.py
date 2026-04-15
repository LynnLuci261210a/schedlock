"""Tests for the backends package public API."""

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
    from schedlock.backends import MemoryBackend, BaseBackend
    assert issubclass(MemoryBackend, BaseBackend)


def test_file_backend_is_base_subclass():
    from schedlock.backends import FileBackend, BaseBackend
    assert issubclass(FileBackend, BaseBackend)


def test_all_exports_defined():
    import schedlock.backends as backends
    for name in backends.__all__:
        if name == "RedisBackend":
            continue
        assert hasattr(backends, name), f"{name} missing from backends package"


def test_redis_backend_attribute_exists():
    import schedlock.backends as backends
    # RedisBackend may be None if redis is not installed, but attr must exist
    assert hasattr(backends, "RedisBackend")


def test_memory_backend_instantiable():
    from schedlock.backends import MemoryBackend
    backend = MemoryBackend()
    assert backend is not None


def test_memory_backend_acquire_and_release():
    from schedlock.backends import MemoryBackend
    backend = MemoryBackend()
    assert backend.acquire("sanity_check", ttl=30, owner="test") is True
    assert backend.is_locked("sanity_check") is True
    assert backend.release("sanity_check", owner="test") is True
    assert backend.is_locked("sanity_check") is False
