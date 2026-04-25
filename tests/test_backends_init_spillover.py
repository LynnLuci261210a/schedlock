import pytest
from schedlock.backends.spillover_backend import SpilloverBackend
from schedlock.backends.base import BaseBackend
from schedlock.backends.memory_backend import MemoryBackend


def test_spillover_backend_importable():
    assert SpilloverBackend is not None


def test_spillover_backend_is_base_subclass():
    assert issubclass(SpilloverBackend, BaseBackend)


def test_spillover_backend_instantiable():
    p = MemoryBackend()
    s = MemoryBackend()
    b = SpilloverBackend(p, s, threshold=3)
    assert isinstance(b, SpilloverBackend)


def test_spillover_backend_has_expected_attrs():
    p = MemoryBackend()
    s = MemoryBackend()
    b = SpilloverBackend(p, s, threshold=5)
    assert hasattr(b, "primary")
    assert hasattr(b, "secondary")
    assert hasattr(b, "threshold")
    assert hasattr(b, "acquire")
    assert hasattr(b, "release")
    assert hasattr(b, "is_locked")
    assert hasattr(b, "refresh")


def test_spillover_backend_threshold_stored():
    p = MemoryBackend()
    s = MemoryBackend()
    b = SpilloverBackend(p, s, threshold=7)
    assert b.threshold == 7
