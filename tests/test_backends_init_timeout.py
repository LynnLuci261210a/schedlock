"""Verify TimeoutBackend is exported from the backends package."""
from __future__ import annotations

import pytest


def test_timeout_backend_importable():
    from schedlock.backends import TimeoutBackend  # noqa: F401


def test_timeout_backend_is_base_subclass():
    from schedlock.backends import TimeoutBackend
    from schedlock.backends.base import BaseBackend

    assert issubclass(TimeoutBackend, BaseBackend)


def test_timeout_backend_in_all():
    import schedlock.backends as pkg

    assert "TimeoutBackend" in pkg.__all__


def test_direct_import_matches_package_import():
    from schedlock.backends import TimeoutBackend as A
    from schedlock.backends.timeout_backend import TimeoutBackend as B

    assert A is B


def test_timeout_backend_instantiable():
    from schedlock.backends import TimeoutBackend
    from schedlock.backends.memory_backend import MemoryBackend

    tb = TimeoutBackend(MemoryBackend(), timeout_seconds=3.0)
    assert tb.timeout_seconds == 3.0
