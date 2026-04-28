"""Tests that StickyWindowBackend is properly exported from the backends package."""

import pytest


def test_sticky_window_backend_importable():
    from schedlock.backends.sticky_window_backend import StickyWindowBackend
    assert StickyWindowBackend is not None


def test_sticky_window_backend_is_base_subclass():
    from schedlock.backends.sticky_window_backend import StickyWindowBackend
    from schedlock.backends.base import BaseBackend
    assert issubclass(StickyWindowBackend, BaseBackend)


def test_sticky_window_backend_has_expected_attrs():
    from schedlock.backends.sticky_window_backend import StickyWindowBackend
    assert hasattr(StickyWindowBackend, "acquire")
    assert hasattr(StickyWindowBackend, "release")
    assert hasattr(StickyWindowBackend, "is_locked")
    assert hasattr(StickyWindowBackend, "refresh")
    assert hasattr(StickyWindowBackend, "inner")
    assert hasattr(StickyWindowBackend, "reason")


def test_sticky_window_backend_instantiable():
    from schedlock.backends.memory_backend import MemoryBackend
    from schedlock.backends.sticky_window_backend import StickyWindowBackend
    inner = MemoryBackend()
    b = StickyWindowBackend(inner, schedule_fn=lambda: True, reason="off-hours")
    assert b.reason == "off-hours"
    assert b.inner is inner
