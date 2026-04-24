"""Verify DampeningBackend is importable from the backends package."""
from __future__ import annotations

from schedlock.backends.dampening_backend import DampeningBackend
from schedlock.backends.base import BaseBackend


def test_dampening_backend_importable():
    assert DampeningBackend is not None


def test_dampening_backend_is_base_subclass():
    assert issubclass(DampeningBackend, BaseBackend)


def test_dampening_backend_has_expected_attrs():
    assert hasattr(DampeningBackend, "acquire")
    assert hasattr(DampeningBackend, "release")
    assert hasattr(DampeningBackend, "is_locked")
    assert hasattr(DampeningBackend, "refresh")
    assert hasattr(DampeningBackend, "inner")
    assert hasattr(DampeningBackend, "dampening_seconds")
