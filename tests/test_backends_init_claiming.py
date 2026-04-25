"""Verify ClaimingBackend is importable from the backends package."""
from __future__ import annotations

import pytest


def test_claiming_backend_importable():
    from schedlock.backends.claiming_backend import ClaimingBackend
    assert ClaimingBackend is not None


def test_claiming_backend_is_base_subclass():
    from schedlock.backends.base import BaseBackend
    from schedlock.backends.claiming_backend import ClaimingBackend
    assert issubclass(ClaimingBackend, BaseBackend)


def test_claiming_backend_has_expected_attrs():
    from schedlock.backends.claiming_backend import ClaimingBackend
    from schedlock.backends.memory_backend import MemoryBackend

    b = ClaimingBackend(MemoryBackend(), max_claim_seconds=10)
    assert hasattr(b, "inner")
    assert hasattr(b, "max_claim_seconds")
    assert hasattr(b, "claim_age")
    assert hasattr(b, "acquire")
    assert hasattr(b, "release")
    assert hasattr(b, "is_locked")
    assert hasattr(b, "refresh")


def test_claiming_backend_instantiable_with_defaults():
    from schedlock.backends.claiming_backend import ClaimingBackend
    from schedlock.backends.memory_backend import MemoryBackend

    b = ClaimingBackend(MemoryBackend())
    assert b.max_claim_seconds == 60.0
