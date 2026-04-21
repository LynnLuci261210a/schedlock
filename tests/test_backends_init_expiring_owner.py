"""Smoke tests verifying ExpiringOwnerBackend is importable from
schedlock.backends and present in __all__."""
from __future__ import annotations

import schedlock.backends as pkg
from schedlock.backends.expiring_owner_backend import ExpiringOwnerBackend
from schedlock.backends.base import BaseBackend


def test_expiring_owner_backend_importable():
    assert ExpiringOwnerBackend is not None


def test_expiring_owner_backend_is_base_subclass():
    assert issubclass(ExpiringOwnerBackend, BaseBackend)


def test_expiring_owner_backend_in_all():
    assert "ExpiringOwnerBackend" in pkg.__all__
