"""Verify HealthBackend is exported from schedlock.backends."""
from __future__ import annotations

import pytest


def test_health_backend_importable():
    from schedlock.backends import HealthBackend  # noqa: F401


def test_health_backend_is_base_subclass():
    from schedlock.backends import HealthBackend
    from schedlock.backends.base import BaseBackend

    assert issubclass(HealthBackend, BaseBackend)


def test_health_backend_in_all():
    import schedlock.backends as pkg

    assert "HealthBackend" in pkg.__all__


def test_direct_import_matches_package_import():
    from schedlock.backends import HealthBackend as pkg_cls
    from schedlock.backends.health_backend import HealthBackend as direct_cls

    assert pkg_cls is direct_cls
