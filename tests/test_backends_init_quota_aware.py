"""Verify QuotaAwareBackend is exported from schedlock.backends."""

from schedlock.backends.quota_aware_backend import QuotaAwareBackend
from schedlock.backends.base import BaseBackend


def test_quota_aware_backend_importable():
    assert QuotaAwareBackend is not None


def test_quota_aware_backend_is_base_subclass():
    assert issubclass(QuotaAwareBackend, BaseBackend)


def test_quota_aware_backend_has_expected_attrs():
    attrs = ["inner", "max_per_owner", "window", "quota_used", "quota_remaining"]
    for attr in attrs:
        assert hasattr(QuotaAwareBackend, attr), f"missing attribute: {attr}"
