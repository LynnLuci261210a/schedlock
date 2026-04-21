"""Verify TokenBucketBackend is exported from schedlock.backends."""
import pytest


def test_token_bucket_backend_importable():
    from schedlock.backends import TokenBucketBackend  # noqa: F401


def test_token_bucket_backend_is_base_subclass():
    from schedlock.backends import TokenBucketBackend
    from schedlock.backends.base import BaseBackend

    assert issubclass(TokenBucketBackend, BaseBackend)


def test_token_bucket_backend_in_all():
    import schedlock.backends as pkg

    assert "TokenBucketBackend" in pkg.__all__
