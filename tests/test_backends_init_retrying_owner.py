"""Verify RetryingOwnerBackend is importable from the backends package."""
import pytest

from schedlock.backends.base import BaseBackend
from schedlock.backends.retrying_owner_backend import RetryingOwnerBackend


def test_retrying_owner_backend_importable():
    assert RetryingOwnerBackend is not None


def test_retrying_owner_backend_is_base_subclass():
    assert issubclass(RetryingOwnerBackend, BaseBackend)


def test_retrying_owner_backend_has_expected_attrs():
    from unittest.mock import MagicMock
    inner = MagicMock(spec=BaseBackend)
    b = RetryingOwnerBackend(inner, lambda: "owner", retries=1, delay=0)
    assert hasattr(b, "inner")
    assert hasattr(b, "retries")
    assert hasattr(b, "delay")
    assert hasattr(b, "acquire")
    assert hasattr(b, "release")
    assert hasattr(b, "is_locked")
    assert hasattr(b, "refresh")


def test_retrying_owner_backend_zero_retries_single_attempt():
    from unittest.mock import MagicMock
    inner = MagicMock(spec=BaseBackend)
    inner.acquire.return_value = False
    b = RetryingOwnerBackend(inner, lambda: "o", retries=0, delay=0)
    result = b.acquire("key", "ignored", 30)
    assert result is False
    assert inner.acquire.call_count == 1
