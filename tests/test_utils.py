"""Tests for schedlock.utils."""

import os
import socket
import time
from unittest.mock import patch

import pytest

from schedlock.utils import (
    default_owner,
    fingerprint,
    is_expired,
    make_lock_key,
    ttl_to_expiry,
)


def test_default_owner_contains_hostname_and_pid():
    owner = default_owner()
    hostname = socket.gethostname()
    pid = str(os.getpid())
    assert hostname in owner
    assert pid in owner


def test_default_owner_format():
    owner = default_owner()
    parts = owner.split(":")
    assert len(parts) == 2
    assert parts[1].isdigit()


def test_make_lock_key_default_prefix():
    key = make_lock_key("my_job")
    assert key == "schedlock:my_job"


def test_make_lock_key_custom_prefix():
    key = make_lock_key("cleanup", prefix="myapp")
    assert key == "myapp:cleanup"


def test_make_lock_key_spaces_replaced():
    key = make_lock_key("my job name")
    assert " " not in key
    assert "my_job_name" in key


def test_ttl_to_expiry_is_in_future():
    before = time.time()
    expiry = ttl_to_expiry(60)
    after = time.time()
    assert expiry >= before + 60
    assert expiry <= after + 60


def test_ttl_to_expiry_zero():
    expiry = ttl_to_expiry(0)
    assert expiry == pytest.approx(time.time(), abs=0.1)


def test_is_expired_past_timestamp():
    past = time.time() - 1
    assert is_expired(past) is True


def test_is_expired_future_timestamp():
    future = time.time() + 100
    assert is_expired(future) is False


def test_is_expired_exact_boundary():
    with patch("schedlock.utils.time") as mock_time:
        mock_time.time.return_value = 1000.0
        assert is_expired(1000.0) is True
        assert is_expired(1000.1) is False


def test_fingerprint_returns_16_chars():
    result = fingerprint("some-value")
    assert len(result) == 16


def test_fingerprint_is_deterministic():
    assert fingerprint("hello") == fingerprint("hello")


def test_fingerprint_differs_for_different_inputs():
    assert fingerprint("hello") != fingerprint("world")


def test_fingerprint_is_hex():
    result = fingerprint("test")
    int(result, 16)  # should not raise
