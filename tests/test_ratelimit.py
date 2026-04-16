"""Tests for schedlock.ratelimit."""
import time
import pytest
from schedlock.ratelimit import RateLimiter


def test_invalid_max_attempts():
    with pytest.raises(ValueError):
        RateLimiter(max_attempts=0, window=10)


def test_invalid_window():
    with pytest.raises(ValueError):
        RateLimiter(max_attempts=5, window=0)


def test_allowed_initially():
    rl = RateLimiter(max_attempts=3, window=10)
    assert rl.allowed() is True


def test_attempt_records_and_returns_true():
    rl = RateLimiter(max_attempts=3, window=10)
    assert rl.attempt() is True
    assert rl.current_count == 1


def test_attempt_blocked_after_max():
    rl = RateLimiter(max_attempts=2, window=10)
    assert rl.attempt() is True
    assert rl.attempt() is True
    assert rl.attempt() is False


def test_current_count_reflects_window():
    rl = RateLimiter(max_attempts=5, window=0.1)
    rl.attempt()
    rl.attempt()
    assert rl.current_count == 2
    time.sleep(0.15)
    assert rl.current_count == 0


def test_allowed_resets_after_window():
    rl = RateLimiter(max_attempts=1, window=0.1)
    assert rl.attempt() is True
    assert rl.attempt() is False
    time.sleep(0.15)
    assert rl.allowed() is True


def test_record_increments_count():
    rl = RateLimiter(max_attempts=10, window=10)
    rl.record()
    rl.record()
    assert rl.current_count == 2
