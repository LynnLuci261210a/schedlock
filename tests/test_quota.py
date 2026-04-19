import time
import pytest
from schedlock.quota import LockQuota


@pytest.fixture
def quota():
    return LockQuota(max_acquires=3, window=60.0)


def test_quota_invalid_max():
    with pytest.raises(ValueError, match="max_acquires"):
        LockQuota(max_acquires=0, window=10.0)


def test_quota_invalid_window():
    with pytest.raises(ValueError, match="window"):
        LockQuota(max_acquires=1, window=0)


def test_allowed_initially(quota):
    assert quota.allowed("job") is True


def test_record_increments_count(quota):
    quota.record("job")
    assert quota.count("job") == 1


def test_allowed_up_to_max(quota):
    for _ in range(3):
        assert quota.allowed("job") is True
        quota.record("job")
    assert quota.allowed("job") is False


def test_blocked_after_max(quota):
    for _ in range(3):
        quota.record("job")
    assert quota.allowed("job") is False


def test_reset_clears_entries(quota):
    quota.record("job")
    quota.reset("job")
    assert quota.count("job") == 0
    assert quota.allowed("job") is True


def test_independent_keys(quota):
    for _ in range(3):
        quota.record("job-a")
    assert quota.allowed("job-a") is False
    assert quota.allowed("job-b") is True


def test_prune_removes_expired_entries():
    q = LockQuota(max_acquires=2, window=0.05)
    q.record("job")
    q.record("job")
    assert q.allowed("job") is False
    time.sleep(0.1)
    assert q.allowed("job") is True


def test_count_zero_for_unknown_key(quota):
    assert quota.count("unknown") == 0


def test_reset_unknown_key_does_not_raise(quota):
    quota.reset("nope")  # should not raise


def test_window_boundary():
    q = LockQuota(max_acquires=1, window=0.05)
    q.record("k")
    assert q.allowed("k") is False
    time.sleep(0.1)
    assert q.allowed("k") is True
    q.record("k")
    assert q.count("k") == 1
