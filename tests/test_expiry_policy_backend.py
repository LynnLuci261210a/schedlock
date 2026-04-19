import pytest
from unittest.mock import MagicMock
from schedlock.backends.expiry_policy_backend import ExpiryPolicyBackend
from schedlock.backends.base import BaseBackend


@pytest.fixture
def inner():
    m = MagicMock(spec=BaseBackend)
    m.acquire.return_value = True
    m.release.return_value = True
    m.is_locked.return_value = False
    m.refresh.return_value = True
    return m


@pytest.fixture
def backend(inner):
    return ExpiryPolicyBackend(inner, policy=lambda key, owner: 30)


def test_requires_inner_backend():
    with pytest.raises(TypeError, match="inner must be a BaseBackend"):
        ExpiryPolicyBackend(object(), policy=lambda k, o: 10)


def test_requires_callable_policy(inner):
    with pytest.raises(TypeError, match="policy must be callable"):
        ExpiryPolicyBackend(inner, policy=42)


def test_inner_property(inner, backend):
    assert backend.inner is inner


def test_policy_property(inner):
    fn = lambda k, o: 60
    b = ExpiryPolicyBackend(inner, policy=fn)
    assert b.policy is fn


def test_acquire_uses_policy_ttl(inner, backend):
    result = backend.acquire("my-job", "worker-1", ttl=999)
    inner.acquire.assert_called_once_with("my-job", "worker-1", 30)
    assert result is True


def test_acquire_policy_receives_key_and_owner(inner):
    calls = []
    def policy(key, owner):
        calls.append((key, owner))
        return 45
    b = ExpiryPolicyBackend(inner, policy=policy)
    b.acquire("job-x", "owner-y", ttl=0)
    assert calls == [("job-x", "owner-y")]


def test_acquire_raises_if_policy_returns_non_positive(inner):
    b = ExpiryPolicyBackend(inner, policy=lambda k, o: 0)
    with pytest.raises(ValueError, match="positive int TTL"):
        b.acquire("k", "o", ttl=10)


def test_acquire_raises_if_policy_returns_non_int(inner):
    b = ExpiryPolicyBackend(inner, policy=lambda k, o: 3.5)
    with pytest.raises(ValueError, match="positive int TTL"):
        b.acquire("k", "o", ttl=10)


def test_release_delegates(inner, backend):
    result = backend.release("my-job", "worker-1")
    inner.release.assert_called_once_with("my-job", "worker-1")
    assert result is True


def test_is_locked_delegates(inner, backend):
    result = backend.is_locked("my-job")
    inner.is_locked.assert_called_once_with("my-job")
    assert result is False


def test_refresh_uses_policy_ttl(inner, backend):
    result = backend.refresh("my-job", "worker-1", ttl=999)
    inner.refresh.assert_called_once_with("my-job", "worker-1", 30)
    assert result is True


def test_per_key_policy(inner):
    def policy(key, owner):
        return 120 if key == "slow-job" else 10
    b = ExpiryPolicyBackend(inner, policy=policy)
    b.acquire("slow-job", "w", ttl=0)
    inner.acquire.assert_called_with("slow-job", "w", 120)
    b.acquire("fast-job", "w", ttl=0)
    inner.acquire.assert_called_with("fast-job", "w", 10)
