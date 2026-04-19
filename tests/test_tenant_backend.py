import pytest
from unittest.mock import MagicMock
from schedlock.backends.tenant_backend import TenantBackend
from schedlock.backends.memory_backend import MemoryBackend
from schedlock.backends.base import BaseBackend


@pytest.fixture
def inner():
    return MagicMock(spec=BaseBackend)


@pytest.fixture
def backend(inner):
    return TenantBackend(inner, "acme")


def test_requires_inner_backend():
    with pytest.raises(TypeError):
        TenantBackend("not-a-backend", "acme")


def test_requires_non_empty_tenant_id(inner):
    with pytest.raises(ValueError):
        TenantBackend(inner, "")
    with pytest.raises(ValueError):
        TenantBackend(inner, "   ")


def test_inner_property(inner, backend):
    assert backend.inner is inner


def test_tenant_id_property(inner):
    b = TenantBackend(inner, "  corp  ")
    assert b.tenant_id == "corp"


def test_acquire_scopes_key(inner, backend):
    inner.acquire.return_value = True
    result = backend.acquire("job1", "worker-1", 30)
    inner.acquire.assert_called_once_with("tenant:acme:job1", "worker-1", 30)
    assert result is True


def test_release_scopes_key(inner, backend):
    inner.release.return_value = True
    result = backend.release("job1", "worker-1")
    inner.release.assert_called_once_with("tenant:acme:job1", "worker-1")
    assert result is True


def test_is_locked_scopes_key(inner, backend):
    inner.is_locked.return_value = False
    result = backend.is_locked("job1")
    inner.is_locked.assert_called_once_with("tenant:acme:job1")
    assert result is False


def test_refresh_scopes_key(inner, backend):
    inner.refresh.return_value = True
    result = backend.refresh("job1", "worker-1", 60)
    inner.refresh.assert_called_once_with("tenant:acme:job1", "worker-1", 60)
    assert result is True


def test_tenants_are_isolated():
    memory = MemoryBackend()
    tenant_a = TenantBackend(memory, "alpha")
    tenant_b = TenantBackend(memory, "beta")

    assert tenant_a.acquire("deploy", "worker", 60) is True
    assert tenant_a.is_locked("deploy") is True
    assert tenant_b.is_locked("deploy") is False
    assert tenant_b.acquire("deploy", "worker", 60) is True


def test_release_only_affects_own_tenant():
    memory = MemoryBackend()
    tenant_a = TenantBackend(memory, "alpha")
    tenant_b = TenantBackend(memory, "beta")

    tenant_a.acquire("job", "w1", 60)
    tenant_b.acquire("job", "w2", 60)

    tenant_a.release("job", "w1")
    assert tenant_a.is_locked("job") is False
    assert tenant_b.is_locked("job") is True
