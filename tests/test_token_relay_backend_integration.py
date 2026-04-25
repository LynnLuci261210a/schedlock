"""Integration tests for TokenRelayBackend over MemoryBackend."""
import pytest

from schedlock.backends.memory_backend import MemoryBackend
from schedlock.backends.namespaced_backend import NamespacedBackend
from schedlock.backends.token_relay_backend import TokenRelayBackend


@pytest.fixture()
def memory():
    return MemoryBackend()


def test_full_lifecycle(memory):
    token = "svc-account-token"
    backend = TokenRelayBackend(memory, token_fn=lambda: token)

    acquired = backend.acquire("job", "anything", 60)
    assert acquired is True
    assert memory.is_locked("job")

    released = backend.release("job", "anything")
    assert released is True
    assert not memory.is_locked("job")


def test_second_acquire_blocked_while_first_holds(memory):
    backend = TokenRelayBackend(memory, token_fn=lambda: "worker-1")
    backend.acquire("job", "x", 60)

    backend2 = TokenRelayBackend(memory, token_fn=lambda: "worker-2")
    result = backend2.acquire("job", "y", 60)
    assert result is False


def test_same_token_can_release_own_lock(memory):
    token = "shared-token"
    backend = TokenRelayBackend(memory, token_fn=lambda: token)
    backend.acquire("job", "ignored", 30)
    assert backend.release("job", "ignored") is True
    assert not memory.is_locked("job")


def test_different_token_cannot_release(memory):
    token_holder = "holder"
    backend_a = TokenRelayBackend(memory, token_fn=lambda: token_holder)
    backend_a.acquire("job", "x", 30)

    backend_b = TokenRelayBackend(memory, token_fn=lambda: "intruder")
    released = backend_b.release("job", "x")
    assert released is False
    assert memory.is_locked("job")


def test_token_relay_over_namespaced(memory):
    ns = NamespacedBackend(memory, namespace="svc")
    backend = TokenRelayBackend(ns, token_fn=lambda: "ns-token")

    assert backend.acquire("task", "ignored", 30) is True
    assert ns.is_locked("task")
    assert backend.release("task", "ignored") is True
    assert not ns.is_locked("task")
