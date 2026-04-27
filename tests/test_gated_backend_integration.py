"""Integration tests for GatedBackend over a real MemoryBackend."""
import pytest

from schedlock.backends.memory_backend import MemoryBackend
from schedlock.backends.gated_backend import GatedBackend
from schedlock.backends.namespaced_backend import NamespacedBackend


@pytest.fixture()
def memory():
    return MemoryBackend()


def test_full_lifecycle_when_gate_open(memory):
    b = GatedBackend(memory, gate_fn=lambda: True, reason="always open")
    assert b.acquire("job", "worker-1", 60) is True
    assert b.is_locked("job") is True
    assert b.release("job", "worker-1") is True
    assert b.is_locked("job") is False


def test_acquire_blocked_when_gate_closed(memory):
    b = GatedBackend(memory, gate_fn=lambda: False, reason="maintenance")
    assert b.acquire("job", "worker-1", 60) is False
    assert b.is_locked("job") is False


def test_held_lock_not_releasable_when_gate_closed(memory):
    # Acquire directly on the inner backend, then try to release via gated.
    memory.acquire("job", "worker-1", 60)
    b = GatedBackend(memory, gate_fn=lambda: False, reason="closed")
    result = b.release("job", "worker-1")
    assert result is False
    # Lock is still held on the inner backend.
    assert memory.is_locked("job") is True


def test_gate_toggle_mid_workflow(memory):
    gate_open = [True]
    b = GatedBackend(memory, gate_fn=lambda: gate_open[0], reason="dynamic")

    assert b.acquire("job", "worker-1", 60) is True

    gate_open[0] = False
    # A second worker cannot acquire while gate is closed.
    assert b.acquire("job", "worker-2", 60) is False

    gate_open[0] = True
    # Original worker can now release.
    assert b.release("job", "worker-1") is True
    assert b.is_locked("job") is False


def test_gated_over_namespaced(memory):
    ns = NamespacedBackend(memory, namespace="ns")
    b = GatedBackend(ns, gate_fn=lambda: True, reason="ok")
    assert b.acquire("task", "w", 30) is True
    assert b.is_locked("task") is True
    assert b.release("task", "w") is True
