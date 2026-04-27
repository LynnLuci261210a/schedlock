"""Unit tests for SemaphoreBackend."""
import pytest

from schedlock.backends.memory_backend import MemoryBackend
from schedlock.backends.semaphore_backend import SemaphoreBackend


@pytest.fixture()
def inner():
    return MemoryBackend()


@pytest.fixture()
def backend(inner):
    return SemaphoreBackend(inner, max_holders=2)


def test_requires_inner_backend():
    with pytest.raises(TypeError):
        SemaphoreBackend("not-a-backend")


def test_requires_positive_max_holders(inner):
    with pytest.raises(ValueError):
        SemaphoreBackend(inner, max_holders=0)


def test_requires_integer_max_holders(inner):
    with pytest.raises(ValueError):
        SemaphoreBackend(inner, max_holders=1.5)  # type: ignore[arg-type]


def test_inner_property(inner, backend):
    assert backend.inner is inner


def test_max_holders_property(backend):
    assert backend.max_holders == 2


def test_acquire_first_holder(backend):
    assert backend.acquire("job", "owner-1", 60) is True


def test_acquire_up_to_max_holders(backend):
    assert backend.acquire("job", "owner-1", 60) is True
    assert backend.acquire("job", "owner-2", 60) is True


def test_acquire_blocked_when_at_capacity(backend):
    backend.acquire("job", "owner-1", 60)
    backend.acquire("job", "owner-2", 60)
    assert backend.acquire("job", "owner-3", 60) is False


def test_slots_available_decrements_on_acquire(backend):
    assert backend.slots_available("job") == 2
    backend.acquire("job", "owner-1", 60)
    assert backend.slots_available("job") == 1


def test_slots_available_increments_on_release(backend):
    backend.acquire("job", "owner-1", 60)
    backend.release("job", "owner-1")
    assert backend.slots_available("job") == 2


def test_current_holders_reflects_state(backend):
    backend.acquire("job", "owner-1", 60)
    backend.acquire("job", "owner-2", 60)
    assert backend.current_holders("job") == {"owner-1", "owner-2"}


def test_release_frees_slot_for_new_holder(backend):
    backend.acquire("job", "owner-1", 60)
    backend.acquire("job", "owner-2", 60)
    backend.release("job", "owner-1")
    assert backend.acquire("job", "owner-3", 60) is True


def test_is_locked_true_when_holders_present(backend):
    backend.acquire("job", "owner-1", 60)
    assert backend.is_locked("job") is True


def test_is_locked_false_when_no_holders(backend):
    assert backend.is_locked("job") is False


def test_is_locked_false_after_all_released(backend):
    backend.acquire("job", "owner-1", 60)
    backend.release("job", "owner-1")
    assert backend.is_locked("job") is False


def test_reentrant_acquire_does_not_consume_extra_slot(backend):
    backend.acquire("job", "owner-1", 60)
    # Re-acquiring as the same owner should not count as a new slot.
    backend.acquire("job", "owner-1", 60)
    assert backend.slots_available("job") == 1


def test_independent_keys_do_not_interfere(backend):
    backend.acquire("job-a", "owner-1", 60)
    backend.acquire("job-a", "owner-2", 60)
    # job-b should still have 2 open slots
    assert backend.slots_available("job-b") == 2
    assert backend.acquire("job-b", "owner-3", 60) is True


def test_single_holder_mode_blocks_second():
    mem = MemoryBackend()
    sem = SemaphoreBackend(mem, max_holders=1)
    assert sem.acquire("job", "owner-1", 60) is True
    assert sem.acquire("job", "owner-2", 60) is False
