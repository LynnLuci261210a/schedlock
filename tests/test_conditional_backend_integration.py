import pytest
from schedlock.backends.memory_backend import MemoryBackend
from schedlock.backends.conditional_backend import ConditionalBackend


@pytest.fixture
def memory():
    return MemoryBackend()


def test_acquire_and_release_when_enabled(memory):
    b = ConditionalBackend(memory, condition=lambda: True)
    assert b.acquire("job", "owner", 30) is True
    assert b.is_locked("job") is True
    assert b.release("job", "owner") is True
    assert b.is_locked("job") is False


def test_acquire_blocked_does_not_lock(memory):
    b = ConditionalBackend(memory, condition=lambda: False)
    assert b.acquire("job", "owner", 30) is False
    assert b.is_locked("job") is False


def test_release_blocked_leaves_lock_held(memory):
    # acquire directly on inner, then try release via conditional
    memory.acquire("job", "owner", 30)
    b = ConditionalBackend(memory, condition=lambda: False)
    result = b.release("job", "owner")
    assert result is False
    assert memory.is_locked("job") is True


def test_condition_toggle_mid_workflow(memory):
    flag = {"on": True}
    b = ConditionalBackend(memory, condition=lambda: flag["on"])
    assert b.acquire("job", "owner", 30) is True
    flag["on"] = False
    # release blocked — lock remains
    assert b.release("job", "owner") is False
    assert memory.is_locked("job") is True
    flag["on"] = True
    assert b.release("job", "owner") is True
    assert memory.is_locked("job") is False


def test_is_locked_visible_regardless_of_condition(memory):
    memory.acquire("job", "owner", 30)
    b = ConditionalBackend(memory, condition=lambda: False)
    assert b.is_locked("job") is True
