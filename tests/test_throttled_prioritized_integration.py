import time
import pytest
from schedlock.backends.memory_backend import MemoryBackend
from schedlock.backends.throttled_backend import ThrottledBackend
from schedlock.backends.prioritized_backend import PrioritizedBackend


@pytest.fixture
def memory():
    return MemoryBackend()


def test_prioritized_over_throttled(memory):
    throttled = ThrottledBackend(memory, max_acquires=2, window=10.0)
    backend = PrioritizedBackend(throttled, min_priority=5)

    assert backend.acquire("job", "o1", 60, priority=5) is True
    memory.release("job", "o1")
    assert backend.acquire("job", "o2", 60, priority=5) is True
    memory.release("job", "o2")
    # throttle limit hit
    assert backend.acquire("job", "o3", 60, priority=10) is False


def test_throttled_over_prioritized(memory):
    prioritized = PrioritizedBackend(memory, min_priority=3)
    backend = ThrottledBackend(prioritized, max_acquires=1, window=10.0)

    # low priority blocked by prioritized layer
    assert backend.acquire("job", "o1", 60) is False


def test_throttled_window_resets_allow_reacquire(memory):
    throttled = ThrottledBackend(memory, max_acquires=1, window=0.1)
    backend = PrioritizedBackend(throttled, min_priority=0)

    assert backend.acquire("job", "o1", 60) is True
    memory.release("job", "o1")
    assert backend.acquire("job", "o2", 60) is False
    time.sleep(0.15)
    assert backend.acquire("job", "o3", 60) is True


def test_priority_zero_min_throttle_two(memory):
    throttled = ThrottledBackend(memory, max_acquires=2, window=60.0)
    backend = PrioritizedBackend(throttled, min_priority=0)

    assert backend.acquire("job", "o1", 60) is True
    memory.release("job", "o1")
    assert backend.acquire("job", "o2", 60) is True
    memory.release("job", "o2")
    assert backend.acquire("job", "o3", 60) is False
