import pytest
from schedlock.backends.memory_backend import MemoryBackend
from schedlock.backends.tagging_policy_backend import TaggingPolicyBackend
from schedlock.backends.namespaced_backend import NamespacedBackend


@pytest.fixture
def memory():
    return MemoryBackend()


def test_full_lifecycle(memory):
    backend = TaggingPolicyBackend(memory, policy=lambda k: {"job": k, "tier": "gold"})
    assert backend.acquire("cron:report", "worker-1", 60)
    tags = backend.tags_for("cron:report")
    assert tags["job"] == "cron:report"
    assert tags["tier"] == "gold"
    assert backend.release("cron:report", "worker-1")
    assert backend.tags_for("cron:report") == {}


def test_second_worker_blocked_does_not_overwrite_tags(memory):
    backend = TaggingPolicyBackend(
        memory, policy=lambda k: {"owner": "static"}
    )
    assert backend.acquire("job", "worker-1", 60)
    assert not backend.acquire("job", "worker-2", 60)
    # tags should still reflect worker-1's acquisition
    assert backend.tags_for("job") == {"owner": "static"}


def test_reacquire_after_release_updates_tags(memory):
    call_count = {"n": 0}

    def policy(key):
        call_count["n"] += 1
        return {"attempt": str(call_count["n"])}

    backend = TaggingPolicyBackend(memory, policy=policy)
    backend.acquire("job", "w", 30)
    assert backend.tags_for("job")["attempt"] == "1"
    backend.release("job", "w")
    backend.acquire("job", "w", 30)
    assert backend.tags_for("job")["attempt"] == "2"


def test_tagging_policy_over_namespaced(memory):
    ns = NamespacedBackend(memory, namespace="prod")
    backend = TaggingPolicyBackend(ns, policy=lambda k: {"ns": "prod", "k": k})
    assert backend.acquire("daily", "w", 30)
    tags = backend.tags_for("daily")
    assert tags["ns"] == "prod"
    assert tags["k"] == "daily"
    assert backend.release("daily", "w")
