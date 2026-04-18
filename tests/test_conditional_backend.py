import pytest
from unittest.mock import MagicMock
from schedlock.backends.conditional_backend import ConditionalBackend
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
    return ConditionalBackend(inner, condition=lambda: True)


def test_requires_inner_backend():
    with pytest.raises(TypeError):
        ConditionalBackend("not-a-backend", condition=lambda: True)


def test_requires_callable_condition(inner):
    with pytest.raises(TypeError):
        ConditionalBackend(inner, condition="not-callable")


def test_requires_non_empty_reason(inner):
    with pytest.raises(ValueError):
        ConditionalBackend(inner, condition=lambda: True, reason="  ")


def test_inner_property(inner, backend):
    assert backend.inner is inner


def test_reason_property(inner):
    b = ConditionalBackend(inner, condition=lambda: True, reason="maintenance")
    assert b.reason == "maintenance"


def test_acquire_delegates_when_condition_true(inner, backend):
    result = backend.acquire("job", "owner1", 30)
    assert result is True
    inner.acquire.assert_called_once_with("job", "owner1", 30)


def test_acquire_blocked_when_condition_false(inner):
    b = ConditionalBackend(inner, condition=lambda: False)
    result = b.acquire("job", "owner1", 30)
    assert result is False
    inner.acquire.assert_not_called()


def test_release_delegates_when_condition_true(inner, backend):
    result = backend.release("job", "owner1")
    assert result is True
    inner.release.assert_called_once_with("job", "owner1")


def test_release_blocked_when_condition_false(inner):
    b = ConditionalBackend(inner, condition=lambda: False)
    result = b.release("job", "owner1")
    assert result is False
    inner.release.assert_not_called()


def test_is_locked_always_delegates(inner):
    b = ConditionalBackend(inner, condition=lambda: False)
    b.is_locked("job")
    inner.is_locked.assert_called_once_with("job")


def test_refresh_delegates_when_condition_true(inner, backend):
    result = backend.refresh("job", "owner1", 60)
    assert result is True
    inner.refresh.assert_called_once_with("job", "owner1", 60)


def test_refresh_blocked_when_condition_false(inner):
    b = ConditionalBackend(inner, condition=lambda: False)
    result = b.refresh("job", "owner1", 60)
    assert result is False
    inner.refresh.assert_not_called()


def test_dynamic_condition_changes(inner):
    state = {"enabled": True}
    b = ConditionalBackend(inner, condition=lambda: state["enabled"])
    assert b.acquire("job", "o", 10) is True
    state["enabled"] = False
    assert b.acquire("job", "o", 10) is False
