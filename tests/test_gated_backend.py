import pytest
from unittest.mock import MagicMock

from schedlock.backends.base import BaseBackend
from schedlock.backends.gated_backend import GatedBackend


@pytest.fixture()
def inner():
    m = MagicMock(spec=BaseBackend)
    m.acquire.return_value = True
    m.release.return_value = True
    m.is_locked.return_value = False
    m.refresh.return_value = True
    return m


@pytest.fixture()
def backend(inner):
    return GatedBackend(inner, gate_fn=lambda: True, reason="open for business")


def test_requires_inner_backend():
    with pytest.raises(ValueError, match="inner"):
        GatedBackend(object(), gate_fn=lambda: True, reason="r")


def test_requires_callable_gate_fn(inner):
    with pytest.raises(ValueError, match="gate_fn"):
        GatedBackend(inner, gate_fn="not callable", reason="r")


def test_requires_non_empty_reason(inner):
    with pytest.raises(ValueError, match="reason"):
        GatedBackend(inner, gate_fn=lambda: True, reason="   ")


def test_inner_property(inner, backend):
    assert backend.inner is inner


def test_reason_property(backend):
    assert backend.reason == "open for business"


def test_is_open_when_gate_returns_true(inner):
    b = GatedBackend(inner, gate_fn=lambda: True, reason="r")
    assert b.is_open() is True


def test_is_closed_when_gate_returns_false(inner):
    b = GatedBackend(inner, gate_fn=lambda: False, reason="r")
    assert b.is_open() is False


def test_acquire_delegates_when_gate_open(inner, backend):
    result = backend.acquire("k", "owner", 60)
    assert result is True
    inner.acquire.assert_called_once_with("k", "owner", 60)


def test_acquire_blocked_when_gate_closed(inner):
    b = GatedBackend(inner, gate_fn=lambda: False, reason="r")
    result = b.acquire("k", "owner", 60)
    assert result is False
    inner.acquire.assert_not_called()


def test_release_delegates_when_gate_open(inner, backend):
    result = backend.release("k", "owner")
    assert result is True
    inner.release.assert_called_once_with("k", "owner")


def test_release_blocked_when_gate_closed(inner):
    b = GatedBackend(inner, gate_fn=lambda: False, reason="r")
    result = b.release("k", "owner")
    assert result is False
    inner.release.assert_not_called()


def test_is_locked_always_delegates(inner):
    b = GatedBackend(inner, gate_fn=lambda: False, reason="r")
    b.is_locked("k")
    inner.is_locked.assert_called_once_with("k")


def test_refresh_delegates_when_gate_open(inner, backend):
    result = backend.refresh("k", "owner", 30)
    assert result is True
    inner.refresh.assert_called_once_with("k", "owner", 30)


def test_refresh_blocked_when_gate_closed(inner):
    b = GatedBackend(inner, gate_fn=lambda: False, reason="r")
    result = b.refresh("k", "owner", 30)
    assert result is False
    inner.refresh.assert_not_called()


def test_gate_fn_evaluated_per_call(inner):
    calls = []

    def toggle():
        calls.append(1)
        return len(calls) % 2 == 1  # True on odd calls, False on even

    b = GatedBackend(inner, gate_fn=toggle, reason="r")
    assert b.acquire("k", "o", 10) is True   # call 1 -> open
    assert b.acquire("k", "o", 10) is False  # call 2 -> closed
    assert b.acquire("k", "o", 10) is True   # call 3 -> open
