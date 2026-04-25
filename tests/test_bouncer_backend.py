import pytest
from unittest.mock import MagicMock

from schedlock.backends.base import BaseBackend
from schedlock.backends.bouncer_backend import BouncerBackend


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


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
    return BouncerBackend(inner, bouncer=lambda key, owner: True)


# ---------------------------------------------------------------------------
# Construction guards
# ---------------------------------------------------------------------------


def test_requires_inner_backend():
    with pytest.raises(TypeError, match="inner must be a BaseBackend"):
        BouncerBackend(object(), bouncer=lambda k, o: True)  # type: ignore


def test_requires_callable_bouncer(inner):
    with pytest.raises(TypeError, match="bouncer must be callable"):
        BouncerBackend(inner, bouncer="not-callable")  # type: ignore


def test_requires_non_empty_reason(inner):
    with pytest.raises(ValueError, match="reason must be a non-empty string"):
        BouncerBackend(inner, bouncer=lambda k, o: True, reason="   ")


# ---------------------------------------------------------------------------
# Properties
# ---------------------------------------------------------------------------


def test_inner_property(inner, backend):
    assert backend.inner is inner


def test_reason_property(inner):
    b = BouncerBackend(inner, bouncer=lambda k, o: True, reason="custom reason")
    assert b.reason == "custom reason"


def test_reason_default(inner):
    b = BouncerBackend(inner, bouncer=lambda k, o: True)
    assert b.reason == "bouncer denied"


# ---------------------------------------------------------------------------
# acquire
# ---------------------------------------------------------------------------


def test_acquire_allowed_delegates_to_inner(inner, backend):
    result = backend.acquire("job", "worker-1", 30)
    assert result is True
    inner.acquire.assert_called_once_with("job", "worker-1", 30)


def test_acquire_denied_does_not_call_inner(inner):
    b = BouncerBackend(inner, bouncer=lambda k, o: False)
    result = b.acquire("job", "worker-1", 30)
    assert result is False
    inner.acquire.assert_not_called()


def test_acquire_bouncer_receives_key_and_owner(inner):
    calls = []

    def bouncer(key, owner):
        calls.append((key, owner))
        return True

    b = BouncerBackend(inner, bouncer=bouncer)
    b.acquire("my-job", "worker-99", 60)
    assert calls == [("my-job", "worker-99")]


def test_acquire_inner_returns_false_propagated(inner):
    inner.acquire.return_value = False
    b = BouncerBackend(inner, bouncer=lambda k, o: True)
    assert b.acquire("job", "worker-1") is False


# ---------------------------------------------------------------------------
# release / is_locked / refresh
# ---------------------------------------------------------------------------


def test_release_delegates(inner, backend):
    backend.release("job", "worker-1")
    inner.release.assert_called_once_with("job", "worker-1")


def test_is_locked_delegates(inner, backend):
    inner.is_locked.return_value = True
    assert backend.is_locked("job") is True
    inner.is_locked.assert_called_once_with("job")


def test_refresh_delegates(inner, backend):
    backend.refresh("job", "worker-1", 60)
    inner.refresh.assert_called_once_with("job", "worker-1", 60)
