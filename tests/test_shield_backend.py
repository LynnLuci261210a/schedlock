"""Tests for ShieldBackend."""
from unittest.mock import MagicMock

import pytest

from schedlock.backends.base import BaseBackend
from schedlock.backends.shield_backend import ShieldBackend


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
    return ShieldBackend(inner)


# ---------------------------------------------------------------------------
# Construction validation
# ---------------------------------------------------------------------------

def test_requires_inner_backend():
    with pytest.raises(TypeError):
        ShieldBackend("not-a-backend")


def test_requires_non_empty_reason(inner):
    with pytest.raises(ValueError):
        ShieldBackend(inner, reason="")


def test_requires_string_reason(inner):
    with pytest.raises(ValueError):
        ShieldBackend(inner, reason=None)


# ---------------------------------------------------------------------------
# Properties
# ---------------------------------------------------------------------------

def test_inner_property(inner, backend):
    assert backend.inner is inner


def test_reason_default(backend):
    assert backend.reason == "shielded"


def test_reason_custom(inner):
    b = ShieldBackend(inner, reason="maintenance shield")
    assert b.reason == "maintenance shield"


def test_is_shielded_initially_false(backend):
    assert backend.is_shielded is False


# ---------------------------------------------------------------------------
# Shield control
# ---------------------------------------------------------------------------

def test_raise_shield_sets_flag(backend):
    backend.raise_shield()
    assert backend.is_shielded is True


def test_lower_shield_clears_flag(backend):
    backend.raise_shield()
    backend.lower_shield()
    assert backend.is_shielded is False


# ---------------------------------------------------------------------------
# acquire behaviour
# ---------------------------------------------------------------------------

def test_acquire_delegates_when_not_shielded(inner, backend):
    result = backend.acquire("job", "worker-1", 30)
    assert result is True
    inner.acquire.assert_called_once_with("job", "worker-1", 30)


def test_acquire_blocked_when_shielded(inner, backend):
    backend.raise_shield()
    result = backend.acquire("job", "worker-1", 30)
    assert result is False
    inner.acquire.assert_not_called()


def test_acquire_resumes_after_shield_lowered(inner, backend):
    backend.raise_shield()
    backend.lower_shield()
    result = backend.acquire("job", "worker-1", 30)
    assert result is True
    inner.acquire.assert_called_once()


# ---------------------------------------------------------------------------
# release / is_locked / refresh always delegate
# ---------------------------------------------------------------------------

def test_release_delegates_regardless_of_shield(inner, backend):
    backend.raise_shield()
    result = backend.release("job", "worker-1")
    assert result is True
    inner.release.assert_called_once_with("job", "worker-1")


def test_is_locked_delegates_regardless_of_shield(inner, backend):
    backend.raise_shield()
    backend.is_locked("job")
    inner.is_locked.assert_called_once_with("job")


def test_refresh_delegates_regardless_of_shield(inner, backend):
    backend.raise_shield()
    result = backend.refresh("job", "worker-1", 60)
    assert result is True
    inner.refresh.assert_called_once_with("job", "worker-1", 60)
