import pytest
from unittest.mock import MagicMock

from schedlock.backends.fallthrough_backend import FallthroughBackend
from schedlock.backends.base import BaseBackend


def make_backend(acquire_result=True, release_result=True, is_locked_result=False):
    b = MagicMock(spec=BaseBackend)
    b.acquire.return_value = acquire_result
    b.release.return_value = release_result
    b.is_locked.return_value = is_locked_result
    b.refresh.return_value = acquire_result
    return b


def test_requires_at_least_two_backends():
    with pytest.raises(ValueError):
        FallthroughBackend([])
    with pytest.raises(ValueError):
        FallthroughBackend([make_backend()])


def test_requires_base_backend_instances():
    with pytest.raises(TypeError):
        FallthroughBackend([make_backend(), "not-a-backend"])


def test_backends_property_returns_copy():
    b1, b2 = make_backend(), make_backend()
    ft = FallthroughBackend([b1, b2])
    result = ft.backends
    assert result == [b1, b2]
    result.append(make_backend())
    assert len(ft.backends) == 2


def test_acquire_uses_first_backend_when_it_succeeds():
    b1, b2 = make_backend(acquire_result=True), make_backend(acquire_result=True)
    ft = FallthroughBackend([b1, b2])
    assert ft.acquire("job", "w1", 60) is True
    b1.acquire.assert_called_once_with("job", "w1", 60)
    b2.acquire.assert_not_called()


def test_acquire_falls_through_to_second_when_first_fails():
    b1 = make_backend(acquire_result=False)
    b2 = make_backend(acquire_result=True)
    ft = FallthroughBackend([b1, b2])
    assert ft.acquire("job", "w1", 60) is True
    b1.acquire.assert_called_once()
    b2.acquire.assert_called_once()


def test_acquire_returns_false_when_all_fail():
    b1 = make_backend(acquire_result=False)
    b2 = make_backend(acquire_result=False)
    ft = FallthroughBackend([b1, b2])
    assert ft.acquire("job", "w1", 60) is False


def test_acquire_skips_raising_backend_and_tries_next():
    b1 = make_backend()
    b1.acquire.side_effect = RuntimeError("connection lost")
    b2 = make_backend(acquire_result=True)
    ft = FallthroughBackend([b1, b2])
    assert ft.acquire("job", "w1", 60) is True
    b2.acquire.assert_called_once()


def test_release_targets_granting_backend():
    b1 = make_backend(acquire_result=False)
    b2 = make_backend(acquire_result=True)
    ft = FallthroughBackend([b1, b2])
    ft.acquire("job", "w1", 60)
    ft.release("job", "w1")
    b2.release.assert_called_once_with("job", "w1")
    b1.release.assert_not_called()


def test_release_without_prior_acquire_tries_all():
    b1 = make_backend(release_result=False)
    b2 = make_backend(release_result=True)
    ft = FallthroughBackend([b1, b2])
    result = ft.release("job", "w1")
    assert result is True
    b1.release.assert_called_once()
    b2.release.assert_called_once()


def test_is_locked_returns_true_if_any_backend_locked():
    b1 = make_backend(is_locked_result=False)
    b2 = make_backend(is_locked_result=True)
    ft = FallthroughBackend([b1, b2])
    assert ft.is_locked("job") is True


def test_is_locked_returns_false_when_none_locked():
    b1 = make_backend(is_locked_result=False)
    b2 = make_backend(is_locked_result=False)
    ft = FallthroughBackend([b1, b2])
    assert ft.is_locked("job") is False


def test_refresh_targets_granting_backend():
    b1 = make_backend(acquire_result=True)
    b2 = make_backend(acquire_result=True)
    ft = FallthroughBackend([b1, b2])
    ft.acquire("job", "w1", 60)
    ft.refresh("job", "w1", 60)
    b1.refresh.assert_called_once_with("job", "w1", 60)
    b2.refresh.assert_not_called()


def test_refresh_returns_false_when_no_grant_recorded():
    b1, b2 = make_backend(), make_backend()
    ft = FallthroughBackend([b1, b2])
    assert ft.refresh("job", "w1", 60) is False
