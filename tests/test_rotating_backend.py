import pytest
from unittest.mock import MagicMock
from schedlock.backends.rotating_backend import RotatingBackend


def make_backend(acquire_result=True, release_result=True, is_locked_result=False):
    b = MagicMock()
    b.acquire.return_value = acquire_result
    b.release.return_value = release_result
    b.is_locked.return_value = is_locked_result
    b.refresh.return_value = True
    return b


def test_requires_at_least_one_backend():
    with pytest.raises(ValueError):
        RotatingBackend([])


def test_backends_property():
    b1, b2 = make_backend(), make_backend()
    rb = RotatingBackend([b1, b2])
    assert rb.backends == [b1, b2]


def test_acquire_uses_first_backend_initially():
    b1, b2 = make_backend(), make_backend()
    rb = RotatingBackend([b1, b2])
    result = rb.acquire("job", "owner1", 30)
    assert result is True
    b1.acquire.assert_called_once_with("job", "owner1", 30)
    b2.acquire.assert_not_called()


def test_acquire_rotates_to_next_backend():
    b1, b2 = make_backend(), make_backend()
    rb = RotatingBackend([b1, b2])
    rb.acquire("job1", "owner1", 30)
    rb.acquire("job2", "owner2", 30)
    b1.acquire.assert_called_once()
    b2.acquire.assert_called_once()


def test_acquire_falls_back_when_first_fails():
    b1 = make_backend(acquire_result=False)
    b2 = make_backend(acquire_result=True)
    rb = RotatingBackend([b1, b2])
    result = rb.acquire("job", "owner", 30)
    assert result is True
    b1.acquire.assert_called_once()
    b2.acquire.assert_called_once()


def test_acquire_returns_false_when_all_fail():
    b1 = make_backend(acquire_result=False)
    b2 = make_backend(acquire_result=False)
    rb = RotatingBackend([b1, b2])
    result = rb.acquire("job", "owner", 30)
    assert result is False


def test_release_targets_owning_backend():
    b1, b2 = make_backend(), make_backend()
    rb = RotatingBackend([b1, b2])
    rb.acquire("job", "owner1", 30)  # b1 gets it
    rb.release("job", "owner1")
    b1.release.assert_called_once_with("job", "owner1")
    b2.release.assert_not_called()


def test_release_fallback_when_no_owner_map():
    b1 = make_backend(release_result=False)
    b2 = make_backend(release_result=True)
    rb = RotatingBackend([b1, b2])
    result = rb.release("unknown", "owner")
    assert result is True


def test_is_locked_true_if_any_backend_locked():
    b1 = make_backend(is_locked_result=False)
    b2 = make_backend(is_locked_result=True)
    rb = RotatingBackend([b1, b2])
    assert rb.is_locked("job") is True


def test_is_locked_false_when_none_locked():
    b1 = make_backend(is_locked_result=False)
    b2 = make_backend(is_locked_result=False)
    rb = RotatingBackend([b1, b2])
    assert rb.is_locked("job") is False


def test_refresh_delegates_to_owning_backend():
    b1, b2 = make_backend(), make_backend()
    rb = RotatingBackend([b1, b2])
    rb.acquire("job", "owner", 30)
    rb.refresh("job", "owner", 60)
    b1.refresh.assert_called_once_with("job", "owner", 60)
    b2.refresh.assert_not_called()


def test_refresh_returns_false_when_no_owning_backend():
    b1, b2 = make_backend(), make_backend()
    rb = RotatingBackend([b1, b2])
    result = rb.refresh("unknown_job", "owner", 60)
    assert result is False
    b1.refresh.assert_not_called()
    b2.refresh.assert_not_called()
