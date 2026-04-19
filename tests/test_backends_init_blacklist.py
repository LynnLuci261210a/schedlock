from schedlock.backends import BlacklistBackend
from schedlock.backends.base import BaseBackend


def test_blacklist_backend_importable():
    assert BlacklistBackend is not None


def test_blacklist_backend_is_base_subclass():
    assert issubclass(BlacklistBackend, BaseBackend)


def test_blacklist_backend_in_all():
    import schedlock.backends as pkg
    assert "BlacklistBackend" in pkg.__all__
