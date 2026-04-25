from schedlock.backends import BlacklistBackend
from schedlock.backends.base import BaseBackend


def test_blacklist_backend_importable():
    assert BlacklistBackend is not None


def test_blacklist_backend_is_base_subclass():
    assert issubclass(BlacklistBackend, BaseBackend)


def test_blacklist_backend_in_all():
    import schedlock.backends as pkg
    assert "BlacklistBackend" in pkg.__all__


def test_blacklist_backend_implements_required_methods():
    """Ensure BlacklistBackend implements the abstract methods defined in BaseBackend."""
    required_methods = ["acquire", "release", "is_locked"]
    for method_name in required_methods:
        assert hasattr(BlacklistBackend, method_name), (
            f"BlacklistBackend is missing required method: {method_name}"
        )
