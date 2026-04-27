"""Re-export shim so AllowlistBackend is accessible from schedlock.backends."""
from schedlock.backends.allowlist_backend import AllowlistBackend

__all__ = ["AllowlistBackend"]
