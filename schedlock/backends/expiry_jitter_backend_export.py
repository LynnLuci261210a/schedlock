"""Re-export shim so ExpiryJitterBackend is accessible from schedlock.backends.

This file exists to keep the main ``schedlock/backends/__init__.py`` tidy
while still making ``ExpiryJitterBackend`` part of the public API surface.

Usage::

    from schedlock.backends import ExpiryJitterBackend
"""

from schedlock.backends.expiry_jitter_backend import ExpiryJitterBackend

__all__ = ["ExpiryJitterBackend"]
