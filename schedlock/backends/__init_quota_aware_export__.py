"""Registers QuotaAwareBackend in the schedlock.backends namespace.

Import this module to ensure QuotaAwareBackend is available via
``schedlock.backends.QuotaAwareBackend``.

This follows the same side-effect-import pattern used by other optional
backends in the package so that the main ``__init__.py`` can stay lean.
"""

from schedlock.backends.quota_aware_backend import QuotaAwareBackend  # noqa: F401

__all__ = ["QuotaAwareBackend"]
