"""Re-export shim so HealthBackend appears in schedlock.backends.__init__.

This file is imported by schedlock/backends/__init__.py via the pattern used
for other late-added backends (circuit_retry, quota_aware, etc.).
"""
from schedlock.backends.health_backend import HealthBackend

__all__ = ["HealthBackend"]
