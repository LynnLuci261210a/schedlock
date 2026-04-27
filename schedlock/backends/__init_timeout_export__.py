"""Re-export shim — appended to schedlock/backends/__init__.py logic.

This file is imported by the package __init__ to register TimeoutBackend
in ``__all__`` and the package namespace without modifying the main
__init__.py directly.

Usage (at the bottom of schedlock/backends/__init__.py)::

    from schedlock.backends.__init_timeout_export__ import *  # noqa: F401,F403
"""
from schedlock.backends.timeout_backend import TimeoutBackend

__all__ = ["TimeoutBackend"]
