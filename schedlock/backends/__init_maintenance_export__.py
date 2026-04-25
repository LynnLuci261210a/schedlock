"""Registers MaintenanceBackend into the backends package __init__ exports.

This file is imported by schedlock/backends/__init__.py to expose
MaintenanceBackend at the package level without cluttering the main __init__.
"""
from schedlock.backends.maintenance_backend import MaintenanceBackend

__all__ = ["MaintenanceBackend"]
