"""Leasing backend export shim.

This module is imported by schedlock/backends/__init__.py to expose
LeasingBackend at the package level without cluttering the main __init__.
"""
from schedlock.backends.leasing_backend import LeasingBackend

__all__ = ["LeasingBackend"]
