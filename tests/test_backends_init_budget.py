"""Tests that BudgetBackend is exported from schedlock.backends."""
from schedlock import backends
from schedlock.backends.budget_backend import BudgetBackend
from schedlock.backends.base import BaseBackend


def test_budget_backend_importable():
    assert hasattr(backends, "BudgetBackend")


def test_budget_backend_is_base_subclass():
    assert issubclass(BudgetBackend, BaseBackend)


def test_budget_backend_in_all():
    assert "BudgetBackend" in backends.__all__
