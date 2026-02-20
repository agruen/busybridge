"""Additional coverage tests for jobs.scheduler."""

from __future__ import annotations


def test_get_scheduler_returns_current_instance(monkeypatch):
    """get_scheduler should return the module's current scheduler reference."""
    import app.jobs.scheduler as scheduler

    sentinel = object()
    monkeypatch.setattr(scheduler, "_scheduler", sentinel)
    assert scheduler.get_scheduler() is sentinel
