"""Additional coverage tests for jobs.scheduler."""

from __future__ import annotations

from types import SimpleNamespace


class _FakeScheduler:
    def __init__(self):
        self.jobs: list[str] = []
        self.started = False
        self.stopped = False

    def add_job(self, *_args, **kwargs):
        self.jobs.append(kwargs["id"])

    def start(self):
        self.started = True

    def shutdown(self, wait: bool = False):
        self.stopped = True


def test_get_scheduler_returns_current_instance(monkeypatch):
    """get_scheduler should return the module's current scheduler reference."""
    import app.jobs.scheduler as scheduler

    sentinel = object()
    monkeypatch.setattr(scheduler, "_scheduler", sentinel)
    assert scheduler.get_scheduler() is sentinel


def test_setup_scheduler_registers_webhook_job_when_enabled(monkeypatch):
    """Scheduler setup should include webhook renewal when ENABLE_WEBHOOKS is true."""
    import app.jobs.scheduler as scheduler

    monkeypatch.setattr(scheduler, "AsyncIOScheduler", _FakeScheduler)
    monkeypatch.setattr(
        scheduler,
        "get_settings",
        lambda: SimpleNamespace(
            sync_interval_minutes=5,
            enable_webhooks=True,
            webhook_renewal_hours=6,
            consistency_check_hours=1,
            token_refresh_minutes=30,
            alert_process_minutes=1,
        ),
    )

    sched = scheduler.setup_scheduler()
    assert sched.started is True
    assert "periodic_sync" in sched.jobs
    assert "webhook_renewal" in sched.jobs
    assert "token_refresh" in sched.jobs

    scheduler.shutdown_scheduler()


def test_setup_scheduler_skips_webhook_job_when_disabled(monkeypatch):
    """Scheduler setup should skip webhook renewal when ENABLE_WEBHOOKS is false."""
    import app.jobs.scheduler as scheduler

    monkeypatch.setattr(scheduler, "AsyncIOScheduler", _FakeScheduler)
    monkeypatch.setattr(
        scheduler,
        "get_settings",
        lambda: SimpleNamespace(
            sync_interval_minutes=5,
            enable_webhooks=False,
            webhook_renewal_hours=6,
            consistency_check_hours=1,
            token_refresh_minutes=30,
            alert_process_minutes=1,
        ),
    )

    sched = scheduler.setup_scheduler()
    assert sched.started is True
    assert "periodic_sync" in sched.jobs
    assert "webhook_renewal" not in sched.jobs
    assert "token_refresh" in sched.jobs

    scheduler.shutdown_scheduler()
