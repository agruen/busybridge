"""Tests for periodic sync job orchestration and locking."""

from __future__ import annotations

from datetime import datetime, timedelta

import pytest

from app.database import get_database, set_setting


async def _insert_user(
    email: str,
    google_user_id: str,
    main_calendar_id: str | None = None,
) -> int:
    db = await get_database()
    cursor = await db.execute(
        """INSERT INTO users (email, google_user_id, display_name, main_calendar_id)
           VALUES (?, ?, ?, ?)
           RETURNING id""",
        (email, google_user_id, "User", main_calendar_id),
    )
    row = await cursor.fetchone()
    await db.commit()
    return row["id"]


async def _insert_token(user_id: int, email: str, expiry: str | None = None) -> int:
    db = await get_database()
    cursor = await db.execute(
        """INSERT INTO oauth_tokens
           (user_id, account_type, google_account_email, access_token_encrypted, refresh_token_encrypted, token_expiry)
           VALUES (?, 'client', ?, ?, ?, ?)
           RETURNING id""",
        (user_id, email, b"a", b"r", expiry),
    )
    row = await cursor.fetchone()
    await db.commit()
    return row["id"]


async def _insert_calendar(user_id: int, token_id: int, calendar_id: str, is_active: bool = True) -> int:
    db = await get_database()
    cursor = await db.execute(
        """INSERT INTO client_calendars
           (user_id, oauth_token_id, google_calendar_id, display_name, is_active)
           VALUES (?, ?, ?, ?, ?)
           RETURNING id""",
        (user_id, token_id, calendar_id, calendar_id, is_active),
    )
    row = await cursor.fetchone()
    await db.commit()
    return row["id"]


@pytest.mark.asyncio
async def test_run_periodic_sync_pause_and_lock_paths(test_db, monkeypatch):
    """Periodic sync should honor pause setting and lock acquisition."""
    from app.jobs.sync_job import run_periodic_sync

    await set_setting("sync_paused", "true")
    called = {"lock": 0}

    async def fake_acquire(_job_name: str):
        called["lock"] += 1
        return True

    monkeypatch.setattr("app.jobs.sync_job.acquire_job_lock", fake_acquire)
    await run_periodic_sync()
    assert called["lock"] == 0

    await set_setting("sync_paused", "false")

    async def fake_lock_false(_job_name: str):
        called["lock"] += 1
        return False

    monkeypatch.setattr("app.jobs.sync_job.acquire_job_lock", fake_lock_false)
    await run_periodic_sync()
    assert called["lock"] == 1


@pytest.mark.asyncio
async def test_run_periodic_sync_runs_calendars_and_main_and_releases_lock(test_db, monkeypatch):
    """Periodic sync should fan out to client and main sync and always release lock."""
    from app.jobs.sync_job import run_periodic_sync

    user_id = await _insert_user("periodic@example.com", "periodic-google", main_calendar_id="main-cal")
    token_id = await _insert_token(user_id, "client@example.com")
    cal_1 = await _insert_calendar(user_id, token_id, "cal-1")
    cal_2 = await _insert_calendar(user_id, token_id, "cal-2")

    calls = {"cal": [], "main": [], "released": 0}

    async def fake_acquire(_job_name: str):
        return True

    async def fake_release(_job_name: str):
        calls["released"] += 1

    async def fake_trigger_sync_for_calendar(calendar_id: int):
        calls["cal"].append(calendar_id)
        if calendar_id == cal_1:
            raise RuntimeError("calendar sync failed")

    async def fake_trigger_sync_for_main_calendar(u_id: int):
        calls["main"].append(u_id)

    monkeypatch.setattr("app.jobs.sync_job.acquire_job_lock", fake_acquire)
    monkeypatch.setattr("app.jobs.sync_job.release_job_lock", fake_release)
    monkeypatch.setattr("app.sync.engine.trigger_sync_for_calendar", fake_trigger_sync_for_calendar)
    monkeypatch.setattr("app.sync.engine.trigger_sync_for_main_calendar", fake_trigger_sync_for_main_calendar)

    await run_periodic_sync()
    assert sorted(calls["cal"]) == sorted([cal_1, cal_2])
    assert calls["main"] == [user_id]
    assert calls["released"] == 1


@pytest.mark.asyncio
async def test_run_consistency_check_job_paths(test_db, monkeypatch):
    """Consistency check job should honor pause/lock and release lock after running."""
    from app.jobs.sync_job import run_consistency_check_job

    await set_setting("sync_paused", "true")
    called = {"run": 0, "release": 0}
    await run_consistency_check_job()
    assert called["run"] == 0

    await set_setting("sync_paused", "false")

    async def lock_false(_name: str):
        return False

    monkeypatch.setattr("app.jobs.sync_job.acquire_job_lock", lock_false)
    await run_consistency_check_job()
    assert called["run"] == 0

    async def lock_true(_name: str):
        return True

    async def release(_name: str):
        called["release"] += 1

    async def run_consistency():
        called["run"] += 1

    monkeypatch.setattr("app.jobs.sync_job.acquire_job_lock", lock_true)
    monkeypatch.setattr("app.jobs.sync_job.release_job_lock", release)
    monkeypatch.setattr("app.sync.consistency.run_consistency_check", run_consistency)
    await run_consistency_check_job()
    assert called["run"] == 1
    assert called["release"] == 1


@pytest.mark.asyncio
async def test_refresh_expiring_tokens_success_and_error_paths(test_db, monkeypatch):
    """Token refresh job should process expiring tokens and alert on invalid_grant."""
    from app.jobs.sync_job import refresh_expiring_tokens

    user_id = await _insert_user("refresh-job@example.com", "refresh-job-google")
    await _insert_token(
        user_id=user_id,
        email="expiring1@example.com",
        expiry=(datetime.utcnow() - timedelta(minutes=5)).isoformat(),
    )
    await _insert_token(
        user_id=user_id,
        email="expiring2@example.com",
        expiry=(datetime.utcnow() - timedelta(minutes=5)).isoformat(),
    )

    refreshed = {"ok": 0, "alerts": 0}

    async def fake_get_valid_access_token(_user_id: int, email: str):
        if email == "expiring2@example.com":
            raise RuntimeError("invalid_grant")
        refreshed["ok"] += 1
        return "token"

    async def fake_queue_alert(**_kwargs):
        refreshed["alerts"] += 1

    monkeypatch.setattr("app.auth.google.get_valid_access_token", fake_get_valid_access_token)
    monkeypatch.setattr("app.alerts.email.queue_alert", fake_queue_alert)

    await refresh_expiring_tokens()
    assert refreshed["ok"] == 1
    assert refreshed["alerts"] == 1


@pytest.mark.asyncio
async def test_acquire_and_release_job_lock(test_db):
    """Lock helpers should acquire once, reject duplicate, clear stale, and release."""
    from app.jobs.sync_job import acquire_job_lock, release_job_lock

    db = await get_database()

    assert await acquire_job_lock("job-a") is True
    assert await acquire_job_lock("job-a") is False

    await release_job_lock("job-a")
    assert await acquire_job_lock("job-a") is True

    # Stale lock should be removed and lock reacquired.
    stale_time = (datetime.utcnow() - timedelta(hours=2)).isoformat()
    await db.execute(
        "INSERT OR REPLACE INTO job_locks (job_name, locked_at, locked_by) VALUES (?, ?, ?)",
        ("job-stale", stale_time, "worker"),
    )
    await db.commit()

    assert await acquire_job_lock("job-stale", timeout_minutes=30) is True

