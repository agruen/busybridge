"""Additional branch coverage tests for jobs.sync_job."""

from __future__ import annotations

import pytest

from app.database import get_database, set_setting


async def _insert_user(email: str, google_user_id: str, main_calendar_id: str | None = "main-cal") -> int:
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


async def _insert_token(user_id: int, email: str) -> int:
    db = await get_database()
    cursor = await db.execute(
        """INSERT INTO oauth_tokens
           (user_id, account_type, google_account_email, access_token_encrypted, refresh_token_encrypted)
           VALUES (?, 'client', ?, ?, ?)
           RETURNING id""",
        (user_id, email, b"a", b"r"),
    )
    row = await cursor.fetchone()
    await db.commit()
    return row["id"]


@pytest.mark.asyncio
async def test_run_periodic_sync_handles_main_calendar_sync_errors(test_db, monkeypatch):
    """Periodic sync should swallow per-user main calendar sync exceptions."""
    from app.jobs.sync_job import run_periodic_sync

    await set_setting("sync_paused", "false")
    user_id = await _insert_user("periodic-main-err@example.com", "periodic-main-err-google", main_calendar_id="main")
    token_id = await _insert_token(user_id, "periodic-client@example.com")
    db = await get_database()
    await db.execute(
        """INSERT INTO client_calendars
           (user_id, oauth_token_id, google_calendar_id, display_name, is_active)
           VALUES (?, ?, ?, ?, TRUE)""",
        (user_id, token_id, "periodic-cal", "Periodic"),
    )
    await db.commit()

    async def fake_trigger_sync_for_calendar(_calendar_id: int):
        return None

    async def exploding_trigger_main(_user_id: int):
        raise RuntimeError("main sync failed")

    monkeypatch.setattr("app.sync.engine.trigger_sync_for_calendar", fake_trigger_sync_for_calendar)
    monkeypatch.setattr("app.sync.engine.trigger_sync_for_main_calendar", exploding_trigger_main)

    await run_periodic_sync()

    cursor = await db.execute("SELECT COUNT(*) FROM job_locks WHERE job_name = 'periodic_sync'")
    assert (await cursor.fetchone())[0] == 0


@pytest.mark.asyncio
async def test_refresh_expiring_tokens_noop_when_none_expiring(test_db):
    """Token refresher should return immediately when no tokens are near expiry."""
    from app.jobs.sync_job import refresh_expiring_tokens

    await refresh_expiring_tokens()
