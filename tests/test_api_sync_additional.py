"""Additional branch coverage tests for api.sync."""

from __future__ import annotations

import pytest

from app.auth.session import User
from app.database import get_database


async def _insert_user(email: str, google_user_id: str) -> int:
    db = await get_database()
    cursor = await db.execute(
        """INSERT INTO users (email, google_user_id, display_name, main_calendar_id)
           VALUES (?, ?, ?, ?)
           RETURNING id""",
        (email, google_user_id, "User", "main-cal"),
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


async def _insert_calendar(user_id: int, token_id: int, calendar_id: str, failures: int) -> int:
    db = await get_database()
    cursor = await db.execute(
        """INSERT INTO client_calendars
           (user_id, oauth_token_id, google_calendar_id, display_name, is_active)
           VALUES (?, ?, ?, ?, TRUE)
           RETURNING id""",
        (user_id, token_id, calendar_id, calendar_id),
    )
    row = await cursor.fetchone()
    cal_id = row["id"]
    await db.execute(
        """INSERT INTO calendar_sync_state (client_calendar_id, consecutive_failures)
           VALUES (?, ?)""",
        (cal_id, failures),
    )
    await db.commit()
    return cal_id


def _user_model(user_id: int, email: str) -> User:
    return User(
        id=user_id,
        email=email,
        google_user_id=f"{email}-google",
        display_name="User",
        main_calendar_id="main-cal",
        is_admin=False,
    )


@pytest.mark.asyncio
async def test_sync_status_all_health_buckets_and_log_status_filter(test_db):
    """Sync API should count healthy/warning/error calendars and filter logs by status."""
    from app.api.sync import get_sync_log, get_sync_status

    user_id = await _insert_user("sync-extra@example.com", "sync-extra-google")
    token_id = await _insert_token(user_id, "sync-extra-client@example.com")
    cal_ok = await _insert_calendar(user_id, token_id, "cal-ok", failures=0)
    cal_warn = await _insert_calendar(user_id, token_id, "cal-warn", failures=1)
    cal_err = await _insert_calendar(user_id, token_id, "cal-err", failures=5)
    db = await get_database()

    await db.execute(
        """INSERT INTO sync_log (user_id, calendar_id, action, status, details)
           VALUES (?, ?, 'sync', 'success', '{}')""",
        (user_id, cal_ok),
    )
    await db.execute(
        """INSERT INTO sync_log (user_id, calendar_id, action, status, details)
           VALUES (?, ?, 'sync', 'failure', '{}')""",
        (user_id, cal_warn),
    )
    await db.execute(
        """INSERT INTO sync_log (user_id, calendar_id, action, status, details)
           VALUES (?, ?, 'sync', 'failure', '{}')""",
        (user_id, cal_err),
    )
    await db.commit()

    user = _user_model(user_id, "sync-extra@example.com")
    status = await get_sync_status(user=user)
    assert status.calendars_connected == 3
    assert status.calendars_healthy == 1
    assert status.calendars_warning == 1
    assert status.calendars_error == 1

    log = await get_sync_log(user=user, status_filter="failure")
    assert log.total == 2
    assert len(log.entries) == 2
    assert all(entry.status == "failure" for entry in log.entries)


@pytest.mark.asyncio
async def test_cleanup_managed_events_endpoint_returns_summary_and_logs_action(test_db, monkeypatch):
    """Managed cleanup endpoint should return counts and write an audit log entry."""
    from app.api.sync import cleanup_managed_events

    user_id = await _insert_user("cleanup-api@example.com", "cleanup-api-google")
    user = _user_model(user_id, "cleanup-api@example.com")

    async def fake_cleanup_managed_events_for_user(_user_id: int):
        return {
            "status": "partial",
            "managed_event_prefix": "[BusyBridge]",
            "db_main_events_deleted": 3,
            "db_busy_blocks_deleted": 5,
            "prefix_calendars_scanned": 4,
            "prefix_events_deleted": 7,
            "local_mappings_deleted": 8,
            "local_busy_blocks_deleted": 9,
            "errors": ["token:bad@example.com:RuntimeError"],
        }

    monkeypatch.setattr(
        "app.sync.engine.cleanup_managed_events_for_user",
        fake_cleanup_managed_events_for_user,
    )

    result = await cleanup_managed_events(user=user)
    assert result.status == "partial"
    assert result.db_main_events_deleted == 3
    assert result.db_busy_blocks_deleted == 5
    assert result.prefix_events_deleted == 7
    assert result.local_mappings_deleted == 8
    assert len(result.errors) == 1

    db = await get_database()
    cursor = await db.execute(
        """SELECT action, status
           FROM sync_log
           WHERE user_id = ?
           ORDER BY id DESC LIMIT 1""",
        (user_id,),
    )
    row = await cursor.fetchone()
    assert row["action"] == "managed_cleanup"
    assert row["status"] == "partial"
