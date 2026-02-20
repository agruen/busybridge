"""Additional coverage tests for sync.consistency error branches."""

from __future__ import annotations

from datetime import datetime

import pytest

from app.database import get_database


async def _insert_user(
    email: str = "user@example.com",
    google_user_id: str = "google-user-1",
    main_calendar_id: str | None = "main-cal",
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


async def _insert_calendar(user_id: int, token_id: int, calendar_id: str) -> int:
    db = await get_database()
    cursor = await db.execute(
        """INSERT INTO client_calendars
           (user_id, oauth_token_id, google_calendar_id, display_name, is_active)
           VALUES (?, ?, ?, ?, TRUE)
           RETURNING id""",
        (user_id, token_id, calendar_id, calendar_id),
    )
    row = await cursor.fetchone()
    await db.commit()
    return row["id"]


@pytest.mark.asyncio
async def test_check_user_consistency_error_branches(test_db, monkeypatch):
    """Per-user consistency should tolerate delete/recreate/token errors and continue."""
    from app.sync.consistency import check_user_consistency

    user_id = await _insert_user(email="cons@example.com", google_user_id="cons-google", main_calendar_id="main-cal")
    token_ok = await _insert_token(user_id, "client-ok@example.com")
    token_raise = await _insert_token(user_id, "client-raise@example.com")
    cal_ok = await _insert_calendar(user_id, token_ok, "cal-ok")
    cal_raise = await _insert_calendar(user_id, token_raise, "cal-raise")
    db = await get_database()

    # Mapping A: origin missing + main delete throws -> covers line 101-102.
    await db.execute(
        """INSERT INTO event_mappings
           (user_id, origin_type, origin_calendar_id, origin_event_id, main_event_id, is_recurring, user_can_edit)
           VALUES (?, 'client', ?, 'origin-missing', 'main-delete-fail', FALSE, TRUE)""",
        (user_id, cal_ok),
    )

    # Mapping B: origin exists + main missing + recreate throws -> covers 131-136 (create fail path).
    await db.execute(
        """INSERT INTO event_mappings
           (user_id, origin_type, origin_calendar_id, origin_event_id, main_event_id, is_recurring, user_can_edit)
           VALUES (?, 'client', ?, 'origin-live', 'main-missing', FALSE, TRUE)""",
        (user_id, cal_ok),
    )

    # Mapping C: token fetch fails for this mapping -> covers outer mapping error branch.
    await db.execute(
        """INSERT INTO event_mappings
           (user_id, origin_type, origin_calendar_id, origin_event_id, main_event_id, is_recurring, user_can_edit)
           VALUES (?, 'client', ?, 'origin-token-fail', 'main-token-fail', FALSE, TRUE)""",
        (user_id, cal_raise),
    )

    # Deleted mapping + stale busy block for orphan cleanup branch with remote delete failure.
    cursor = await db.execute(
        """INSERT INTO event_mappings
           (user_id, origin_type, origin_calendar_id, origin_event_id, main_event_id, deleted_at, is_recurring, user_can_edit)
           VALUES (?, 'client', ?, 'origin-deleted', 'main-deleted', ?, FALSE, TRUE)
           RETURNING id""",
        (user_id, cal_ok, datetime.utcnow().isoformat()),
    )
    deleted_mapping_id = (await cursor.fetchone())["id"]
    await db.execute(
        """INSERT INTO busy_blocks (event_mapping_id, client_calendar_id, busy_block_event_id)
           VALUES (?, ?, ?)""",
        (deleted_mapping_id, cal_ok, "busy-stale"),
    )
    await db.commit()

    async def fake_get_valid_access_token(_user_id: int, email: str) -> str:
        if email == "cons@example.com":
            return "main-token"
        if email == "client-raise@example.com":
            raise RuntimeError("token lookup failed")
        return "client-token"

    class FakeGoogleCalendarClient:
        def __init__(self, token: str):
            self.token = token

        def get_event(self, _calendar_id: str, event_id: str):
            if self.token == "main-token":
                if event_id == "main-missing":
                    return None
                return {"id": event_id}
            if event_id == "origin-missing":
                return None
            if event_id == "origin-live":
                return {
                    "id": "origin-live",
                    "start": {"dateTime": "2026-01-01T10:00:00Z"},
                    "end": {"dateTime": "2026-01-01T11:00:00Z"},
                }
            return {"id": event_id}

        def delete_event(self, _calendar_id: str, _event_id: str):
            raise RuntimeError("delete failed")

        def create_event(self, _calendar_id: str, _event_data: dict):
            raise RuntimeError("create failed")

    monkeypatch.setattr("app.auth.google.get_valid_access_token", fake_get_valid_access_token)
    monkeypatch.setattr("app.sync.consistency.GoogleCalendarClient", FakeGoogleCalendarClient)

    summary = {
        "users_checked": 0,
        "mappings_checked": 0,
        "orphaned_main_events_deleted": 0,
        "missing_copies_recreated": 0,
        "orphaned_busy_blocks_deleted": 0,
        "errors": 0,
    }

    await check_user_consistency(user_id, summary)

    assert summary["mappings_checked"] == 3
    assert summary["orphaned_main_events_deleted"] == 0
    assert summary["missing_copies_recreated"] == 0
    assert summary["errors"] >= 1
    assert summary["orphaned_busy_blocks_deleted"] == 0

    cursor = await db.execute(
        "SELECT COUNT(*) FROM busy_blocks WHERE event_mapping_id = ?",
        (deleted_mapping_id,),
    )
    assert (await cursor.fetchone())[0] == 0
