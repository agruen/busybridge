"""Tests for single-instance recurring event handling in sync rules."""

from __future__ import annotations

from types import SimpleNamespace
from typing import Optional
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from app.database import get_database


# ---------------------------------------------------------------------------
# DB helpers (shared with other engine-path tests)
# ---------------------------------------------------------------------------

async def _insert_user(db, email: str = "user@example.com") -> int:
    cursor = await db.execute(
        """INSERT INTO users (email, google_user_id, display_name, main_calendar_id)
           VALUES (?, ?, ?, ?) RETURNING id""",
        (email, "gid-1", "User", "main-cal"),
    )
    row = await cursor.fetchone()
    await db.commit()
    return row["id"]


async def _insert_token(db, user_id: int, email: str) -> int:
    cursor = await db.execute(
        """INSERT INTO oauth_tokens
           (user_id, account_type, google_account_email,
            access_token_encrypted, refresh_token_encrypted)
           VALUES (?, 'client', ?, ?, ?) RETURNING id""",
        (user_id, email, b"a", b"r"),
    )
    row = await cursor.fetchone()
    await db.commit()
    return row["id"]


async def _insert_calendar(db, user_id: int, token_id: int, cal_id: str) -> int:
    cursor = await db.execute(
        """INSERT INTO client_calendars
           (user_id, oauth_token_id, google_calendar_id, display_name, is_active)
           VALUES (?, ?, ?, ?, TRUE) RETURNING id""",
        (user_id, token_id, cal_id, cal_id),
    )
    row = await cursor.fetchone()
    await db.commit()
    return row["id"]


async def _insert_mapping(
    db,
    user_id: int,
    origin_calendar_id: Optional[int],
    origin_event_id: str,
    main_event_id: str,
    origin_type: str = "client",
    origin_recurring_event_id: Optional[str] = None,
    is_recurring: bool = True,
) -> int:
    cursor = await db.execute(
        """INSERT INTO event_mappings
           (user_id, origin_type, origin_calendar_id, origin_event_id,
            origin_recurring_event_id, main_event_id,
            event_start, event_end, is_all_day, is_recurring, user_can_edit)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?, FALSE, ?, TRUE) RETURNING id""",
        (
            user_id,
            origin_type,
            origin_calendar_id,
            origin_event_id,
            origin_recurring_event_id,
            main_event_id,
            "2026-01-05T15:00:00Z",
            "2026-01-05T16:00:00Z",
            is_recurring,
        ),
    )
    row = await cursor.fetchone()
    await db.commit()
    return row["id"]


async def _insert_busy_block(db, mapping_id: int, cal_id: int, bb_event_id: str) -> int:
    cursor = await db.execute(
        """INSERT INTO busy_blocks (event_mapping_id, client_calendar_id, busy_block_event_id)
           VALUES (?, ?, ?) RETURNING id""",
        (mapping_id, cal_id, bb_event_id),
    )
    row = await cursor.fetchone()
    await db.commit()
    return row["id"]


# ---------------------------------------------------------------------------
# handle_deleted_client_event – single-instance cancellation
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_handle_deleted_client_event_instance_cancels_busy_block_instance(test_db):
    """Cancelling one recurring instance should delete that instance from the
    busy-block calendar, not the whole busy-block series."""
    from app.sync.rules import handle_deleted_client_event

    db = test_db
    user_id = await _insert_user(db)
    token_id = await _insert_token(db, user_id, "client@example.com")
    client_cal_db_id = await _insert_calendar(db, user_id, token_id, "client-cal-1")
    other_token_id = await _insert_token(db, user_id, "other@example.com")
    other_cal_db_id = await _insert_calendar(db, user_id, other_token_id, "other-cal-1")

    # Parent recurring series mapping
    mapping_id = await _insert_mapping(
        db, user_id, client_cal_db_id, "series-abc", "main-series-xyz",
        is_recurring=True,
    )
    await _insert_busy_block(db, mapping_id, other_cal_db_id, "bb-series-xyz")

    # Track which event IDs were deleted
    deleted: list[str] = []

    def fake_delete(cal_id: str, event_id: str, **_kw) -> bool:
        deleted.append(event_id)
        return True

    main_client = SimpleNamespace(delete_event=fake_delete)

    with patch(
        "app.auth.google.get_valid_access_token",
        AsyncMock(return_value="token"),
    ), patch(
        "app.sync.rules.GoogleCalendarClient",
    ) as MockClient:
        fake_other = MagicMock()
        fake_other.delete_event = fake_delete
        MockClient.return_value = fake_other

        await handle_deleted_client_event(
            user_id=user_id,
            client_calendar_id=client_cal_db_id,
            event_id="series-abc_20260227T150000Z",
            main_calendar_id="main-cal",
            main_client=main_client,
            recurring_event_id="series-abc",
            original_start_time={"dateTime": "2026-02-27T10:00:00-05:00"},
        )

    # The main-calendar series *instance* and the busy-block *instance* should
    # have been deleted, not the parent IDs.
    assert "main-series-xyz_20260227T150000Z" in deleted
    assert "bb-series-xyz_20260227T150000Z" in deleted

    # Parent mapping and busy block record must still exist
    cursor = await db.execute(
        "SELECT id FROM event_mappings WHERE id = ?", (mapping_id,)
    )
    assert await cursor.fetchone() is not None

    cursor = await db.execute(
        "SELECT id FROM busy_blocks WHERE event_mapping_id = ?", (mapping_id,)
    )
    assert await cursor.fetchone() is not None


@pytest.mark.asyncio
async def test_handle_deleted_client_event_no_parent_mapping_is_noop(test_db):
    """If there is no parent mapping the function should return without error."""
    from app.sync.rules import handle_deleted_client_event

    db = test_db
    user_id = await _insert_user(db)
    token_id = await _insert_token(db, user_id, "client@example.com")
    client_cal_db_id = await _insert_calendar(db, user_id, token_id, "client-cal-1")

    main_client = SimpleNamespace(delete_event=MagicMock())

    # Should not raise even though there is no mapping
    await handle_deleted_client_event(
        user_id=user_id,
        client_calendar_id=client_cal_db_id,
        event_id="unknown_20260101T100000Z",
        main_calendar_id="main-cal",
        main_client=main_client,
        recurring_event_id="unknown-series",
        original_start_time={"dateTime": "2026-01-01T10:00:00Z"},
    )

    main_client.delete_event.assert_not_called()


# ---------------------------------------------------------------------------
# handle_deleted_main_event – single-instance cancellation
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_handle_deleted_main_event_instance_cancels_busy_block_instance(test_db):
    """Cancelling one instance of a main recurring event should cancel the
    corresponding busy-block instance on each client calendar."""
    from app.sync.rules import handle_deleted_main_event

    db = test_db
    user_id = await _insert_user(db)
    token_id = await _insert_token(db, user_id, "client@example.com")
    client_cal_db_id = await _insert_calendar(db, user_id, token_id, "client-cal-1")

    mapping_id = await _insert_mapping(
        db, user_id, None, "main-series-xyz", "main-series-xyz",
        origin_type="main", is_recurring=True,
    )
    await _insert_busy_block(db, mapping_id, client_cal_db_id, "bb-main-series")

    deleted: list[str] = []

    with patch(
        "app.auth.google.get_valid_access_token",
        AsyncMock(return_value="token"),
    ), patch(
        "app.sync.rules.GoogleCalendarClient",
    ) as MockClient:
        fake_client = MagicMock()
        fake_client.delete_event = lambda cal_id, event_id, **_: deleted.append(event_id)
        MockClient.return_value = fake_client

        await handle_deleted_main_event(
            user_id=user_id,
            event_id="main-series-xyz_20260310T140000Z",
            recurring_event_id="main-series-xyz",
            original_start_time={"dateTime": "2026-03-10T09:00:00-05:00"},
        )

    # 09:00 EST = 14:00 UTC → instance suffix is 20260310T140000Z
    assert "bb-main-series_20260310T140000Z" in deleted

    # The parent mapping must still exist
    cursor = await db.execute(
        "SELECT id FROM event_mappings WHERE id = ?", (mapping_id,)
    )
    assert await cursor.fetchone() is not None


# ---------------------------------------------------------------------------
# sync_client_event_to_main – modified recurring instance
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_sync_client_event_modified_instance_forks_and_cancels_old_slots(test_db):
    """When a recurring instance is modified (new time), the old recurring slot
    should be cancelled before creating the standalone replacement."""
    from app.sync.rules import sync_client_event_to_main

    db = test_db
    user_id = await _insert_user(db)
    token_id = await _insert_token(db, user_id, "client@example.com")
    client_cal_db_id = await _insert_calendar(db, user_id, token_id, "client-cal-1")
    other_token_id = await _insert_token(db, user_id, "other@example.com")
    other_cal_db_id = await _insert_calendar(db, user_id, other_token_id, "other-cal-1")

    # Parent series already tracked
    mapping_id = await _insert_mapping(
        db, user_id, client_cal_db_id, "series-abc", "main-series-xyz",
        is_recurring=True,
    )
    await _insert_busy_block(db, mapping_id, other_cal_db_id, "bb-series-xyz")

    deleted: list[str] = []
    created: list[dict] = []

    new_main_event_id = "main-instance-new"

    def fake_delete(cal_id: str, event_id: str, **_kw) -> bool:
        deleted.append(event_id)
        return True

    def fake_create(cal_id: str, body: dict, **_kw) -> dict:
        created.append(body)
        body["id"] = new_main_event_id
        return body

    main_client = SimpleNamespace(
        delete_event=fake_delete,
        create_event=fake_create,
    )
    client_obj = SimpleNamespace(is_our_event=lambda _e: False)

    modified_instance = {
        "id": "series-abc_20260227T150000Z",
        "status": "confirmed",
        "recurringEventId": "series-abc",
        "originalStartTime": {"dateTime": "2026-02-27T10:00:00-05:00"},
        "summary": "Weekly Meeting (rescheduled)",
        "start": {"dateTime": "2026-02-27T11:00:00-05:00", "timeZone": "America/New_York"},
        "end": {"dateTime": "2026-02-27T12:00:00-05:00", "timeZone": "America/New_York"},
    }

    with patch(
        "app.auth.google.get_valid_access_token",
        AsyncMock(return_value="token"),
    ), patch(
        "app.sync.rules.GoogleCalendarClient",
    ) as MockClient:
        fake_other = MagicMock()
        fake_other.delete_event = fake_delete
        MockClient.return_value = fake_other

        result = await sync_client_event_to_main(
            client=client_obj,
            main_client=main_client,
            event=modified_instance,
            user_id=user_id,
            client_calendar_id=client_cal_db_id,
            main_calendar_id="main-cal",
            client_email="client@example.com",
        )

    # The old recurring slots should have been cancelled
    assert "main-series-xyz_20260227T150000Z" in deleted
    assert "bb-series-xyz_20260227T150000Z" in deleted

    # A new standalone event should have been created on main
    assert len(created) == 1
    assert result == new_main_event_id

    # A new instance-level mapping should exist in the DB
    cursor = await db.execute(
        """SELECT * FROM event_mappings
           WHERE user_id = ? AND origin_event_id = ?""",
        (user_id, "series-abc_20260227T150000Z"),
    )
    instance_row = await cursor.fetchone()
    assert instance_row is not None
    assert instance_row["main_event_id"] == new_main_event_id
    assert instance_row["origin_recurring_event_id"] == "series-abc"


@pytest.mark.asyncio
async def test_sync_client_event_modified_instance_no_parent_creates_standalone(test_db):
    """If the parent series is not tracked, treat the modified instance as a
    brand-new standalone event (existing behaviour, no crash)."""
    from app.sync.rules import sync_client_event_to_main

    db = test_db
    user_id = await _insert_user(db)
    token_id = await _insert_token(db, user_id, "client@example.com")
    client_cal_db_id = await _insert_calendar(db, user_id, token_id, "client-cal-1")

    created: list[dict] = []

    def fake_create(cal_id: str, body: dict, **_kw) -> dict:
        created.append(body)
        body["id"] = "main-new-standalone"
        return body

    main_client = SimpleNamespace(
        delete_event=MagicMock(),
        create_event=fake_create,
    )
    client_obj = SimpleNamespace(is_our_event=lambda _e: False)

    modified_instance = {
        "id": "unknown-series_20260301T150000Z",
        "status": "confirmed",
        "recurringEventId": "unknown-series",
        "originalStartTime": {"dateTime": "2026-03-01T10:00:00-05:00"},
        "summary": "Mystery Meeting",
        "start": {"dateTime": "2026-03-01T11:00:00-05:00"},
        "end": {"dateTime": "2026-03-01T12:00:00-05:00"},
    }

    result = await sync_client_event_to_main(
        client=client_obj,
        main_client=main_client,
        event=modified_instance,
        user_id=user_id,
        client_calendar_id=client_cal_db_id,
        main_calendar_id="main-cal",
        client_email="client@example.com",
    )

    # No old slots to cancel (no parent mapping)
    main_client.delete_event.assert_not_called()
    # A new standalone event should still be created
    assert result == "main-new-standalone"
    assert len(created) == 1
