"""Expanded tests for sync rule branch behavior."""

from __future__ import annotations

from datetime import datetime
from itertools import count
from types import SimpleNamespace

import pytest

from app.database import get_database


async def _insert_user(
    email: str = "user@example.com",
    google_user_id: str = "google-user-1",
) -> int:
    db = await get_database()
    cursor = await db.execute(
        """INSERT INTO users (email, google_user_id, display_name, main_calendar_id)
           VALUES (?, ?, ?, ?)
           RETURNING id""",
        (email, google_user_id, "User", "main-calendar"),
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
async def test_sync_client_event_to_main_skip_paths(test_db):
    """Client-origin sync should skip self-created and cancelled events."""
    from app.sync.rules import sync_client_event_to_main

    client = SimpleNamespace(is_our_event=lambda _event: True)
    main_client = SimpleNamespace()
    event = {"id": "evt-1", "start": {"dateTime": "2026-01-01T10:00:00Z"}, "end": {"dateTime": "2026-01-01T11:00:00Z"}}
    assert (
        await sync_client_event_to_main(
            client=client,
            main_client=main_client,
            event=event,
            user_id=1,
            client_calendar_id=1,
            main_calendar_id="main",
            client_email="user@example.com",
        )
        is None
    )

    client = SimpleNamespace(is_our_event=lambda _event: False)
    cancelled = {"id": "evt-2", "status": "cancelled"}
    assert (
        await sync_client_event_to_main(
            client=client,
            main_client=main_client,
            event=cancelled,
            user_id=1,
            client_calendar_id=1,
            main_calendar_id="main",
            client_email="user@example.com",
        )
        is None
    )


@pytest.mark.asyncio
async def test_sync_client_event_to_main_create_update_and_failure_paths(test_db):
    """Client-origin sync should create, update, and safely handle create failure."""
    from app.sync.rules import sync_client_event_to_main

    user_id = await _insert_user()
    token_id = await _insert_token(user_id, "client@example.com")
    calendar_id = await _insert_calendar(user_id, token_id, "client-cal-1")
    db = await get_database()

    class FakeMainClient:
        def __init__(self):
            self.update_calls = 0
            self.created = 0
            self.fail_update = False
            self.fail_create = False

        def create_event(self, _calendar_id: str, _event_data: dict):
            if self.fail_create:
                raise RuntimeError("create failed")
            self.created += 1
            return {"id": f"main-{self.created}"}

        def update_event(self, _calendar_id: str, _event_id: str, _event_data: dict):
            self.update_calls += 1
            if self.fail_update:
                raise RuntimeError("update failed")
            return {"id": _event_id}

    client = SimpleNamespace(is_our_event=lambda _event: False)
    main_client = FakeMainClient()

    event = {
        "id": "origin-1",
        "summary": "Client Event",
        "start": {"dateTime": "2026-01-01T10:00:00Z"},
        "end": {"dateTime": "2026-01-01T11:00:00Z"},
        "organizer": {"email": "user@example.com"},
    }

    created_main_id = await sync_client_event_to_main(
        client=client,
        main_client=main_client,
        event=event,
        user_id=user_id,
        client_calendar_id=calendar_id,
        main_calendar_id="main-cal",
        client_email="user@example.com",
    )
    assert created_main_id == "main-1"

    cursor = await db.execute(
        "SELECT main_event_id, user_can_edit FROM event_mappings WHERE user_id = ? AND origin_event_id = ?",
        (user_id, "origin-1"),
    )
    row = await cursor.fetchone()
    assert row["main_event_id"] == "main-1"
    assert row["user_can_edit"] == 1

    # Existing mapping update path with update failure fallback to create.
    main_client.fail_update = True
    updated_id = await sync_client_event_to_main(
        client=client,
        main_client=main_client,
        event={**event, "summary": "Changed"},
        user_id=user_id,
        client_calendar_id=calendar_id,
        main_calendar_id="main-cal",
        client_email="user@example.com",
    )
    assert updated_id == "main-2"

    cursor = await db.execute(
        "SELECT main_event_id FROM event_mappings WHERE user_id = ? AND origin_event_id = ?",
        (user_id, "origin-1"),
    )
    assert (await cursor.fetchone())["main_event_id"] == "main-2"

    # New event with create failure should return None and not create mapping.
    main_client.fail_create = True
    failed = await sync_client_event_to_main(
        client=client,
        main_client=main_client,
        event={**event, "id": "origin-fail"},
        user_id=user_id,
        client_calendar_id=calendar_id,
        main_calendar_id="main-cal",
        client_email="user@example.com",
    )
    assert failed is None
    cursor = await db.execute(
        "SELECT COUNT(*) FROM event_mappings WHERE user_id = ? AND origin_event_id = ?",
        (user_id, "origin-fail"),
    )
    assert (await cursor.fetchone())[0] == 0


@pytest.mark.asyncio
async def test_sync_main_event_to_clients_create_and_origin_skip_paths(test_db, monkeypatch):
    """Main-origin and client-origin events should create busy blocks on appropriate calendars."""
    from app.sync.rules import sync_main_event_to_clients

    user_id = await _insert_user(email="main@example.com", google_user_id="main-google")
    token_1 = await _insert_token(user_id, "c1@example.com")
    token_2 = await _insert_token(user_id, "c2@example.com")
    cal_1 = await _insert_calendar(user_id, token_1, "cal-1")
    cal_2 = await _insert_calendar(user_id, token_2, "cal-2")

    async def fake_get_valid_access_token(_user_id: int, _email: str) -> str:
        return "token"

    counter = count(1)

    class FakeGoogleClient:
        def __init__(self, _token: str):
            pass

        def create_event(self, _calendar_id: str, _event_data: dict):
            return {"id": f"busy-{next(counter)}"}

        def update_event(self, _calendar_id: str, _event_id: str, _event_data: dict):
            return {"id": _event_id}

        def delete_event(self, _calendar_id: str, _event_id: str):
            return True

    monkeypatch.setattr("app.auth.google.get_valid_access_token", fake_get_valid_access_token)
    monkeypatch.setattr("app.sync.rules.GoogleCalendarClient", FakeGoogleClient)

    main_client = SimpleNamespace(is_our_event=lambda _event: False)
    event = {
        "id": "main-evt-1",
        "status": "confirmed",
        "start": {"dateTime": "2026-01-01T10:00:00Z"},
        "end": {"dateTime": "2026-01-01T11:00:00Z"},
    }

    created = await sync_main_event_to_clients(
        main_client=main_client,
        event=event,
        user_id=user_id,
        main_calendar_id="main-cal",
        user_email="main@example.com",
    )
    assert len(created) == 2

    db = await get_database()
    cursor = await db.execute(
        "SELECT COUNT(*) FROM busy_blocks"
    )
    assert (await cursor.fetchone())[0] == 2

    # Client-origin mapping should skip origin calendar and only block other calendars.
    cursor = await db.execute(
        """INSERT INTO event_mappings
           (user_id, origin_type, origin_calendar_id, origin_event_id, main_event_id, is_recurring, user_can_edit)
           VALUES (?, 'client', ?, ?, ?, FALSE, TRUE)
           RETURNING id""",
        (user_id, cal_1, "origin-client", "main-client-evt"),
    )
    mapping_id = (await cursor.fetchone())["id"]
    await db.commit()

    created_for_client_origin = await sync_main_event_to_clients(
        main_client=main_client,
        event={**event, "id": "main-client-evt"},
        user_id=user_id,
        main_calendar_id="main-cal",
        user_email="main@example.com",
    )
    assert len(created_for_client_origin) == 1

    cursor = await db.execute(
        "SELECT client_calendar_id FROM busy_blocks WHERE event_mapping_id = ?",
        (mapping_id,),
    )
    rows = await cursor.fetchall()
    assert len(rows) == 1
    assert rows[0]["client_calendar_id"] == cal_2


@pytest.mark.asyncio
async def test_sync_main_event_to_clients_skip_and_existing_update_paths(test_db, monkeypatch):
    """Main-to-client sync should skip non-blocking events and update existing mappings."""
    from app.sync.rules import sync_main_event_to_clients

    user_id = await _insert_user(email="u@example.com", google_user_id="u-google")
    token = await _insert_token(user_id, "c1@example.com")
    cal_1 = await _insert_calendar(user_id, token, "cal-1")

    async def fake_get_valid_access_token(_user_id: int, _email: str) -> str:
        return "token"

    class FakeGoogleClient:
        def __init__(self, _token: str):
            self.updated = []

        def create_event(self, _calendar_id: str, _event_data: dict):
            return {"id": "busy-created"}

        def update_event(self, _calendar_id: str, _event_id: str, _event_data: dict):
            self.updated.append(_event_id)
            return {"id": _event_id}

        def delete_event(self, _calendar_id: str, _event_id: str):
            return True

    monkeypatch.setattr("app.auth.google.get_valid_access_token", fake_get_valid_access_token)
    monkeypatch.setattr("app.sync.rules.GoogleCalendarClient", FakeGoogleClient)

    # Skip own event.
    main_client = SimpleNamespace(is_our_event=lambda _event: True)
    assert (
        await sync_main_event_to_clients(
            main_client=main_client,
            event={"id": "main-1", "status": "confirmed", "start": {"dateTime": "x"}, "end": {"dateTime": "y"}},
            user_id=user_id,
            main_calendar_id="main",
            user_email="u@example.com",
        )
        == []
    )

    # Skip non-blocking event.
    main_client = SimpleNamespace(is_our_event=lambda _event: False)
    assert (
        await sync_main_event_to_clients(
            main_client=main_client,
            event={
                "id": "main-2",
                "status": "confirmed",
                "start": {"date": "2026-01-01"},
                "end": {"date": "2026-01-02"},
                "transparency": "transparent",
            },
            user_id=user_id,
            main_calendar_id="main",
            user_email="u@example.com",
        )
        == []
    )

    # Existing mapping + existing busy block update path.
    db = await get_database()
    cursor = await db.execute(
        """INSERT INTO event_mappings
           (user_id, origin_type, origin_event_id, main_event_id, is_recurring, user_can_edit)
           VALUES (?, 'main', ?, ?, FALSE, TRUE)
           RETURNING id""",
        (user_id, "main-3", "main-3"),
    )
    mapping_id = (await cursor.fetchone())["id"]
    await db.execute(
        "INSERT INTO busy_blocks (event_mapping_id, client_calendar_id, busy_block_event_id) VALUES (?, ?, ?)",
        (mapping_id, cal_1, "busy-old"),
    )
    await db.commit()

    created = await sync_main_event_to_clients(
        main_client=main_client,
        event={
            "id": "main-3",
            "status": "confirmed",
            "start": {"dateTime": "2026-01-01T10:00:00Z"},
            "end": {"dateTime": "2026-01-01T11:00:00Z"},
        },
        user_id=user_id,
        main_calendar_id="main",
        user_email="u@example.com",
    )
    assert created == []

    cursor = await db.execute("SELECT event_start, event_end FROM event_mappings WHERE id = ?", (mapping_id,))
    row = await cursor.fetchone()
    assert row["event_start"] == "2026-01-01T10:00:00Z"
    assert row["event_end"] == "2026-01-01T11:00:00Z"


@pytest.mark.asyncio
async def test_handle_deleted_client_event_paths(test_db, monkeypatch):
    """Deleted client events should clean up mappings and busy blocks safely."""
    from app.sync.rules import handle_deleted_client_event

    user_id = await _insert_user(email="del@example.com", google_user_id="del-google")
    token = await _insert_token(user_id, "client@example.com")
    cal_id = await _insert_calendar(user_id, token, "cal-del")
    db = await get_database()

    # No mapping -> no-op
    await handle_deleted_client_event(
        user_id=user_id,
        client_calendar_id=cal_id,
        event_id="missing",
        main_calendar_id="main",
        main_client=SimpleNamespace(delete_event=lambda *_args, **_kwargs: True),
    )

    # Non-recurring -> hard delete mapping
    cursor = await db.execute(
        """INSERT INTO event_mappings
           (user_id, origin_type, origin_calendar_id, origin_event_id, main_event_id, is_recurring, user_can_edit)
           VALUES (?, 'client', ?, ?, ?, FALSE, TRUE)
           RETURNING id""",
        (user_id, cal_id, "origin-1", "main-1"),
    )
    mapping_id = (await cursor.fetchone())["id"]
    await db.execute(
        "INSERT INTO busy_blocks (event_mapping_id, client_calendar_id, busy_block_event_id) VALUES (?, ?, ?)",
        (mapping_id, cal_id, "busy-1"),
    )
    await db.commit()

    async def fake_get_valid_access_token(_user_id: int, _email: str) -> str:
        return "token"

    class FakeGC:
        def __init__(self, _token: str):
            pass

        def delete_event(self, _calendar_id: str, _event_id: str):
            return True

    monkeypatch.setattr("app.auth.google.get_valid_access_token", fake_get_valid_access_token)
    monkeypatch.setattr("app.sync.rules.GoogleCalendarClient", FakeGC)

    await handle_deleted_client_event(
        user_id=user_id,
        client_calendar_id=cal_id,
        event_id="origin-1",
        main_calendar_id="main",
        main_client=SimpleNamespace(delete_event=lambda *_args, **_kwargs: True),
    )

    cursor = await db.execute("SELECT COUNT(*) FROM event_mappings WHERE id = ?", (mapping_id,))
    assert (await cursor.fetchone())[0] == 0

    # Recurring -> soft delete mapping
    cursor = await db.execute(
        """INSERT INTO event_mappings
           (user_id, origin_type, origin_calendar_id, origin_event_id, main_event_id, is_recurring, user_can_edit)
           VALUES (?, 'client', ?, ?, ?, TRUE, TRUE)
           RETURNING id""",
        (user_id, cal_id, "origin-2", "main-2"),
    )
    recurring_id = (await cursor.fetchone())["id"]
    await db.commit()

    await handle_deleted_client_event(
        user_id=user_id,
        client_calendar_id=cal_id,
        event_id="origin-2",
        main_calendar_id="main",
        main_client=SimpleNamespace(delete_event=lambda *_args, **_kwargs: True),
    )
    cursor = await db.execute("SELECT deleted_at FROM event_mappings WHERE id = ?", (recurring_id,))
    assert (await cursor.fetchone())["deleted_at"] is not None


@pytest.mark.asyncio
async def test_handle_deleted_main_event_paths(test_db, monkeypatch):
    """Deleted main events should clean up client-origin links and busy blocks."""
    from app.sync.rules import handle_deleted_main_event

    user_id = await _insert_user(email="main-del@example.com", google_user_id="main-del-google")
    token = await _insert_token(user_id, "client-origin@example.com")
    cal_id = await _insert_calendar(user_id, token, "cal-origin")
    db = await get_database()

    # No mapping -> no-op
    await handle_deleted_main_event(user_id=user_id, event_id="missing-main")

    cursor = await db.execute(
        """INSERT INTO event_mappings
           (user_id, origin_type, origin_calendar_id, origin_event_id, main_event_id, is_recurring, user_can_edit)
           VALUES (?, 'client', ?, ?, ?, FALSE, TRUE)
           RETURNING id""",
        (user_id, cal_id, "origin-client-event", "main-event-1"),
    )
    mapping_id = (await cursor.fetchone())["id"]
    await db.execute(
        "INSERT INTO busy_blocks (event_mapping_id, client_calendar_id, busy_block_event_id) VALUES (?, ?, ?)",
        (mapping_id, cal_id, "busy-main-1"),
    )
    await db.commit()

    async def fake_get_valid_access_token(_user_id: int, _email: str) -> str:
        return "token"

    class FakeGC:
        def __init__(self, _token: str):
            pass

        def delete_event(self, _calendar_id: str, _event_id: str):
            return True

    monkeypatch.setattr("app.auth.google.get_valid_access_token", fake_get_valid_access_token)
    monkeypatch.setattr("app.sync.rules.GoogleCalendarClient", FakeGC)

    await handle_deleted_main_event(user_id=user_id, event_id="main-event-1")

    cursor = await db.execute("SELECT COUNT(*) FROM event_mappings WHERE id = ?", (mapping_id,))
    assert (await cursor.fetchone())[0] == 0

    # Recurring mapping should be soft-deleted.
    cursor = await db.execute(
        """INSERT INTO event_mappings
           (user_id, origin_type, origin_event_id, main_event_id, is_recurring, user_can_edit)
           VALUES (?, 'main', ?, ?, TRUE, TRUE)
           RETURNING id""",
        (user_id, "main-origin-rec", "main-event-rec"),
    )
    recurring_id = (await cursor.fetchone())["id"]
    await db.commit()

    await handle_deleted_main_event(user_id=user_id, event_id="main-event-rec")
    cursor = await db.execute("SELECT deleted_at FROM event_mappings WHERE id = ?", (recurring_id,))
    assert (await cursor.fetchone())["deleted_at"] is not None
