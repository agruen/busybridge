"""Additional branch coverage tests for sync.rules."""

from __future__ import annotations

from itertools import count

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
async def test_sync_client_event_to_main_all_day_and_update_success(test_db):
    """Client->main sync should parse all-day dates and cover update-success path."""
    from app.sync.rules import sync_client_event_to_main

    user_id = await _insert_user()
    token_id = await _insert_token(user_id, "client@example.com")
    calendar_id = await _insert_calendar(user_id, token_id, "client-cal")
    db = await get_database()

    class FakeMainClient:
        def __init__(self):
            self.updated = []

        def create_event(self, _calendar_id: str, _event_data: dict):
            return {"id": "main-created"}

        def update_event(self, _calendar_id: str, event_id: str, _event_data: dict):
            self.updated.append(event_id)
            return {"id": event_id}

    main_client = FakeMainClient()
    client = type("Client", (), {"is_our_event": lambda self, _event: False})()

    all_day_event = {
        "id": "all-day-1",
        "summary": "All Day",
        "start": {"date": "2026-02-01"},
        "end": {"date": "2026-02-02"},
        "organizer": {"email": "client@example.com"},
    }

    first_main_id = await sync_client_event_to_main(
        client=client,
        main_client=main_client,
        event=all_day_event,
        user_id=user_id,
        client_calendar_id=calendar_id,
        main_calendar_id="main-cal",
        client_email="client@example.com",
    )
    assert first_main_id == "main-created"

    second_main_id = await sync_client_event_to_main(
        client=client,
        main_client=main_client,
        event=all_day_event,
        user_id=user_id,
        client_calendar_id=calendar_id,
        main_calendar_id="main-cal",
        client_email="client@example.com",
    )
    assert second_main_id == "main-created"
    assert main_client.updated == ["main-created"]

    cursor = await db.execute(
        """SELECT event_start, event_end, is_all_day
           FROM event_mappings
           WHERE user_id = ? AND origin_event_id = ?""",
        (user_id, "all-day-1"),
    )
    row = await cursor.fetchone()
    assert row["event_start"] == "2026-02-01"
    assert row["event_end"] == "2026-02-02"
    assert row["is_all_day"] == 1


@pytest.mark.asyncio
async def test_sync_main_event_to_clients_replacement_and_recurrence_paths(test_db, monkeypatch):
    """Main->client sync should copy recurrence and repoint busy-block mapping on replacement."""
    from app.sync.rules import sync_main_event_to_clients

    user_id = await _insert_user(email="main@example.com", google_user_id="main-google")
    token_id = await _insert_token(user_id, "client@example.com")
    cal_id = await _insert_calendar(user_id, token_id, "client-cal")
    db = await get_database()

    cursor = await db.execute(
        """INSERT INTO event_mappings
           (user_id, origin_type, origin_event_id, main_event_id, is_recurring, user_can_edit)
           VALUES (?, 'main', ?, ?, TRUE, TRUE)
           RETURNING id""",
        (user_id, "main-rec-1", "main-rec-1"),
    )
    mapping_id = (await cursor.fetchone())["id"]
    await db.execute(
        """INSERT INTO busy_blocks (event_mapping_id, client_calendar_id, busy_block_event_id)
           VALUES (?, ?, ?)""",
        (mapping_id, cal_id, "busy-old"),
    )
    await db.commit()

    async def fake_get_valid_access_token(_user_id: int, _email: str) -> str:
        return "token"

    captures = {"created_body": None}

    class ReplacementClient:
        def __init__(self, _token: str):
            pass

        def update_event(self, _calendar_id: str, _event_id: str, _event_data: dict):
            raise RuntimeError("update failed")

        def create_event(self, _calendar_id: str, event_data: dict):
            captures["created_body"] = event_data
            return {"id": "busy-replacement"}

        def delete_event(self, _calendar_id: str, _event_id: str):
            raise RuntimeError("delete old failed")

    monkeypatch.setattr("app.auth.google.get_valid_access_token", fake_get_valid_access_token)
    monkeypatch.setattr("app.sync.rules.GoogleCalendarClient", ReplacementClient)

    main_client = type("MainClient", (), {"is_our_event": lambda self, _event: False})()
    event = {
        "id": "main-rec-1",
        "status": "confirmed",
        "start": {"date": "2026-03-01"},
        "end": {"date": "2026-03-02"},
        "recurrence": ["RRULE:FREQ=DAILY;COUNT=2"],
    }

    created = await sync_main_event_to_clients(
        main_client=main_client,
        event=event,
        user_id=user_id,
        main_calendar_id="main-cal",
        user_email="main@example.com",
    )
    assert created == ["busy-replacement"]
    assert captures["created_body"]["recurrence"] == ["RRULE:FREQ=DAILY;COUNT=2"]
    assert captures["created_body"]["start"]["date"] == "2026-03-01"
    assert captures["created_body"]["end"]["date"] == "2026-03-02"

    cursor = await db.execute(
        """SELECT busy_block_event_id FROM busy_blocks
           WHERE event_mapping_id = ? AND client_calendar_id = ?""",
        (mapping_id, cal_id),
    )
    row = await cursor.fetchone()
    assert row["busy_block_event_id"] == "busy-replacement"


@pytest.mark.asyncio
async def test_sync_main_event_to_clients_per_calendar_failure_path(test_db, monkeypatch):
    """Main->client sync should tolerate per-calendar errors and continue."""
    from app.sync.rules import sync_main_event_to_clients

    user_id = await _insert_user(email="multi@example.com", google_user_id="multi-google")
    token_1 = await _insert_token(user_id, "fail@example.com")
    token_2 = await _insert_token(user_id, "ok@example.com")
    cal_1 = await _insert_calendar(user_id, token_1, "cal-fail")
    cal_2 = await _insert_calendar(user_id, token_2, "cal-ok")

    async def fake_get_valid_access_token(_user_id: int, email: str) -> str:
        if email == "fail@example.com":
            raise RuntimeError("token failed")
        return "token-ok"

    ids = count(1)

    class CreateOnlyClient:
        def __init__(self, _token: str):
            pass

        def create_event(self, _calendar_id: str, _event_data: dict):
            return {"id": f"busy-{next(ids)}"}

    monkeypatch.setattr("app.auth.google.get_valid_access_token", fake_get_valid_access_token)
    monkeypatch.setattr("app.sync.rules.GoogleCalendarClient", CreateOnlyClient)

    main_client = type("MainClient", (), {"is_our_event": lambda self, _event: False})()
    event = {
        "id": "main-err-path",
        "status": "confirmed",
        "start": {"dateTime": "2026-04-01T10:00:00Z"},
        "end": {"dateTime": "2026-04-01T11:00:00Z"},
    }

    created = await sync_main_event_to_clients(
        main_client=main_client,
        event=event,
        user_id=user_id,
        main_calendar_id="main",
        user_email="multi@example.com",
    )

    assert created == ["busy-1"]
    db = await get_database()
    cursor = await db.execute(
        """SELECT COUNT(*) FROM busy_blocks
           WHERE client_calendar_id IN (?, ?)""",
        (cal_1, cal_2),
    )
    assert (await cursor.fetchone())[0] == 1


@pytest.mark.asyncio
async def test_handle_deleted_client_event_error_logging_paths(test_db, monkeypatch):
    """Deleted client event handler should survive main/busy-block deletion errors."""
    from app.sync.rules import handle_deleted_client_event

    user_id = await _insert_user(email="del-client@example.com", google_user_id="del-client-google")
    token_id = await _insert_token(user_id, "del-client-token@example.com")
    cal_id = await _insert_calendar(user_id, token_id, "del-client-cal")
    db = await get_database()

    cursor = await db.execute(
        """INSERT INTO event_mappings
           (user_id, origin_type, origin_calendar_id, origin_event_id, main_event_id, is_recurring, user_can_edit)
           VALUES (?, 'client', ?, ?, ?, FALSE, TRUE)
           RETURNING id""",
        (user_id, cal_id, "origin-delete-err", "main-delete-err"),
    )
    mapping_id = (await cursor.fetchone())["id"]
    await db.execute(
        """INSERT INTO busy_blocks (event_mapping_id, client_calendar_id, busy_block_event_id)
           VALUES (?, ?, ?)""",
        (mapping_id, cal_id, "busy-delete-err"),
    )
    await db.commit()

    async def fake_get_valid_access_token(_user_id: int, _email: str) -> str:
        return "token"

    class FailingDeleteClient:
        def __init__(self, _token: str):
            pass

        def delete_event(self, *_args, **_kwargs):
            raise RuntimeError("busy delete failed")

    class FailingMainClient:
        def delete_event(self, *_args, **_kwargs):
            raise RuntimeError("main delete failed")

    monkeypatch.setattr("app.auth.google.get_valid_access_token", fake_get_valid_access_token)
    monkeypatch.setattr("app.sync.rules.GoogleCalendarClient", FailingDeleteClient)

    await handle_deleted_client_event(
        user_id=user_id,
        client_calendar_id=cal_id,
        event_id="origin-delete-err",
        main_calendar_id="main-cal",
        main_client=FailingMainClient(),
    )

    cursor = await db.execute("SELECT COUNT(*) FROM event_mappings WHERE id = ?", (mapping_id,))
    assert (await cursor.fetchone())[0] == 0


@pytest.mark.asyncio
async def test_handle_deleted_main_event_error_logging_paths(test_db, monkeypatch):
    """Deleted main event handler should survive client and busy-block deletion errors."""
    from app.sync.rules import handle_deleted_main_event

    user_id = await _insert_user(email="del-main@example.com", google_user_id="del-main-google")
    token_id = await _insert_token(user_id, "del-main-client@example.com")
    cal_id = await _insert_calendar(user_id, token_id, "del-main-cal")
    db = await get_database()

    cursor = await db.execute(
        """INSERT INTO event_mappings
           (user_id, origin_type, origin_calendar_id, origin_event_id, main_event_id, is_recurring, user_can_edit)
           VALUES (?, 'client', ?, ?, ?, FALSE, TRUE)
           RETURNING id""",
        (user_id, cal_id, "origin-main-delete", "main-delete"),
    )
    mapping_id = (await cursor.fetchone())["id"]
    await db.execute(
        """INSERT INTO busy_blocks (event_mapping_id, client_calendar_id, busy_block_event_id)
           VALUES (?, ?, ?)""",
        (mapping_id, cal_id, "busy-main-delete"),
    )
    await db.commit()

    async def fake_get_valid_access_token(_user_id: int, _email: str) -> str:
        return "token"

    class FailingDeleteClient:
        def __init__(self, _token: str):
            pass

        def delete_event(self, *_args, **_kwargs):
            raise RuntimeError("delete failed")

    monkeypatch.setattr("app.auth.google.get_valid_access_token", fake_get_valid_access_token)
    monkeypatch.setattr("app.sync.rules.GoogleCalendarClient", FailingDeleteClient)

    await handle_deleted_main_event(user_id=user_id, event_id="main-delete")

    cursor = await db.execute("SELECT COUNT(*) FROM event_mappings WHERE id = ?", (mapping_id,))
    assert (await cursor.fetchone())[0] == 0
