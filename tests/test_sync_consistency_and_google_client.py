"""Tests for sync consistency logic and Google Calendar client wrapper."""

from __future__ import annotations

import sys
import types
from datetime import datetime, timedelta
from types import SimpleNamespace

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


async def _insert_calendar(user_id: int, oauth_token_id: int, google_calendar_id: str, is_active: bool = True) -> int:
    db = await get_database()
    cursor = await db.execute(
        """INSERT INTO client_calendars
           (user_id, oauth_token_id, google_calendar_id, display_name, is_active)
           VALUES (?, ?, ?, ?, ?)
           RETURNING id""",
        (user_id, oauth_token_id, google_calendar_id, google_calendar_id, is_active),
    )
    row = await cursor.fetchone()
    await db.commit()
    return row["id"]


@pytest.mark.asyncio
async def test_google_calendar_client_init_and_list_events_pagination(monkeypatch):
    """Client wrapper should initialize and paginate list_events correctly."""
    from app.sync.google_calendar import GoogleCalendarClient

    class FakeCredentials:
        def __init__(self, token: str):
            self.token = token

        def authorize(self, http):
            return http

    class FakeEvents:
        def __init__(self):
            self.calls = []

        def list(self, **kwargs):
            self.calls.append(dict(kwargs))
            if kwargs.get("pageToken") == "page-2":
                return SimpleNamespace(
                    execute=lambda: {"items": [{"id": "e2"}], "nextSyncToken": "sync-2"}
                )
            return SimpleNamespace(
                execute=lambda: {"items": [{"id": "e1"}], "nextPageToken": "page-2"}
            )

    class FakeService:
        def __init__(self):
            self.events_api = FakeEvents()

        def events(self):
            return self.events_api

    fake_service = FakeService()
    fake_http_module = types.SimpleNamespace(Http=lambda timeout=30: SimpleNamespace(timeout=timeout))

    monkeypatch.setattr("app.sync.google_calendar.Credentials", FakeCredentials)
    monkeypatch.setattr("app.sync.google_calendar.build", lambda *_args, **_kwargs: fake_service)
    monkeypatch.setitem(sys.modules, "httplib2", fake_http_module)

    client = GoogleCalendarClient("access-token")
    result = client.list_events("cal-1", sync_token="sync-1")
    assert [e["id"] for e in result["events"]] == ["e1", "e2"]
    assert result["next_sync_token"] == "sync-2"
    assert fake_service.events_api.calls[0]["syncToken"] == "sync-1"

    # Full sync should set time range parameters.
    full = client.list_events("cal-1", sync_token=None)
    assert "timeMin" in fake_service.events_api.calls[-1]
    assert "timeMax" in fake_service.events_api.calls[-1]
    assert full["events"]


@pytest.mark.asyncio
async def test_google_calendar_client_http_error_paths(monkeypatch):
    """Google client should map specific HTTP error statuses to safe behavior."""
    from app.sync import google_calendar as module
    from app.sync.google_calendar import GoogleCalendarClient

    class FakeHttpError(Exception):
        def __init__(self, status: int):
            self.resp = SimpleNamespace(status=status)

    class FakeEvents:
        def __init__(self, status: int):
            self.status = status

        def list(self, **_kwargs):
            def _raise():
                raise FakeHttpError(self.status)

            return SimpleNamespace(execute=_raise)

        def get(self, **_kwargs):
            def _raise():
                raise FakeHttpError(self.status)

            return SimpleNamespace(execute=_raise)

        def delete(self, **_kwargs):
            def _raise():
                raise FakeHttpError(self.status)

            return SimpleNamespace(execute=_raise)

    class FakeService:
        def __init__(self, status: int):
            self.status = status

        def events(self):
            return FakeEvents(self.status)

        def calendars(self):
            return SimpleNamespace(
                get=lambda **_kwargs: SimpleNamespace(execute=lambda: (_ for _ in ()).throw(FakeHttpError(self.status)))
            )

    client = object.__new__(GoogleCalendarClient)
    client.settings = SimpleNamespace(calendar_sync_tag="calendarSyncEngine", busy_block_title="Busy")

    monkeypatch.setattr(module, "HttpError", FakeHttpError)

    client.service = FakeService(410)
    assert client.list_events("cal")["sync_token_expired"] is True
    assert client.delete_event("cal", "evt") is True

    client.service = FakeService(403)
    with pytest.raises(PermissionError):
        client.list_events("cal")

    client.service = FakeService(404)
    with pytest.raises(FileNotFoundError):
        client.list_events("cal")
    assert client.get_event("cal", "evt") is None
    assert client.delete_event("cal", "evt") is True
    assert client.get_calendar("cal") is None

    client.service = FakeService(500)
    with pytest.raises(FakeHttpError):
        client.list_events("cal")
    with pytest.raises(FakeHttpError):
        client.get_event("cal", "evt")
    with pytest.raises(FakeHttpError):
        client.delete_event("cal", "evt")
    with pytest.raises(FakeHttpError):
        client.get_calendar("cal")


@pytest.mark.asyncio
async def test_google_calendar_client_event_crud_and_helpers():
    """CRUD helpers should pass through and tag events created by the sync engine."""
    from app.sync.google_calendar import GoogleCalendarClient

    class FakeEvents:
        def __init__(self):
            self.insert_body = None

        def insert(self, **kwargs):
            self.insert_body = kwargs["body"]
            return SimpleNamespace(execute=lambda: {"id": "new-id"})

        def update(self, **_kwargs):
            return SimpleNamespace(execute=lambda: {"id": "updated"})

        def patch(self, **_kwargs):
            return SimpleNamespace(execute=lambda: {"id": "patched"})

        def delete(self, **_kwargs):
            return SimpleNamespace(execute=lambda: {})

    class FakeService:
        def __init__(self):
            self.events_api = FakeEvents()

        def events(self):
            return self.events_api

        def calendarList(self):
            return SimpleNamespace(list=lambda: SimpleNamespace(execute=lambda: {"items": [{"id": "cal"}]}))

        def calendars(self):
            return SimpleNamespace(get=lambda **_kwargs: SimpleNamespace(execute=lambda: {"id": "cal"}))

    client = object.__new__(GoogleCalendarClient)
    client.settings = SimpleNamespace(calendar_sync_tag="syncTag", busy_block_title="Busy")
    client.service = FakeService()

    event_data = {"summary": "Meeting"}
    created = client.create_event("cal", event_data)
    assert created["id"] == "new-id"
    assert client.service.events_api.insert_body["extendedProperties"]["private"]["syncTag"] == "true"

    assert client.update_event("cal", "evt", {"summary": "x"})["id"] == "updated"
    assert client.patch_event("cal", "evt", {"summary": "x"})["id"] == "patched"
    assert client.delete_event("cal", "evt") is True
    assert client.list_calendars() == [{"id": "cal"}]
    assert client.get_calendar("cal") == {"id": "cal"}
    assert client.is_our_event({"extendedProperties": {"private": {"syncTag": "true"}}}) is True
    assert client.is_our_event({"extendedProperties": {"private": {"syncTag": "false"}}}) is False


@pytest.mark.asyncio
async def test_run_consistency_check_and_user_error_path(test_db, monkeypatch):
    """Top-level consistency check should count users and tolerate per-user failures."""
    from app.sync.consistency import run_consistency_check

    user_1 = await _insert_user(email="u1@example.com", google_user_id="u1-google", main_calendar_id="main-1")
    await _insert_user(email="u2@example.com", google_user_id="u2-google", main_calendar_id="main-2")

    async def fake_check_user_consistency(user_id: int, summary: dict):
        if user_id == user_1:
            raise RuntimeError("boom")
        summary["mappings_checked"] += 1

    monkeypatch.setattr("app.sync.consistency.check_user_consistency", fake_check_user_consistency)
    summary = await run_consistency_check()
    assert summary["users_checked"] == 2
    assert summary["errors"] == 1
    assert summary["mappings_checked"] == 1


@pytest.mark.asyncio
async def test_check_user_consistency_handles_origin_delete_recreate_and_stale_busy_block(test_db, monkeypatch):
    """Per-user consistency should repair mappings and clean stale busy blocks."""
    from app.sync.consistency import check_user_consistency

    user_id = await _insert_user(email="main@example.com", google_user_id="main-google", main_calendar_id="main-cal")
    token_id = await _insert_token(user_id, "client@example.com")
    client_calendar_id = await _insert_calendar(user_id, token_id, "client-cal", is_active=True)
    db = await get_database()

    # Mapping where origin event is gone -> should delete mapping and count orphaned main event delete.
    cursor = await db.execute(
        """INSERT INTO event_mappings
           (user_id, origin_type, origin_calendar_id, origin_event_id, main_event_id, is_recurring, user_can_edit)
           VALUES (?, 'client', ?, ?, ?, FALSE, TRUE)
           RETURNING id""",
        (user_id, client_calendar_id, "origin-deleted", "main-delete"),
    )
    mapping_delete_id = (await cursor.fetchone())["id"]

    # Mapping where origin exists but main copy missing -> should recreate.
    cursor = await db.execute(
        """INSERT INTO event_mappings
           (user_id, origin_type, origin_calendar_id, origin_event_id, main_event_id, is_recurring, user_can_edit)
           VALUES (?, 'client', ?, ?, ?, FALSE, TRUE)
           RETURNING id""",
        (user_id, client_calendar_id, "origin-live", "main-missing"),
    )
    mapping_recreate_id = (await cursor.fetchone())["id"]

    # Mapping soft-deleted with busy block -> should remove stale busy block.
    cursor = await db.execute(
        """INSERT INTO event_mappings
           (user_id, origin_type, origin_calendar_id, origin_event_id, main_event_id, deleted_at, is_recurring, user_can_edit)
           VALUES (?, 'client', ?, ?, ?, ?, FALSE, TRUE)
           RETURNING id""",
        (
            user_id,
            client_calendar_id,
            "origin-old",
            "main-old",
            (datetime.utcnow() - timedelta(days=1)).isoformat(),
        ),
    )
    mapping_deleted_id = (await cursor.fetchone())["id"]

    await db.execute(
        "INSERT INTO busy_blocks (event_mapping_id, client_calendar_id, busy_block_event_id) VALUES (?, ?, ?)",
        (mapping_delete_id, client_calendar_id, "busy-delete"),
    )
    await db.execute(
        "INSERT INTO busy_blocks (event_mapping_id, client_calendar_id, busy_block_event_id) VALUES (?, ?, ?)",
        (mapping_deleted_id, client_calendar_id, "busy-stale"),
    )
    await db.commit()

    async def fake_get_valid_access_token(_user_id: int, _email: str) -> str:
        return "token"

    class FakeGoogleClient:
        def __init__(self, _token: str):
            self.deleted = []

        def get_event(self, calendar_id: str, event_id: str):
            if event_id == "origin-deleted":
                return None
            if event_id == "origin-live":
                return {
                    "summary": "Origin Event",
                    "start": {"dateTime": "2026-01-01T10:00:00Z"},
                    "end": {"dateTime": "2026-01-01T11:00:00Z"},
                }
            if event_id == "main-missing":
                return None
            return {"id": event_id}

        def delete_event(self, calendar_id: str, event_id: str):
            self.deleted.append((calendar_id, event_id))
            return True

        def create_event(self, calendar_id: str, event_data: dict):
            return {"id": "main-recreated"}

    monkeypatch.setattr("app.auth.google.get_valid_access_token", fake_get_valid_access_token)
    monkeypatch.setattr("app.sync.consistency.GoogleCalendarClient", FakeGoogleClient)

    summary = {
        "users_checked": 0,
        "mappings_checked": 0,
        "orphaned_main_events_deleted": 0,
        "missing_copies_recreated": 0,
        "orphaned_busy_blocks_deleted": 0,
        "errors": 0,
    }
    await check_user_consistency(user_id, summary)

    assert summary["mappings_checked"] == 2
    assert summary["orphaned_main_events_deleted"] == 1
    assert summary["missing_copies_recreated"] == 1
    assert summary["orphaned_busy_blocks_deleted"] == 1

    cursor = await db.execute("SELECT COUNT(*) FROM event_mappings WHERE id = ?", (mapping_delete_id,))
    assert (await cursor.fetchone())[0] == 0

    cursor = await db.execute("SELECT main_event_id FROM event_mappings WHERE id = ?", (mapping_recreate_id,))
    assert (await cursor.fetchone())["main_event_id"] == "main-recreated"

    cursor = await db.execute("SELECT COUNT(*) FROM busy_blocks WHERE event_mapping_id = ?", (mapping_deleted_id,))
    assert (await cursor.fetchone())[0] == 0


@pytest.mark.asyncio
async def test_check_user_consistency_handles_missing_user_or_token_failure(test_db, monkeypatch):
    """Consistency check should no-op safely when prerequisites are missing."""
    from app.sync.consistency import check_user_consistency

    # Missing user
    summary = {
        "users_checked": 0,
        "mappings_checked": 0,
        "orphaned_main_events_deleted": 0,
        "missing_copies_recreated": 0,
        "orphaned_busy_blocks_deleted": 0,
        "errors": 0,
    }
    await check_user_consistency(999, summary)
    assert summary["errors"] == 0

    # User exists but token fetch fails
    user_id = await _insert_user(email="tokenfail@example.com", google_user_id="tokenfail-google", main_calendar_id="main")

    async def failing_get_token(_user_id: int, _email: str) -> str:
        raise RuntimeError("token unavailable")

    monkeypatch.setattr("app.auth.google.get_valid_access_token", failing_get_token)
    await check_user_consistency(user_id, summary)
    assert summary["errors"] == 0


@pytest.mark.asyncio
async def test_reconcile_calendar_paths(test_db, monkeypatch):
    """Reconcile should return defaults when missing and remove stale mappings when present."""
    from app.sync.consistency import reconcile_calendar

    # Missing calendar
    missing = await reconcile_calendar(12345)
    assert missing["events_found"] == 0

    user_id = await _insert_user(email="rec@example.com", google_user_id="rec-google", main_calendar_id="main")
    token_id = await _insert_token(user_id, "rec-client@example.com")
    calendar_id = await _insert_calendar(user_id, token_id, "rec-client-cal", is_active=True)

    db = await get_database()
    await db.execute(
        """INSERT INTO event_mappings
           (user_id, origin_type, origin_calendar_id, origin_event_id, main_event_id, is_recurring, user_can_edit)
           VALUES (?, 'client', ?, 'keep-event', 'main-keep', FALSE, TRUE)""",
        (user_id, calendar_id),
    )
    await db.execute(
        """INSERT INTO event_mappings
           (user_id, origin_type, origin_calendar_id, origin_event_id, main_event_id, is_recurring, user_can_edit)
           VALUES (?, 'client', ?, 'stale-event', 'main-stale', FALSE, TRUE)""",
        (user_id, calendar_id),
    )
    await db.commit()

    async def fake_get_valid_access_token(_user_id: int, _email: str) -> str:
        return "token"

    class FakeGoogleClient:
        def __init__(self, _token: str):
            pass

        def list_events(self, _calendar_id: str):
            return {
                "events": [
                    {"id": "keep-event", "status": "confirmed"},
                    {"id": "cancelled-event", "status": "cancelled"},
                    {"id": "ours-event", "status": "confirmed"},
                ]
            }

        def is_our_event(self, event: dict) -> bool:
            return event["id"] == "ours-event"

        def delete_event(self, _calendar_id: str, _event_id: str):
            return True

    monkeypatch.setattr("app.auth.google.get_valid_access_token", fake_get_valid_access_token)
    monkeypatch.setattr("app.sync.consistency.GoogleCalendarClient", FakeGoogleClient)

    summary = await reconcile_calendar(calendar_id)
    assert summary["events_found"] == 3
    assert summary["stale_mappings_removed"] == 1

    cursor = await db.execute(
        "SELECT COUNT(*) FROM event_mappings WHERE origin_calendar_id = ? AND origin_event_id = 'stale-event'",
        (calendar_id,),
    )
    assert (await cursor.fetchone())[0] == 0

    # Exception path should still return summary.
    async def failing_get_token(_user_id: int, _email: str) -> str:
        raise RuntimeError("bad token")

    monkeypatch.setattr("app.auth.google.get_valid_access_token", failing_get_token)
    errored = await reconcile_calendar(calendar_id)
    assert errored["events_found"] == 0

