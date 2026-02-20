"""Additional branch coverage tests for sync.engine."""

from __future__ import annotations

import json
from datetime import datetime, timedelta

import pytest

from app.database import get_database, set_setting


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


async def _insert_token(user_id: int, email: str, account_type: str = "client") -> int:
    db = await get_database()
    cursor = await db.execute(
        """INSERT INTO oauth_tokens
           (user_id, account_type, google_account_email, access_token_encrypted, refresh_token_encrypted)
           VALUES (?, ?, ?, ?, ?)
           RETURNING id""",
        (user_id, account_type, email, b"a", b"r"),
    )
    row = await cursor.fetchone()
    await db.commit()
    return row["id"]


async def _insert_calendar(
    user_id: int,
    oauth_token_id: int,
    google_calendar_id: str,
    is_active: bool = True,
) -> int:
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
async def test_trigger_sync_for_calendar_missing_and_user_without_main(test_db):
    """Calendar sync should safely no-op for missing calendar and missing main calendar."""
    from app.sync.engine import trigger_sync_for_calendar

    await trigger_sync_for_calendar(999999)

    user_id = await _insert_user(
        email="no-main@example.com",
        google_user_id="no-main-google",
        main_calendar_id=None,
    )
    token_id = await _insert_token(user_id, "no-main-client@example.com")
    cal_id = await _insert_calendar(user_id, token_id, "no-main-client-cal")
    await trigger_sync_for_calendar(cal_id)

    db = await get_database()
    cursor = await db.execute("SELECT COUNT(*) FROM sync_log")
    assert (await cursor.fetchone())[0] == 0


@pytest.mark.asyncio
async def test_trigger_sync_for_calendar_token_expired_success_paths(test_db, monkeypatch):
    """Calendar sync should handle token-expired re-fetch, cancelled events, and success logging."""
    from app.sync.engine import trigger_sync_for_calendar

    user_id = await _insert_user(email="main@example.com", google_user_id="main-google", main_calendar_id="main-cal")
    await _insert_token(user_id, "main@example.com", account_type="home")
    client_token_id = await _insert_token(user_id, "client@example.com")
    client_calendar_id = await _insert_calendar(user_id, client_token_id, "client-cal")

    db = await get_database()
    await db.execute(
        """INSERT INTO calendar_sync_state (client_calendar_id, sync_token, consecutive_failures)
           VALUES (?, ?, 0)""",
        (client_calendar_id, "old-sync-token"),
    )
    await db.commit()

    async def fake_get_valid_access_token(_user_id: int, email: str) -> str:
        if email == "client@example.com":
            return "client-token"
        return "main-token"

    state = {"client_list_calls": 0}

    class FakeGoogleCalendarClient:
        def __init__(self, token: str):
            self.token = token

        def list_events(self, _calendar_id: str, sync_token: str | None = None):
            if self.token != "client-token":
                return {"events": [], "next_sync_token": "unused"}
            state["client_list_calls"] += 1
            if state["client_list_calls"] == 1:
                assert sync_token == "old-sync-token"
                return {"events": [], "sync_token_expired": True}
            return {
                "events": [
                    {"id": "deleted-1", "status": "cancelled"},
                    {
                        "id": "active-1",
                        "status": "confirmed",
                        "start": {"dateTime": "2026-01-01T10:00:00Z"},
                        "end": {"dateTime": "2026-01-01T11:00:00Z"},
                    },
                ],
                "next_sync_token": "next-sync-token",
            }

        def get_event(self, _calendar_id: str, event_id: str):
            return {
                "id": event_id,
                "status": "confirmed",
                "start": {"dateTime": "2026-01-01T10:00:00Z"},
                "end": {"dateTime": "2026-01-01T11:00:00Z"},
            }

    deleted_calls: list[str] = []
    client_to_main_calls: list[str] = []
    main_to_clients_calls: list[str] = []

    async def fake_handle_deleted_client_event(**kwargs):
        deleted_calls.append(kwargs["event_id"])

    async def fake_sync_client_event_to_main(**kwargs):
        client_to_main_calls.append(kwargs["event"]["id"])
        return "main-event-1"

    async def fake_sync_main_event_to_clients(**kwargs):
        main_to_clients_calls.append(kwargs["event"]["id"])
        return []

    monkeypatch.setattr("app.auth.google.get_valid_access_token", fake_get_valid_access_token)
    monkeypatch.setattr("app.sync.engine.GoogleCalendarClient", FakeGoogleCalendarClient)
    monkeypatch.setattr("app.sync.engine.handle_deleted_client_event", fake_handle_deleted_client_event)
    monkeypatch.setattr("app.sync.engine.sync_client_event_to_main", fake_sync_client_event_to_main)
    monkeypatch.setattr("app.sync.engine.sync_main_event_to_clients", fake_sync_main_event_to_clients)

    await trigger_sync_for_calendar(client_calendar_id)

    assert deleted_calls == ["deleted-1"]
    assert client_to_main_calls == ["active-1"]
    assert main_to_clients_calls == ["main-event-1"]

    cursor = await db.execute(
        """SELECT sync_token, consecutive_failures, last_error
           FROM calendar_sync_state WHERE client_calendar_id = ?""",
        (client_calendar_id,),
    )
    sync_row = await cursor.fetchone()
    assert sync_row["sync_token"] == "next-sync-token"
    assert sync_row["consecutive_failures"] == 0
    assert sync_row["last_error"] is None

    cursor = await db.execute(
        """SELECT status, details FROM sync_log
           WHERE user_id = ? AND calendar_id = ?
           ORDER BY id DESC LIMIT 1""",
        (user_id, client_calendar_id),
    )
    log_row = await cursor.fetchone()
    assert log_row["status"] == "success"
    details = json.loads(log_row["details"])
    assert details["synced"] == 1
    assert details["deleted"] == 1


@pytest.mark.asyncio
async def test_trigger_sync_for_calendar_failure_queues_alert_at_threshold(test_db, monkeypatch):
    """Calendar sync failure should increment failures and queue alert when threshold is reached."""
    from app.sync.engine import trigger_sync_for_calendar

    user_id = await _insert_user(email="alert@example.com", google_user_id="alert-google", main_calendar_id="main")
    await _insert_token(user_id, "alert@example.com", account_type="home")
    client_token_id = await _insert_token(user_id, "alert-client@example.com")
    client_calendar_id = await _insert_calendar(user_id, client_token_id, "alert-client-cal")

    db = await get_database()
    await db.execute(
        """INSERT INTO calendar_sync_state
           (client_calendar_id, sync_token, consecutive_failures)
           VALUES (?, ?, 4)""",
        (client_calendar_id, "old-token"),
    )
    await db.commit()

    async def fake_get_valid_access_token(_user_id: int, _email: str) -> str:
        return "token"

    class ExplodingGoogleCalendarClient:
        def __init__(self, _token: str):
            pass

        def list_events(self, *_args, **_kwargs):
            raise RuntimeError("list failed")

    queued: list[dict] = []

    async def fake_queue_alert(**kwargs):
        queued.append(kwargs)

    monkeypatch.setattr("app.auth.google.get_valid_access_token", fake_get_valid_access_token)
    monkeypatch.setattr("app.sync.engine.GoogleCalendarClient", ExplodingGoogleCalendarClient)
    monkeypatch.setattr("app.alerts.email.queue_alert", fake_queue_alert)

    await trigger_sync_for_calendar(client_calendar_id)

    cursor = await db.execute(
        """SELECT sync_token, consecutive_failures, last_error
           FROM calendar_sync_state WHERE client_calendar_id = ?""",
        (client_calendar_id,),
    )
    row = await cursor.fetchone()
    assert row["sync_token"] == "old-token"
    assert row["consecutive_failures"] == 5
    assert "list failed" in (row["last_error"] or "")

    assert len(queued) == 1
    assert queued[0]["alert_type"] == "sync_failures"
    assert queued[0]["user_id"] == user_id
    assert queued[0]["calendar_id"] == client_calendar_id


@pytest.mark.asyncio
async def test_trigger_sync_for_calendar_full_sync_state_update_path(test_db, monkeypatch):
    """Calendar sync should execute full-sync state update path when sync token is missing."""
    from app.sync.engine import trigger_sync_for_calendar

    user_id = await _insert_user(email="fullsync@example.com", google_user_id="fullsync-google", main_calendar_id="main")
    await _insert_token(user_id, "fullsync@example.com", account_type="home")
    client_token_id = await _insert_token(user_id, "fullsync-client@example.com")
    client_calendar_id = await _insert_calendar(user_id, client_token_id, "fullsync-client-cal")

    db = await get_database()
    await db.execute(
        """INSERT INTO calendar_sync_state (client_calendar_id, sync_token, consecutive_failures)
           VALUES (?, NULL, 2)""",
        (client_calendar_id,),
    )
    await db.commit()

    async def fake_get_valid_access_token(_user_id: int, _email: str) -> str:
        return "token"

    class FakeGoogleCalendarClient:
        def __init__(self, _token: str):
            pass

        def list_events(self, _calendar_id: str, sync_token: str | None = None):
            assert sync_token is None
            return {"events": [], "next_sync_token": "full-sync-token"}

    monkeypatch.setattr("app.auth.google.get_valid_access_token", fake_get_valid_access_token)
    monkeypatch.setattr("app.sync.engine.GoogleCalendarClient", FakeGoogleCalendarClient)

    await trigger_sync_for_calendar(client_calendar_id)

    cursor = await db.execute(
        """SELECT sync_token, consecutive_failures, last_error, last_full_sync, last_incremental_sync
           FROM calendar_sync_state WHERE client_calendar_id = ?""",
        (client_calendar_id,),
    )
    row = await cursor.fetchone()
    assert row["sync_token"] == "full-sync-token"
    assert row["consecutive_failures"] == 0
    assert row["last_error"] is None
    assert row["last_full_sync"] is not None
    assert row["last_incremental_sync"] is not None


@pytest.mark.asyncio
async def test_trigger_sync_for_main_calendar_pause_and_missing_user(test_db):
    """Main-calendar sync should return when paused or when user is invalid."""
    from app.sync.engine import trigger_sync_for_main_calendar

    await set_setting("sync_paused", "true")
    await trigger_sync_for_main_calendar(12345)

    await set_setting("sync_paused", "false")
    await trigger_sync_for_main_calendar(12345)

    db = await get_database()
    cursor = await db.execute("SELECT COUNT(*) FROM main_calendar_sync_state")
    assert (await cursor.fetchone())[0] == 0


@pytest.mark.asyncio
async def test_trigger_sync_for_main_calendar_state_creation_token_expiry_and_success(test_db, monkeypatch):
    """Main-calendar sync should create state, handle token expiry, process deletes, and persist token."""
    from app.sync.engine import trigger_sync_for_main_calendar

    user_id = await _insert_user(email="mainsync@example.com", google_user_id="mainsync-google", main_calendar_id="main-cal")
    await _insert_token(user_id, "mainsync@example.com", account_type="home")

    async def fake_get_valid_access_token(_user_id: int, _email: str) -> str:
        return "main-token"

    state = {"list_calls": 0}

    class FakeGoogleCalendarClient:
        def __init__(self, _token: str):
            pass

        def list_events(self, _calendar_id: str, sync_token: str | None = None):
            state["list_calls"] += 1
            if state["list_calls"] == 1:
                assert sync_token is None
                return {"events": [], "sync_token_expired": True}
            return {
                "events": [
                    {"id": "main-del", "status": "cancelled"},
                    {
                        "id": "main-live",
                        "status": "confirmed",
                        "start": {"dateTime": "2026-01-01T12:00:00Z"},
                        "end": {"dateTime": "2026-01-01T13:00:00Z"},
                    },
                ],
                "next_sync_token": "main-next-token",
            }

    deleted_main_ids: list[str] = []
    synced_main_ids: list[str] = []

    async def fake_handle_deleted_main_event(_user_id: int, event_id: str):
        deleted_main_ids.append(event_id)

    async def fake_sync_main_event_to_clients(**kwargs):
        synced_main_ids.append(kwargs["event"]["id"])
        return []

    monkeypatch.setattr("app.auth.google.get_valid_access_token", fake_get_valid_access_token)
    monkeypatch.setattr("app.sync.engine.GoogleCalendarClient", FakeGoogleCalendarClient)
    monkeypatch.setattr("app.sync.engine.handle_deleted_main_event", fake_handle_deleted_main_event)
    monkeypatch.setattr("app.sync.engine.sync_main_event_to_clients", fake_sync_main_event_to_clients)

    await trigger_sync_for_main_calendar(user_id)

    assert deleted_main_ids == ["main-del"]
    assert synced_main_ids == ["main-live"]

    db = await get_database()
    cursor = await db.execute(
        """SELECT sync_token, consecutive_failures, last_error, last_full_sync, last_incremental_sync
           FROM main_calendar_sync_state WHERE user_id = ?""",
        (user_id,),
    )
    row = await cursor.fetchone()
    assert row["sync_token"] == "main-next-token"
    assert row["consecutive_failures"] == 0
    assert row["last_error"] is None
    assert row["last_full_sync"] is not None
    assert row["last_incremental_sync"] is not None


@pytest.mark.asyncio
async def test_trigger_sync_for_main_calendar_incremental_path(test_db, monkeypatch):
    """Main-calendar sync should update incremental sync fields when sync token exists."""
    from app.sync.engine import trigger_sync_for_main_calendar

    user_id = await _insert_user(email="incremental@example.com", google_user_id="incremental-google", main_calendar_id="main-inc")
    await _insert_token(user_id, "incremental@example.com", account_type="home")

    db = await get_database()
    await db.execute(
        """INSERT INTO main_calendar_sync_state (user_id, sync_token, consecutive_failures)
           VALUES (?, ?, 2)""",
        (user_id, "old-main-sync"),
    )
    await db.commit()

    async def fake_get_valid_access_token(_user_id: int, _email: str) -> str:
        return "token"

    class FakeGoogleCalendarClient:
        def __init__(self, _token: str):
            pass

        def list_events(self, _calendar_id: str, sync_token: str | None = None):
            assert sync_token == "old-main-sync"
            return {"events": [], "next_sync_token": "new-main-sync"}

    monkeypatch.setattr("app.auth.google.get_valid_access_token", fake_get_valid_access_token)
    monkeypatch.setattr("app.sync.engine.GoogleCalendarClient", FakeGoogleCalendarClient)

    await trigger_sync_for_main_calendar(user_id)

    cursor = await db.execute(
        """SELECT sync_token, consecutive_failures, last_error, last_incremental_sync
           FROM main_calendar_sync_state WHERE user_id = ?""",
        (user_id,),
    )
    row = await cursor.fetchone()
    assert row["sync_token"] == "new-main-sync"
    assert row["consecutive_failures"] == 0
    assert row["last_error"] is None
    assert row["last_incremental_sync"] is not None


@pytest.mark.asyncio
async def test_trigger_sync_for_user_pause_and_dispatch_paths(test_db, monkeypatch):
    """User sync should respect pause flag and dispatch all active calendars plus main."""
    from app.sync.engine import trigger_sync_for_user

    user_id = await _insert_user(email="user-sync@example.com", google_user_id="user-sync-google", main_calendar_id="main")
    token_id = await _insert_token(user_id, "user-sync-client@example.com")
    cal_1 = await _insert_calendar(user_id, token_id, "user-sync-cal-1", is_active=True)
    cal_2 = await _insert_calendar(user_id, token_id, "user-sync-cal-2", is_active=True)
    await _insert_calendar(user_id, token_id, "user-sync-cal-inactive", is_active=False)

    calendar_calls: list[int] = []
    main_calls: list[int] = []

    async def fake_trigger_sync_for_calendar(calendar_id: int):
        calendar_calls.append(calendar_id)

    async def fake_trigger_sync_for_main_calendar(u_id: int):
        main_calls.append(u_id)

    monkeypatch.setattr("app.sync.engine.trigger_sync_for_calendar", fake_trigger_sync_for_calendar)
    monkeypatch.setattr("app.sync.engine.trigger_sync_for_main_calendar", fake_trigger_sync_for_main_calendar)

    await set_setting("sync_paused", "true")
    await trigger_sync_for_user(user_id)
    assert calendar_calls == []
    assert main_calls == []

    await set_setting("sync_paused", "false")
    await trigger_sync_for_user(user_id)
    assert sorted(calendar_calls) == sorted([cal_1, cal_2])
    assert main_calls == [user_id]


@pytest.mark.asyncio
async def test_cleanup_disconnected_calendar_missing_calendar_noop(test_db):
    """Cleanup should no-op when calendar does not exist."""
    from app.sync.engine import cleanup_disconnected_calendar

    await cleanup_disconnected_calendar(9999, 1)


@pytest.mark.asyncio
async def test_cleanup_disconnected_calendar_successfully_deletes_remote_and_local(test_db, monkeypatch):
    """Cleanup should remove local records only after successful remote deletions."""
    from app.sync.engine import cleanup_disconnected_calendar

    user_id = await _insert_user(email="cleanup@example.com", google_user_id="cleanup-google", main_calendar_id="main-cleanup")
    token_1 = await _insert_token(user_id, "cleanup-client-1@example.com")
    token_2 = await _insert_token(user_id, "cleanup-client-2@example.com")
    cal_1 = await _insert_calendar(user_id, token_1, "cleanup-cal-1")
    cal_2 = await _insert_calendar(user_id, token_2, "cleanup-cal-2")

    db = await get_database()
    cursor = await db.execute(
        """INSERT INTO event_mappings
           (user_id, origin_type, origin_calendar_id, origin_event_id, main_event_id, is_recurring, user_can_edit)
           VALUES (?, 'client', ?, ?, ?, FALSE, TRUE)
           RETURNING id""",
        (user_id, cal_1, "origin-cleanup", "main-cleanup-evt"),
    )
    mapping_id = (await cursor.fetchone())["id"]

    await db.execute(
        "INSERT INTO busy_blocks (event_mapping_id, client_calendar_id, busy_block_event_id) VALUES (?, ?, ?)",
        (mapping_id, cal_1, "busy-on-cal-1"),
    )
    await db.execute(
        "INSERT INTO busy_blocks (event_mapping_id, client_calendar_id, busy_block_event_id) VALUES (?, ?, ?)",
        (mapping_id, cal_2, "busy-on-cal-2"),
    )
    await db.execute(
        """INSERT INTO webhook_channels
           (user_id, calendar_type, client_calendar_id, channel_id, resource_id, expiration)
           VALUES (?, 'client', ?, 'ch-cleanup', 'res-cleanup', ?)""",
        (user_id, cal_1, (datetime.utcnow() + timedelta(days=1)).isoformat()),
    )
    await db.execute(
        "INSERT INTO calendar_sync_state (client_calendar_id, sync_token) VALUES (?, ?)",
        (cal_1, "sync-cleanup"),
    )
    await db.commit()

    async def fake_get_valid_access_token(_user_id: int, _email: str) -> str:
        return "token"

    class SuccessfulDeleteClient:
        def __init__(self, _token: str):
            pass

        def delete_event(self, _calendar_id: str, _event_id: str):
            return True

    monkeypatch.setattr("app.auth.google.get_valid_access_token", fake_get_valid_access_token)
    monkeypatch.setattr("app.sync.engine.GoogleCalendarClient", SuccessfulDeleteClient)

    await cleanup_disconnected_calendar(cal_1, user_id)

    cursor = await db.execute("SELECT COUNT(*) FROM event_mappings WHERE id = ?", (mapping_id,))
    assert (await cursor.fetchone())[0] == 0

    cursor = await db.execute("SELECT COUNT(*) FROM busy_blocks WHERE event_mapping_id = ?", (mapping_id,))
    assert (await cursor.fetchone())[0] == 0

    cursor = await db.execute("SELECT COUNT(*) FROM webhook_channels WHERE client_calendar_id = ?", (cal_1,))
    assert (await cursor.fetchone())[0] == 0

    cursor = await db.execute("SELECT COUNT(*) FROM calendar_sync_state WHERE client_calendar_id = ?", (cal_1,))
    assert (await cursor.fetchone())[0] == 0


@pytest.mark.asyncio
async def test_cleanup_disconnected_calendar_handles_token_failures(test_db, monkeypatch):
    """Cleanup should preserve mappings when token retrieval fails during remote cleanup."""
    from app.sync.engine import cleanup_disconnected_calendar

    user_id = await _insert_user(email="tokfail@example.com", google_user_id="tokfail-google", main_calendar_id="main-tokfail")
    token_1 = await _insert_token(user_id, "tokfail-client-1@example.com")
    token_2 = await _insert_token(user_id, "tokfail-client-2@example.com")
    cal_1 = await _insert_calendar(user_id, token_1, "tokfail-cal-1")
    cal_2 = await _insert_calendar(user_id, token_2, "tokfail-cal-2")

    db = await get_database()
    cursor = await db.execute(
        """INSERT INTO event_mappings
           (user_id, origin_type, origin_calendar_id, origin_event_id, main_event_id, is_recurring, user_can_edit)
           VALUES (?, 'client', ?, ?, ?, FALSE, TRUE)
           RETURNING id""",
        (user_id, cal_1, "origin-tokfail", "main-tokfail"),
    )
    mapping_id = (await cursor.fetchone())["id"]
    await db.execute(
        "INSERT INTO busy_blocks (event_mapping_id, client_calendar_id, busy_block_event_id) VALUES (?, ?, ?)",
        (mapping_id, cal_1, "busy-token-fail"),
    )
    await db.execute(
        "INSERT INTO busy_blocks (event_mapping_id, client_calendar_id, busy_block_event_id) VALUES (?, ?, ?)",
        (mapping_id, cal_2, "busy-token-fail-2"),
    )
    await db.commit()

    async def fake_get_valid_access_token(_user_id: int, email: str) -> str:
        if email in {"tokfail-client-1@example.com", "tokfail@example.com"}:
            raise RuntimeError("token unavailable")
        return "token"

    monkeypatch.setattr("app.auth.google.get_valid_access_token", fake_get_valid_access_token)

    await cleanup_disconnected_calendar(cal_1, user_id)

    cursor = await db.execute("SELECT COUNT(*) FROM event_mappings WHERE id = ?", (mapping_id,))
    assert (await cursor.fetchone())[0] == 1


@pytest.mark.asyncio
async def test_cleanup_disconnected_calendar_outer_exception_is_swallowed(monkeypatch):
    """Cleanup should catch unexpected top-level exceptions and not raise."""
    from app.sync.engine import cleanup_disconnected_calendar

    class FakeCursor:
        def __init__(self, row=None, rows=None):
            self._row = row
            self._rows = rows or []

        async def fetchone(self):
            return self._row

        async def fetchall(self):
            return self._rows

    class FakeDB:
        def __init__(self):
            self._first = True

        async def execute(self, sql, _params=()):
            if self._first and "FROM client_calendars cc" in sql:
                self._first = False
                return FakeCursor(
                    row={
                        "id": 1,
                        "user_id": 7,
                        "google_calendar_id": "cal",
                        "google_account_email": "client@example.com",
                        "user_email": "home@example.com",
                        "main_calendar_id": "main",
                    }
                )
            raise RuntimeError("unexpected db failure")

        async def commit(self):
            return None

    async def fake_get_database():
        return FakeDB()

    monkeypatch.setattr("app.sync.engine.get_database", fake_get_database)

    await cleanup_disconnected_calendar(1, 7)
