"""Edge-case and fail-safe behavior tests."""

from datetime import datetime, timedelta
from types import SimpleNamespace

import pytest

from app.database import get_database, set_setting


async def _insert_user(
    email: str = "user@example.com",
    google_user_id: str = "google-user-1",
    main_calendar_id: str | None = "main-calendar",
) -> int:
    db = await get_database()
    cursor = await db.execute(
        """INSERT INTO users (email, google_user_id, display_name, main_calendar_id, is_admin)
           VALUES (?, ?, ?, ?, ?)
           RETURNING id""",
        (email, google_user_id, "Test User", main_calendar_id, False),
    )
    row = await cursor.fetchone()
    await db.commit()
    return row["id"]


async def _insert_token(
    user_id: int,
    email: str,
    account_type: str = "client",
    token_expiry: str | None = None,
) -> int:
    db = await get_database()
    cursor = await db.execute(
        """INSERT INTO oauth_tokens
           (user_id, account_type, google_account_email,
            access_token_encrypted, refresh_token_encrypted, token_expiry)
           VALUES (?, ?, ?, ?, ?, ?)
           RETURNING id""",
        (user_id, account_type, email, b"access", b"refresh", token_expiry),
    )
    row = await cursor.fetchone()
    await db.commit()
    return row["id"]


async def _insert_client_calendar(
    user_id: int,
    oauth_token_id: int,
    google_calendar_id: str,
    is_active: bool = True,
    disconnected_at: str | None = None,
) -> int:
    db = await get_database()
    cursor = await db.execute(
        """INSERT INTO client_calendars
           (user_id, oauth_token_id, google_calendar_id, display_name, is_active, disconnected_at)
           VALUES (?, ?, ?, ?, ?, ?)
           RETURNING id""",
        (
            user_id,
            oauth_token_id,
            google_calendar_id,
            google_calendar_id,
            is_active,
            disconnected_at,
        ),
    )
    row = await cursor.fetchone()
    await db.commit()
    return row["id"]


@pytest.mark.asyncio
async def test_trigger_sync_for_calendar_respects_pause_flag(test_db):
    """When sync is paused, calendar sync should return immediately."""
    from app.sync.engine import trigger_sync_for_calendar

    await set_setting("sync_paused", "true")
    await trigger_sync_for_calendar(9999)

    db = await get_database()
    cursor = await db.execute("SELECT COUNT(*) FROM sync_log")
    count = (await cursor.fetchone())[0]
    assert count == 0


@pytest.mark.asyncio
async def test_webhook_sync_message_returns_ok_without_lookup(test_db):
    """Initial Google sync webhook message should be acknowledged."""
    from app.api.webhooks import receive_google_calendar_webhook

    result = await receive_google_calendar_webhook(
        request=None,
        x_goog_channel_id="channel-1",
        x_goog_resource_id="resource-1",
        x_goog_resource_state="sync",
        x_goog_message_number="1",
    )

    assert result == {"status": "ok"}


@pytest.mark.asyncio
async def test_webhook_unknown_channel_returns_ok(test_db):
    """Unknown webhook channels should not return an error."""
    from app.api.webhooks import receive_google_calendar_webhook

    result = await receive_google_calendar_webhook(
        request=None,
        x_goog_channel_id="missing-channel",
        x_goog_resource_id="resource-1",
        x_goog_resource_state="exists",
        x_goog_message_number="1",
    )

    assert result["status"] == "ok"
    assert "Unknown channel" in result["message"]


@pytest.mark.asyncio
async def test_webhook_expired_channel_is_removed(test_db):
    """Expired webhook channels should be removed from the database."""
    from app.api.webhooks import receive_google_calendar_webhook

    user_id = await _insert_user()
    db = await get_database()
    await db.execute(
        """INSERT INTO webhook_channels
           (user_id, calendar_type, client_calendar_id, channel_id, resource_id, expiration)
           VALUES (?, ?, ?, ?, ?, ?)""",
        (
            user_id,
            "main",
            None,
            "expired-channel",
            "resource-1",
            (datetime.utcnow() - timedelta(minutes=5)).isoformat(),
        ),
    )
    await db.commit()

    result = await receive_google_calendar_webhook(
        request=None,
        x_goog_channel_id="expired-channel",
        x_goog_resource_id="resource-1",
        x_goog_resource_state="exists",
        x_goog_message_number="2",
    )

    assert result["status"] == "ok"
    assert "expired" in result["message"].lower()

    cursor = await db.execute(
        "SELECT COUNT(*) FROM webhook_channels WHERE channel_id = ?",
        ("expired-channel",),
    )
    assert (await cursor.fetchone())[0] == 0


@pytest.mark.asyncio
async def test_webhook_resource_id_mismatch_does_not_trigger_sync(test_db, monkeypatch):
    """
    Fail-safe expectation: mismatched resource IDs should be ignored.

    This prevents spoofed/incorrect webhook payloads from triggering sync writes.
    """
    from app.api.webhooks import receive_google_calendar_webhook

    user_id = await _insert_user()
    db = await get_database()
    await db.execute(
        """INSERT INTO webhook_channels
           (user_id, calendar_type, client_calendar_id, channel_id, resource_id, expiration)
           VALUES (?, ?, ?, ?, ?, ?)""",
        (
            user_id,
            "main",
            None,
            "channel-main",
            "expected-resource",
            (datetime.utcnow() + timedelta(days=1)).isoformat(),
        ),
    )
    await db.commit()

    calls: list[str] = []

    async def fake_sync_main(_user_id: int) -> None:
        return None

    def fake_create_background_task(coro, task_name: str = "background_task"):
        calls.append(task_name)
        coro.close()

    monkeypatch.setattr("app.sync.engine.trigger_sync_for_main_calendar", fake_sync_main)
    monkeypatch.setattr("app.utils.tasks.create_background_task", fake_create_background_task)

    await receive_google_calendar_webhook(
        request=None,
        x_goog_channel_id="channel-main",
        x_goog_resource_id="unexpected-resource",
        x_goog_resource_state="exists",
        x_goog_message_number="3",
    )

    assert calls == []


@pytest.mark.asyncio
async def test_sync_main_event_to_clients_preserves_busy_block_record_on_recreate_failure(test_db, monkeypatch):
    """
    Fail-safe expectation: never drop tracking state before replacement succeeds.

    If updating a busy block fails and creating a replacement also fails, the old
    DB mapping should remain so cleanup/retry is still possible.
    """
    from app.sync.rules import sync_main_event_to_clients

    user_id = await _insert_user()
    token_id = await _insert_token(user_id, "client1@example.com")
    client_calendar_id = await _insert_client_calendar(user_id, token_id, "client-calendar-1")

    db = await get_database()
    cursor = await db.execute(
        """INSERT INTO event_mappings
           (user_id, origin_type, origin_event_id, main_event_id, event_start, event_end, is_all_day, is_recurring, user_can_edit)
           VALUES (?, 'main', ?, ?, ?, ?, FALSE, FALSE, TRUE)
           RETURNING id""",
        (
            user_id,
            "main-event-1",
            "main-event-1",
            "2026-01-01T10:00:00Z",
            "2026-01-01T11:00:00Z",
        ),
    )
    mapping_id = (await cursor.fetchone())["id"]
    await db.execute(
        """INSERT INTO busy_blocks (event_mapping_id, client_calendar_id, busy_block_event_id)
           VALUES (?, ?, ?)""",
        (mapping_id, client_calendar_id, "old-busy-block"),
    )
    await db.commit()

    async def fake_get_valid_access_token(_user_id: int, _email: str) -> str:
        return "token"

    class FailingCalendarClient:
        def __init__(self, _token: str):
            pass

        def update_event(self, *_args, **_kwargs):
            raise RuntimeError("update failed")

        def create_event(self, *_args, **_kwargs):
            raise RuntimeError("create failed")

    monkeypatch.setattr("app.auth.google.get_valid_access_token", fake_get_valid_access_token)
    monkeypatch.setattr("app.sync.rules.GoogleCalendarClient", FailingCalendarClient)

    main_client = SimpleNamespace(is_our_event=lambda event: False)
    event = {
        "id": "main-event-1",
        "status": "confirmed",
        "start": {"dateTime": "2026-01-01T10:00:00Z"},
        "end": {"dateTime": "2026-01-01T11:00:00Z"},
    }

    await sync_main_event_to_clients(
        main_client=main_client,
        event=event,
        user_id=user_id,
        main_calendar_id="main-calendar",
        user_email="user@example.com",
    )

    cursor = await db.execute(
        """SELECT COUNT(*) FROM busy_blocks
           WHERE event_mapping_id = ? AND client_calendar_id = ?""",
        (mapping_id, client_calendar_id),
    )
    assert (await cursor.fetchone())[0] == 1


@pytest.mark.asyncio
async def test_cleanup_disconnected_calendar_preserves_mapping_when_remote_delete_fails(test_db, monkeypatch):
    """
    Fail-safe expectation: keep mapping records when cleanup deletes fail remotely.

    Losing the mapping makes automated or manual remediation much harder.
    """
    from app.sync.engine import cleanup_disconnected_calendar

    user_id = await _insert_user(email="home@example.com", google_user_id="home-google-id")
    token_1 = await _insert_token(user_id, "client1@example.com")
    token_2 = await _insert_token(user_id, "client2@example.com")

    cal_1 = await _insert_client_calendar(user_id, token_1, "client-calendar-1")
    cal_2 = await _insert_client_calendar(user_id, token_2, "client-calendar-2")

    db = await get_database()
    cursor = await db.execute(
        """INSERT INTO event_mappings
           (user_id, origin_type, origin_calendar_id, origin_event_id, main_event_id,
            event_start, event_end, is_all_day, is_recurring, user_can_edit)
           VALUES (?, 'client', ?, ?, ?, ?, ?, FALSE, FALSE, TRUE)
           RETURNING id""",
        (
            user_id,
            cal_1,
            "origin-event-1",
            "main-event-1",
            "2026-01-01T10:00:00Z",
            "2026-01-01T11:00:00Z",
        ),
    )
    mapping_id = (await cursor.fetchone())["id"]

    await db.execute(
        """INSERT INTO busy_blocks (event_mapping_id, client_calendar_id, busy_block_event_id)
           VALUES (?, ?, ?)""",
        (mapping_id, cal_1, "busy-on-cal-1"),
    )
    await db.execute(
        """INSERT INTO busy_blocks (event_mapping_id, client_calendar_id, busy_block_event_id)
           VALUES (?, ?, ?)""",
        (mapping_id, cal_2, "busy-on-cal-2"),
    )
    await db.commit()

    async def fake_get_valid_access_token(_user_id: int, _email: str) -> str:
        return "token"

    class AlwaysFailDeleteClient:
        def __init__(self, _token: str):
            pass

        def delete_event(self, *_args, **_kwargs):
            raise RuntimeError("delete failed")

    monkeypatch.setattr("app.auth.google.get_valid_access_token", fake_get_valid_access_token)
    monkeypatch.setattr("app.sync.engine.GoogleCalendarClient", AlwaysFailDeleteClient)

    await cleanup_disconnected_calendar(cal_1, user_id)

    cursor = await db.execute(
        "SELECT COUNT(*) FROM event_mappings WHERE id = ?",
        (mapping_id,),
    )
    assert (await cursor.fetchone())[0] == 1


@pytest.mark.asyncio
async def test_run_retention_cleanup_removes_old_records(test_db):
    """Retention cleanup should remove old mappings/logs/disconnected calendars."""
    from app.jobs.cleanup import run_retention_cleanup

    user_id = await _insert_user()
    token_id = await _insert_token(user_id, "cleanup@example.com")
    active_calendar_id = await _insert_client_calendar(user_id, token_id, "active-calendar")
    old_disconnected_calendar_id = await _insert_client_calendar(
        user_id,
        token_id,
        "old-disconnected-calendar",
        is_active=False,
        disconnected_at=(datetime.utcnow() - timedelta(days=35)).isoformat(),
    )

    db = await get_database()
    cursor = await db.execute(
        """INSERT INTO event_mappings
           (user_id, origin_type, origin_calendar_id, origin_event_id, main_event_id,
            event_start, event_end, is_all_day, is_recurring, user_can_edit)
           VALUES (?, 'client', ?, ?, ?, ?, ?, FALSE, FALSE, TRUE)
           RETURNING id""",
        (
            user_id,
            active_calendar_id,
            "old-origin-event",
            "old-main-event",
            "2025-01-01T10:00:00Z",
            "2025-01-01T11:00:00Z",
        ),
    )
    old_mapping_id = (await cursor.fetchone())["id"]

    await db.execute(
        """INSERT INTO busy_blocks (event_mapping_id, client_calendar_id, busy_block_event_id)
           VALUES (?, ?, ?)""",
        (old_mapping_id, active_calendar_id, "old-block"),
    )
    await db.execute(
        """INSERT INTO sync_log (user_id, calendar_id, action, status, details, created_at)
           VALUES (?, ?, 'sync', 'success', '{}', ?)""",
        (user_id, active_calendar_id, (datetime.utcnow() - timedelta(days=95)).isoformat()),
    )
    await db.commit()

    summary = await run_retention_cleanup()

    assert summary["expired_event_mappings"] >= 1
    assert summary["old_sync_logs"] >= 1
    assert summary["disconnected_calendars"] >= 1

    cursor = await db.execute("SELECT COUNT(*) FROM event_mappings WHERE id = ?", (old_mapping_id,))
    assert (await cursor.fetchone())[0] == 0

    cursor = await db.execute("SELECT COUNT(*) FROM client_calendars WHERE id = ?", (old_disconnected_calendar_id,))
    assert (await cursor.fetchone())[0] == 0


@pytest.mark.asyncio
async def test_refresh_expiring_tokens_queues_alert_on_invalid_grant(test_db, monkeypatch):
    """Invalid-grant refresh errors should queue a token-revoked alert."""
    from app.jobs.sync_job import refresh_expiring_tokens

    user_id = await _insert_user()
    await _insert_token(
        user_id=user_id,
        email="refresh@example.com",
        account_type="client",
        token_expiry=(datetime.utcnow() - timedelta(minutes=5)).isoformat(),
    )

    queued: list[dict] = []

    async def fake_get_valid_access_token(_user_id: int, _email: str) -> str:
        raise RuntimeError("invalid_grant: token expired or revoked")

    async def fake_queue_alert(**kwargs):
        queued.append(kwargs)

    monkeypatch.setattr("app.auth.google.get_valid_access_token", fake_get_valid_access_token)
    monkeypatch.setattr("app.alerts.email.queue_alert", fake_queue_alert)

    await refresh_expiring_tokens()

    assert len(queued) == 1
    assert queued[0]["alert_type"] == "token_revoked"
    assert queued[0]["user_id"] == user_id


@pytest.mark.asyncio
async def test_trigger_sync_for_calendar_does_not_advance_token_on_partial_event_failure(test_db, monkeypatch):
    """Client calendar sync should fail-safe when individual event processing fails."""
    from app.sync.engine import trigger_sync_for_calendar

    user_id = await _insert_user(email="main@example.com", google_user_id="main-user-google-id")
    await _insert_token(user_id, "main@example.com", account_type="home")
    client_token_id = await _insert_token(user_id, "client@example.com", account_type="client")
    client_calendar_id = await _insert_client_calendar(user_id, client_token_id, "client-calendar-id")

    db = await get_database()
    await db.execute(
        """INSERT INTO calendar_sync_state
           (client_calendar_id, sync_token, consecutive_failures)
           VALUES (?, ?, 0)""",
        (client_calendar_id, "old-sync-token"),
    )
    await db.commit()

    async def fake_get_valid_access_token(_user_id: int, _email: str) -> str:
        return "token"

    class FakeCalendarClient:
        def __init__(self, _token: str):
            pass

        def list_events(self, *_args, **_kwargs):
            return {
                "events": [
                    {
                        "id": "evt-1",
                        "status": "confirmed",
                        "start": {"dateTime": "2026-01-01T10:00:00Z"},
                        "end": {"dateTime": "2026-01-01T11:00:00Z"},
                    }
                ],
                "next_sync_token": "new-sync-token",
            }

        def get_event(self, *_args, **_kwargs):
            return None

    async def failing_sync_client_event_to_main(**_kwargs):
        raise RuntimeError("event processing failed")

    monkeypatch.setattr("app.auth.google.get_valid_access_token", fake_get_valid_access_token)
    monkeypatch.setattr("app.sync.engine.GoogleCalendarClient", FakeCalendarClient)
    monkeypatch.setattr("app.sync.engine.sync_client_event_to_main", failing_sync_client_event_to_main)

    await trigger_sync_for_calendar(client_calendar_id)

    cursor = await db.execute(
        """SELECT sync_token, consecutive_failures, last_error
           FROM calendar_sync_state
           WHERE client_calendar_id = ?""",
        (client_calendar_id,),
    )
    row = await cursor.fetchone()
    assert row["sync_token"] == "old-sync-token"
    assert row["consecutive_failures"] == 1
    assert "event(s) failed" in (row["last_error"] or "")

    cursor = await db.execute(
        """SELECT status, details FROM sync_log
           WHERE calendar_id = ?
           ORDER BY id DESC LIMIT 1""",
        (client_calendar_id,),
    )
    log_row = await cursor.fetchone()
    assert log_row["status"] == "failure"
    assert "event(s) failed" in (log_row["details"] or "")


@pytest.mark.asyncio
async def test_trigger_sync_for_main_calendar_does_not_advance_token_on_partial_event_failure(test_db, monkeypatch):
    """Main calendar sync should not advance token when event handling fails."""
    from app.sync.engine import trigger_sync_for_main_calendar

    user_id = await _insert_user(email="home-main@example.com", google_user_id="home-main-google-id")
    await _insert_token(user_id, "home-main@example.com", account_type="home")

    db = await get_database()
    await db.execute(
        """INSERT INTO main_calendar_sync_state
           (user_id, sync_token, consecutive_failures)
           VALUES (?, ?, 0)""",
        (user_id, "main-old-sync-token"),
    )
    await db.commit()

    async def fake_get_valid_access_token(_user_id: int, _email: str) -> str:
        return "token"

    class FakeCalendarClient:
        def __init__(self, _token: str):
            pass

        def list_events(self, *_args, **_kwargs):
            return {
                "events": [
                    {
                        "id": "main-evt-1",
                        "status": "confirmed",
                        "start": {"dateTime": "2026-01-01T10:00:00Z"},
                        "end": {"dateTime": "2026-01-01T11:00:00Z"},
                    }
                ],
                "next_sync_token": "main-new-sync-token",
            }

    async def failing_sync_main_event_to_clients(**_kwargs):
        raise RuntimeError("main event processing failed")

    monkeypatch.setattr("app.auth.google.get_valid_access_token", fake_get_valid_access_token)
    monkeypatch.setattr("app.sync.engine.GoogleCalendarClient", FakeCalendarClient)
    monkeypatch.setattr("app.sync.engine.sync_main_event_to_clients", failing_sync_main_event_to_clients)

    await trigger_sync_for_main_calendar(user_id)

    cursor = await db.execute(
        """SELECT sync_token, consecutive_failures, last_error
           FROM main_calendar_sync_state
           WHERE user_id = ?""",
        (user_id,),
    )
    row = await cursor.fetchone()
    assert row["sync_token"] == "main-old-sync-token"
    assert row["consecutive_failures"] == 1
    assert "event(s) failed" in (row["last_error"] or "")
