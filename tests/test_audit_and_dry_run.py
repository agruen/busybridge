"""Tests for the four rock-solid improvements:

1. Consistency checker logs to sync_log
2. Consistency checker dry_run — no writes, planned_actions populated
3. Sync engine failure details include per-event context
4. Main-calendar sync logs to sync_log
5. cleanup_disconnected_calendar logs to sync_log
6. cleanup_managed_events_for_user logs to sync_log
7. Admin /consistency/check endpoint (full and per-user, live and dry-run)
8. reconcile_calendar logs to sync_log
9. reconcile_calendar dry_run — no writes, planned_actions populated
"""

from __future__ import annotations

import json
from datetime import datetime

import pytest

from app.database import get_database


# ---------------------------------------------------------------------------
# Shared helpers
# ---------------------------------------------------------------------------

async def _insert_user(
    email: str = "u@example.com",
    google_user_id: str = "gu1",
    main_calendar_id: str | None = "main-cal",
) -> int:
    db = await get_database()
    cursor = await db.execute(
        """INSERT INTO users (email, google_user_id, display_name, main_calendar_id)
           VALUES (?, ?, 'U', ?)
           RETURNING id""",
        (email, google_user_id, main_calendar_id),
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


async def _insert_calendar(user_id: int, token_id: int, google_cal_id: str, is_active: bool = True) -> int:
    db = await get_database()
    cursor = await db.execute(
        """INSERT INTO client_calendars
           (user_id, oauth_token_id, google_calendar_id, display_name, is_active)
           VALUES (?, ?, ?, ?, ?)
           RETURNING id""",
        (user_id, token_id, google_cal_id, google_cal_id, is_active),
    )
    row = await cursor.fetchone()
    await db.commit()
    return row["id"]


async def _insert_sync_state(calendar_id: int) -> None:
    db = await get_database()
    await db.execute(
        "INSERT OR IGNORE INTO calendar_sync_state (client_calendar_id) VALUES (?)",
        (calendar_id,),
    )
    await db.commit()


# ---------------------------------------------------------------------------
# 1. Consistency checker logs to sync_log
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_consistency_check_logs_to_sync_log(test_db, monkeypatch):
    """After a normal (non-dry-run) consistency pass a sync_log row is created."""
    from app.sync.consistency import check_user_consistency

    user_id = await _insert_user(email="log@example.com", google_user_id="log-g")
    token_id = await _insert_token(user_id, "client@example.com")
    await _insert_calendar(user_id, token_id, "client-cal")
    db = await get_database()

    async def fake_token(*_a): return "tok"

    class FakeClient:
        def __init__(self, _t): pass
        def get_event(self, *_a): return {"id": "x"}

    monkeypatch.setattr("app.auth.google.get_valid_access_token", fake_token)
    monkeypatch.setattr("app.sync.consistency.GoogleCalendarClient", FakeClient)

    summary = {
        "users_checked": 0, "mappings_checked": 0,
        "orphaned_main_events_deleted": 0, "missing_copies_recreated": 0,
        "orphaned_busy_blocks_deleted": 0, "errors": 0,
    }
    await check_user_consistency(user_id, summary)

    cursor = await db.execute(
        "SELECT * FROM sync_log WHERE user_id = ? AND action = 'consistency_check'",
        (user_id,),
    )
    row = await cursor.fetchone()
    assert row is not None, "Expected a sync_log entry for the consistency check"
    assert row["status"] == "success"
    details = json.loads(row["details"])
    assert "mappings_checked" in details
    assert "errors" in details


@pytest.mark.asyncio
async def test_consistency_check_warns_on_errors(test_db, monkeypatch):
    """If per-mapping errors occur the sync_log entry has status='warning'."""
    from app.sync.consistency import check_user_consistency

    user_id = await _insert_user(email="warn@example.com", google_user_id="warn-g")
    token_id = await _insert_token(user_id, "client@example.com")
    cal_id = await _insert_calendar(user_id, token_id, "client-cal")
    db = await get_database()

    # This token email triggers the "raise on token fetch" path in the fake.
    bad_token_id = await _insert_token(user_id, "bad@example.com")
    bad_cal_id = await _insert_calendar(user_id, bad_token_id, "bad-cal")

    await db.execute(
        """INSERT INTO event_mappings
           (user_id, origin_type, origin_calendar_id, origin_event_id, main_event_id, is_recurring, user_can_edit)
           VALUES (?, 'client', ?, 'ev1', 'main1', FALSE, TRUE)""",
        (user_id, bad_cal_id),
    )
    await db.commit()

    async def fake_token(_uid, email):
        if email == "bad@example.com":
            raise RuntimeError("token unavailable")
        return "tok"

    class FakeClient:
        def __init__(self, _t): pass
        def get_event(self, *_a): return {"id": "x"}

    monkeypatch.setattr("app.auth.google.get_valid_access_token", fake_token)
    monkeypatch.setattr("app.sync.consistency.GoogleCalendarClient", FakeClient)

    summary = {
        "users_checked": 0, "mappings_checked": 0,
        "orphaned_main_events_deleted": 0, "missing_copies_recreated": 0,
        "orphaned_busy_blocks_deleted": 0, "errors": 0,
    }
    await check_user_consistency(user_id, summary)

    cursor = await db.execute(
        "SELECT status FROM sync_log WHERE user_id = ? AND action = 'consistency_check'",
        (user_id,),
    )
    row = await cursor.fetchone()
    assert row is not None
    assert row["status"] == "warning"


# ---------------------------------------------------------------------------
# 2. Consistency checker dry_run — no writes, planned_actions populated
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_consistency_check_dry_run_no_writes(test_db, monkeypatch):
    """dry_run=True: DB unchanged, no sync_log entry, planned_actions populated."""
    from app.sync.consistency import check_user_consistency

    user_id = await _insert_user(email="dry@example.com", google_user_id="dry-g")
    token_id = await _insert_token(user_id, "client@example.com")
    cal_id = await _insert_calendar(user_id, token_id, "client-cal")
    db = await get_database()

    # A mapping whose origin is gone — would normally be deleted.
    cursor = await db.execute(
        """INSERT INTO event_mappings
           (user_id, origin_type, origin_calendar_id, origin_event_id, main_event_id, is_recurring, user_can_edit)
           VALUES (?, 'client', ?, 'gone-origin', 'gone-main', FALSE, TRUE)
           RETURNING id""",
        (user_id, cal_id),
    )
    mapping_id = (await cursor.fetchone())["id"]
    await db.commit()

    writes = {"delete": 0, "create": 0}

    async def fake_token(*_a): return "tok"

    class FakeClient:
        def __init__(self, _t): pass
        def get_event(self, _cal, event_id):
            if event_id == "gone-origin":
                return None          # origin gone
            return {"id": event_id}  # main still exists
        def delete_event(self, *_a):
            writes["delete"] += 1
        def create_event(self, *_a):
            writes["create"] += 1

    monkeypatch.setattr("app.auth.google.get_valid_access_token", fake_token)
    monkeypatch.setattr("app.sync.consistency.GoogleCalendarClient", FakeClient)

    summary = {
        "users_checked": 0, "mappings_checked": 0,
        "orphaned_main_events_deleted": 0, "missing_copies_recreated": 0,
        "orphaned_busy_blocks_deleted": 0, "errors": 0,
        "planned_actions": [],
    }
    await check_user_consistency(user_id, summary, dry_run=True)

    # Mapping must still exist
    cursor = await db.execute(
        "SELECT COUNT(*) FROM event_mappings WHERE id = ?", (mapping_id,)
    )
    assert (await cursor.fetchone())[0] == 1

    # No remote calls
    assert writes["delete"] == 0
    assert writes["create"] == 0

    # No sync_log entry
    cursor = await db.execute(
        "SELECT COUNT(*) FROM sync_log WHERE action = 'consistency_check'"
    )
    assert (await cursor.fetchone())[0] == 0

    # Planned actions should describe the would-be deletion
    assert summary["orphaned_main_events_deleted"] == 1
    assert any(a["action"] == "delete_orphaned_main_event" for a in summary["planned_actions"])


@pytest.mark.asyncio
async def test_consistency_check_dry_run_recreate(test_db, monkeypatch):
    """dry_run=True: missing main copy recorded in planned_actions, not recreated."""
    from app.sync.consistency import check_user_consistency

    user_id = await _insert_user(email="dryrec@example.com", google_user_id="dryrec-g")
    token_id = await _insert_token(user_id, "client@example.com")
    cal_id = await _insert_calendar(user_id, token_id, "client-cal")
    db = await get_database()

    cursor = await db.execute(
        """INSERT INTO event_mappings
           (user_id, origin_type, origin_calendar_id, origin_event_id, main_event_id, is_recurring, user_can_edit)
           VALUES (?, 'client', ?, 'live-origin', 'missing-main', FALSE, TRUE)
           RETURNING id""",
        (user_id, cal_id),
    )
    mapping_id = (await cursor.fetchone())["id"]
    await db.commit()

    async def fake_token(*_a): return "tok"

    class FakeClient:
        def __init__(self, _t): pass
        def get_event(self, _cal, event_id):
            if event_id == "missing-main":
                return None
            return {
                "id": event_id,
                "summary": "Meeting",
                "start": {"dateTime": "2026-01-01T10:00:00Z"},
                "end": {"dateTime": "2026-01-01T11:00:00Z"},
            }
        def create_event(self, *_a):
            raise AssertionError("create_event must not be called in dry_run")

    monkeypatch.setattr("app.auth.google.get_valid_access_token", fake_token)
    monkeypatch.setattr("app.sync.consistency.GoogleCalendarClient", FakeClient)

    summary = {
        "users_checked": 0, "mappings_checked": 0,
        "orphaned_main_events_deleted": 0, "missing_copies_recreated": 0,
        "orphaned_busy_blocks_deleted": 0, "errors": 0,
        "planned_actions": [],
    }
    await check_user_consistency(user_id, summary, dry_run=True)

    # mapping_id unchanged in DB
    cursor = await db.execute(
        "SELECT main_event_id FROM event_mappings WHERE id = ?", (mapping_id,)
    )
    assert (await cursor.fetchone())["main_event_id"] == "missing-main"

    assert summary["missing_copies_recreated"] == 1
    assert any(a["action"] == "recreate_main_event" for a in summary["planned_actions"])


@pytest.mark.asyncio
async def test_consistency_check_dry_run_orphaned_busy_block(test_db, monkeypatch):
    """dry_run=True: orphaned busy block recorded in planned_actions, not deleted."""
    from app.sync.consistency import check_user_consistency

    user_id = await _insert_user(email="drybb@example.com", google_user_id="drybb-g")
    token_id = await _insert_token(user_id, "client@example.com")
    cal_id = await _insert_calendar(user_id, token_id, "client-cal")
    db = await get_database()

    cursor = await db.execute(
        """INSERT INTO event_mappings
           (user_id, origin_type, origin_calendar_id, origin_event_id, main_event_id,
            deleted_at, is_recurring, user_can_edit)
           VALUES (?, 'client', ?, 'old-origin', 'old-main', ?, FALSE, TRUE)
           RETURNING id""",
        (user_id, cal_id, datetime.utcnow().isoformat()),
    )
    mapping_id = (await cursor.fetchone())["id"]
    await db.execute(
        "INSERT INTO busy_blocks (event_mapping_id, client_calendar_id, busy_block_event_id) VALUES (?,?,?)",
        (mapping_id, cal_id, "bb-event-1"),
    )
    await db.commit()

    async def fake_token(*_a): return "tok"

    class FakeClient:
        def __init__(self, _t): pass
        def delete_event(self, *_a):
            raise AssertionError("delete_event must not be called in dry_run")

    monkeypatch.setattr("app.auth.google.get_valid_access_token", fake_token)
    monkeypatch.setattr("app.sync.consistency.GoogleCalendarClient", FakeClient)

    summary = {
        "users_checked": 0, "mappings_checked": 0,
        "orphaned_main_events_deleted": 0, "missing_copies_recreated": 0,
        "orphaned_busy_blocks_deleted": 0, "errors": 0,
        "planned_actions": [],
    }
    await check_user_consistency(user_id, summary, dry_run=True)

    # Busy block still exists
    cursor = await db.execute(
        "SELECT COUNT(*) FROM busy_blocks WHERE event_mapping_id = ?", (mapping_id,)
    )
    assert (await cursor.fetchone())[0] == 1

    assert summary["orphaned_busy_blocks_deleted"] == 1
    assert any(a["action"] == "delete_orphaned_busy_block" for a in summary["planned_actions"])


# ---------------------------------------------------------------------------
# 3. Sync engine failure details include per-event context
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_sync_failure_includes_event_details(test_db, monkeypatch):
    """When event processing fails, sync_log details includes failed_events list."""
    from app.sync.engine import trigger_sync_for_calendar

    user_id = await _insert_user(email="evtfail@example.com", google_user_id="ef-g")
    token_id = await _insert_token(user_id, "client@example.com")
    cal_id = await _insert_calendar(user_id, token_id, "client-cal")
    await _insert_sync_state(cal_id)
    db = await get_database()

    async def fake_token(*_a): return "tok"

    class FakeClient:
        def __init__(self, _t): pass
        def list_events(self, _cal, sync_token=None):
            return {
                "events": [{"id": "bad-event", "status": "confirmed"}],
                "next_sync_token": "new-tok",
            }

    async def exploding_sync(**_kwargs):
        raise RuntimeError("processing failed for bad-event")

    monkeypatch.setattr("app.auth.google.get_valid_access_token", fake_token)
    monkeypatch.setattr("app.sync.engine.GoogleCalendarClient", FakeClient)
    monkeypatch.setattr("app.sync.engine.sync_client_event_to_main", exploding_sync)

    await trigger_sync_for_calendar(cal_id)

    cursor = await db.execute(
        "SELECT details FROM sync_log WHERE calendar_id = ? AND action = 'sync' AND status = 'failure'",
        (cal_id,),
    )
    row = await cursor.fetchone()
    assert row is not None
    details = json.loads(row["details"])
    assert "failed_events" in details
    assert any("bad-event" in ev for ev in details["failed_events"])


# ---------------------------------------------------------------------------
# 4. Main-calendar sync logs to sync_log
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_main_calendar_sync_logs_success(test_db, monkeypatch):
    """A successful main-calendar sync produces a sync_log entry."""
    from app.sync.engine import trigger_sync_for_main_calendar

    user_id = await _insert_user(email="main@example.com", google_user_id="main-g")
    db = await get_database()

    async def fake_token(*_a): return "tok"

    class FakeClient:
        def __init__(self, _t): pass
        def list_events(self, _cal, sync_token=None):
            return {"events": [], "next_sync_token": "new-tok"}

    monkeypatch.setattr("app.auth.google.get_valid_access_token", fake_token)
    monkeypatch.setattr("app.sync.engine.GoogleCalendarClient", FakeClient)

    await trigger_sync_for_main_calendar(user_id)

    cursor = await db.execute(
        "SELECT details FROM sync_log WHERE user_id = ? AND action = 'sync_main' AND status = 'success'",
        (user_id,),
    )
    row = await cursor.fetchone()
    assert row is not None
    details = json.loads(row["details"])
    assert "events_processed" in details
    assert "is_full_sync" in details


@pytest.mark.asyncio
async def test_main_calendar_sync_logs_failure(test_db, monkeypatch):
    """A failed main-calendar sync produces a failure sync_log entry with failed_events."""
    from app.sync.engine import trigger_sync_for_main_calendar

    user_id = await _insert_user(email="mainfail@example.com", google_user_id="mf-g")
    db = await get_database()

    async def fake_token(*_a): return "tok"

    class FakeClient:
        def __init__(self, _t): pass
        def list_events(self, _cal, sync_token=None):
            return {
                "events": [{"id": "main-bad", "status": "confirmed"}],
                "next_sync_token": "new-tok",
            }

    async def exploding_main_sync(**_kwargs):
        raise RuntimeError("main sync exploded")

    monkeypatch.setattr("app.auth.google.get_valid_access_token", fake_token)
    monkeypatch.setattr("app.sync.engine.GoogleCalendarClient", FakeClient)
    monkeypatch.setattr("app.sync.engine.sync_main_event_to_clients", exploding_main_sync)

    await trigger_sync_for_main_calendar(user_id)

    cursor = await db.execute(
        "SELECT details FROM sync_log WHERE user_id = ? AND action = 'sync_main' AND status = 'failure'",
        (user_id,),
    )
    row = await cursor.fetchone()
    assert row is not None
    details = json.loads(row["details"])
    assert "failed_events" in details
    assert any("main-bad" in ev for ev in details["failed_events"])


# ---------------------------------------------------------------------------
# 5. cleanup_disconnected_calendar logs to sync_log
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_disconnect_cleanup_logs_to_sync_log(test_db, monkeypatch):
    """Disconnecting a calendar writes a disconnect_cleanup entry to sync_log."""
    from app.sync.engine import cleanup_disconnected_calendar

    user_id = await _insert_user(email="disc@example.com", google_user_id="disc-g")
    token_id = await _insert_token(user_id, "client@example.com")
    cal_id = await _insert_calendar(user_id, token_id, "client-cal")
    db = await get_database()

    async def fake_token(*_a): return "tok"

    class FakeClient:
        def __init__(self, _t): pass
        def delete_event(self, *_a): return True

    monkeypatch.setattr("app.auth.google.get_valid_access_token", fake_token)
    monkeypatch.setattr("app.sync.engine.GoogleCalendarClient", FakeClient)

    await cleanup_disconnected_calendar(cal_id, user_id)

    cursor = await db.execute(
        "SELECT * FROM sync_log WHERE user_id = ? AND action = 'disconnect_cleanup'",
        (user_id,),
    )
    row = await cursor.fetchone()
    assert row is not None
    assert row["status"] == "success"
    details = json.loads(row["details"])
    assert "mappings_cleaned" in details
    assert "busy_blocks_deleted" in details


@pytest.mark.asyncio
async def test_disconnect_cleanup_warns_on_partial_failure(test_db, monkeypatch):
    """If some mappings can't be cleaned remotely, the log status is 'warning'."""
    from app.sync.engine import cleanup_disconnected_calendar

    user_id = await _insert_user(email="discwarn@example.com", google_user_id="dw-g")
    token_id = await _insert_token(user_id, "client@example.com")
    cal_id = await _insert_calendar(user_id, token_id, "client-cal")
    db = await get_database()

    # One mapping whose main-event delete will fail.
    await db.execute(
        """INSERT INTO event_mappings
           (user_id, origin_type, origin_calendar_id, origin_event_id, main_event_id, is_recurring, user_can_edit)
           VALUES (?, 'client', ?, 'ev1', 'main1', FALSE, TRUE)""",
        (user_id, cal_id),
    )
    await db.commit()

    async def fake_token(*_a): return "tok"

    class FakeClient:
        def __init__(self, _t): pass
        def delete_event(self, _cal, event_id):
            if event_id == "main1":
                raise RuntimeError("main delete failed")
            return True

    monkeypatch.setattr("app.auth.google.get_valid_access_token", fake_token)
    monkeypatch.setattr("app.sync.engine.GoogleCalendarClient", FakeClient)

    await cleanup_disconnected_calendar(cal_id, user_id)

    cursor = await db.execute(
        "SELECT status FROM sync_log WHERE user_id = ? AND action = 'disconnect_cleanup'",
        (user_id,),
    )
    row = await cursor.fetchone()
    assert row is not None
    assert row["status"] == "warning"


# ---------------------------------------------------------------------------
# 6. cleanup_managed_events_for_user logs to sync_log
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_managed_cleanup_logs_to_sync_log(test_db, monkeypatch):
    """cleanup_managed_events_for_user writes a managed_cleanup entry to sync_log."""
    from app.sync.engine import cleanup_managed_events_for_user
    from app.config import get_settings

    user_id = await _insert_user(email="managed@example.com", google_user_id="mg-g")
    db = await get_database()

    async def fake_token(*_a): return "tok"

    class FakeClient:
        def __init__(self, _t): pass
        def delete_event(self, *_a): return True
        def search_events(self, _cal, _prefix): return []

    monkeypatch.setattr("app.auth.google.get_valid_access_token", fake_token)
    monkeypatch.setattr("app.sync.engine.GoogleCalendarClient", FakeClient)
    monkeypatch.setattr(
        "app.sync.engine.get_settings",
        lambda: type("S", (), {"managed_event_prefix": "[BB]"})(),
    )

    await cleanup_managed_events_for_user(user_id)

    cursor = await db.execute(
        "SELECT * FROM sync_log WHERE user_id = ? AND action = 'managed_cleanup'",
        (user_id,),
    )
    row = await cursor.fetchone()
    assert row is not None
    details = json.loads(row["details"])
    assert "db_main_events_deleted" in details
    assert "error_count" in details


# ---------------------------------------------------------------------------
# 7. Admin /consistency/check endpoint
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_admin_consistency_check_endpoint_full_run(test_db, monkeypatch):
    """POST /admin/consistency/check runs the full check and returns a summary."""
    from app.api.admin import trigger_consistency_check
    from app.auth.session import User

    # Insert a user so there's at least one to check.
    user_id = await _insert_user(email="adm@example.com", google_user_id="adm-g")
    admin = User(
        id=user_id, email="adm@example.com",
        google_user_id="adm-g", display_name="Admin",
        main_calendar_id="main-cal", is_admin=True,
    )

    async def fake_token(*_a): return "tok"

    class FakeClient:
        def __init__(self, _t): pass
        def get_event(self, *_a): return {"id": "x"}

    monkeypatch.setattr("app.auth.google.get_valid_access_token", fake_token)
    monkeypatch.setattr("app.sync.consistency.GoogleCalendarClient", FakeClient)

    # Run against the live (in-memory) DB — no calendars, so nothing to do.
    result = await trigger_consistency_check(dry_run=False, user_id=None, admin=admin)
    assert result["dry_run"] is False
    assert "summary" in result
    assert result["summary"]["users_checked"] == 1


@pytest.mark.asyncio
async def test_admin_consistency_check_endpoint_per_user_dry_run(test_db, monkeypatch):
    """POST /admin/consistency/check?dry_run=true&user_id=X runs per-user preview."""
    from app.api.admin import trigger_consistency_check
    from app.auth.session import User
    from fastapi import HTTPException

    user_id = await _insert_user(email="admdry@example.com", google_user_id="admdry-g")
    admin = User(
        id=user_id, email="admdry@example.com",
        google_user_id="admdry-g", display_name="Admin",
        main_calendar_id="main-cal", is_admin=True,
    )

    async def fake_token(*_a): return "tok"

    class FakeClient:
        def __init__(self, _t): pass
        def get_event(self, *_a): return {"id": "x"}

    monkeypatch.setattr("app.auth.google.get_valid_access_token", fake_token)
    monkeypatch.setattr("app.sync.consistency.GoogleCalendarClient", FakeClient)

    result = await trigger_consistency_check(dry_run=True, user_id=user_id, admin=admin)
    assert result["dry_run"] is True
    assert "planned_actions" in result["summary"]

    # 404 for non-existent user
    with pytest.raises(HTTPException) as exc_info:
        await trigger_consistency_check(dry_run=False, user_id=99999, admin=admin)
    assert exc_info.value.status_code == 404


# ---------------------------------------------------------------------------
# 8. reconcile_calendar logs to sync_log
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_reconcile_calendar_logs_to_sync_log(test_db, monkeypatch):
    """reconcile_calendar writes a 'reconcile' entry to sync_log."""
    from app.sync.consistency import reconcile_calendar

    user_id = await _insert_user(email="rec@example.com", google_user_id="rec-g")
    token_id = await _insert_token(user_id, "client@example.com")
    cal_id = await _insert_calendar(user_id, token_id, "client-cal")
    db = await get_database()

    async def fake_token(*_a): return "tok"

    class FakeClient:
        def __init__(self, _t): pass
        def list_events(self, _cal):
            return {"events": [{"id": "ev1", "status": "confirmed"}]}
        def get_event(self, _cal, _eid): return {"id": _eid}
        def is_our_event(self, _e): return False
        def delete_event(self, *_a): return True

    monkeypatch.setattr("app.auth.google.get_valid_access_token", fake_token)
    monkeypatch.setattr("app.sync.consistency.GoogleCalendarClient", FakeClient)

    await reconcile_calendar(cal_id)

    cursor = await db.execute(
        "SELECT * FROM sync_log WHERE calendar_id = ? AND action = 'reconcile'",
        (cal_id,),
    )
    row = await cursor.fetchone()
    assert row is not None
    assert row["status"] == "success"
    details = json.loads(row["details"])
    assert "events_found" in details
    assert "stale_mappings_removed" in details


# ---------------------------------------------------------------------------
# 9. reconcile_calendar dry_run — no writes, planned_actions populated
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_reconcile_calendar_dry_run_no_writes(test_db, monkeypatch):
    """reconcile_calendar with dry_run=True: no DB changes, planned_actions populated."""
    from app.sync.consistency import reconcile_calendar

    user_id = await _insert_user(email="recdry@example.com", google_user_id="recdry-g")
    token_id = await _insert_token(user_id, "client@example.com")
    cal_id = await _insert_calendar(user_id, token_id, "client-cal")
    db = await get_database()

    # A mapping whose origin no longer appears in list_events → stale.
    cursor = await db.execute(
        """INSERT INTO event_mappings
           (user_id, origin_type, origin_calendar_id, origin_event_id, main_event_id, is_recurring, user_can_edit)
           VALUES (?, 'client', ?, 'stale-ev', 'stale-main', FALSE, TRUE)
           RETURNING id""",
        (user_id, cal_id),
    )
    mapping_id = (await cursor.fetchone())["id"]
    await db.commit()

    deleted_calls = []

    async def fake_token(*_a): return "tok"

    class FakeClient:
        def __init__(self, _t): pass
        def list_events(self, _cal):
            # stale-ev is absent from live events
            return {"events": [{"id": "live-ev", "status": "confirmed"}]}
        def get_event(self, _cal, event_id):
            if event_id == "stale-ev":
                return None  # confirms it's truly gone
            return {"id": event_id}
        def is_our_event(self, _e): return False
        def delete_event(self, _cal, event_id):
            deleted_calls.append(event_id)
            return True

    monkeypatch.setattr("app.auth.google.get_valid_access_token", fake_token)
    monkeypatch.setattr("app.sync.consistency.GoogleCalendarClient", FakeClient)

    result = await reconcile_calendar(cal_id, dry_run=True)

    # Mapping must still exist
    cursor = await db.execute(
        "SELECT COUNT(*) FROM event_mappings WHERE id = ?", (mapping_id,)
    )
    assert (await cursor.fetchone())[0] == 1

    # No remote deletes
    assert deleted_calls == []

    # No sync_log entry
    cursor = await db.execute(
        "SELECT COUNT(*) FROM sync_log WHERE action = 'reconcile'"
    )
    assert (await cursor.fetchone())[0] == 0

    # Planned actions describe the stale mapping
    assert result["stale_mappings_removed"] == 1
    assert "planned_actions" in result
    assert any(a["action"] == "remove_stale_mapping" for a in result["planned_actions"])
