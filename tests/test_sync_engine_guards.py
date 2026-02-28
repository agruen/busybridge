"""Tests for app/sync/engine.py — sync-paused guard, lock-contention guard, and helpers."""

from __future__ import annotations

import asyncio
from typing import Optional
from unittest.mock import AsyncMock, patch

import pytest

from app.database import get_database, set_setting


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


async def _insert_user(
    email: str,
    google_user_id: str,
    main_calendar_id: Optional[str] = "main-cal",
) -> int:
    db = await get_database()
    cursor = await db.execute(
        """INSERT INTO users (email, google_user_id, display_name, main_calendar_id)
           VALUES (?, ?, ?, ?)
           RETURNING id""",
        (email, google_user_id, email.split("@")[0], main_calendar_id),
    )
    row = await cursor.fetchone()
    await db.commit()
    return row["id"]


async def _insert_oauth_token(user_id: int, email: str) -> int:
    db = await get_database()
    cursor = await db.execute(
        """INSERT INTO oauth_tokens
           (user_id, account_type, google_account_email, access_token_encrypted, refresh_token_encrypted)
           VALUES (?, 'client', ?, ?, ?)
           RETURNING id""",
        (user_id, email, b"enc-access", b"enc-refresh"),
    )
    row = await cursor.fetchone()
    await db.commit()
    return row["id"]


async def _insert_client_calendar(user_id: int, token_id: int, cal_id: str) -> int:
    db = await get_database()
    cursor = await db.execute(
        """INSERT INTO client_calendars
           (user_id, oauth_token_id, google_calendar_id, display_name, is_active)
           VALUES (?, ?, ?, ?, TRUE)
           RETURNING id""",
        (user_id, token_id, cal_id, cal_id),
    )
    row = await cursor.fetchone()
    await db.commit()
    return row["id"]


async def _pause_sync() -> None:
    await set_setting("sync_paused", "true")


async def _resume_sync() -> None:
    await set_setting("sync_paused", "false")


# ---------------------------------------------------------------------------
# is_sync_paused
# ---------------------------------------------------------------------------


class TestIsSyncPaused:
    @pytest.mark.asyncio
    async def test_returns_true_when_paused(self, test_db):
        from app.sync.engine import is_sync_paused

        await _pause_sync()
        assert await is_sync_paused() is True

    @pytest.mark.asyncio
    async def test_returns_false_when_not_paused(self, test_db):
        from app.sync.engine import is_sync_paused

        await _resume_sync()
        assert await is_sync_paused() is False

    @pytest.mark.asyncio
    async def test_returns_falsy_when_setting_absent(self, test_db):
        from app.sync.engine import is_sync_paused

        # No setting in DB at all — returns None/falsy (not paused)
        assert not await is_sync_paused()


# ---------------------------------------------------------------------------
# trigger_sync_for_calendar — sync-paused guard
# ---------------------------------------------------------------------------


class TestTriggerSyncForCalendarPausedGuard:
    @pytest.mark.asyncio
    async def test_skips_sync_when_paused(self, test_db, monkeypatch):
        from app.sync.engine import trigger_sync_for_calendar

        await _pause_sync()
        inner_called = []

        async def fake_inner(cal_id):
            inner_called.append(cal_id)

        monkeypatch.setattr("app.sync.engine._sync_client_calendar", fake_inner)
        await trigger_sync_for_calendar(client_calendar_id=1)

        assert inner_called == []

    @pytest.mark.asyncio
    async def test_runs_sync_when_not_paused(self, test_db, monkeypatch):
        from app.sync.engine import trigger_sync_for_calendar

        await _resume_sync()
        inner_called = []

        async def fake_inner(cal_id):
            inner_called.append(cal_id)

        monkeypatch.setattr("app.sync.engine._sync_client_calendar", fake_inner)
        await trigger_sync_for_calendar(client_calendar_id=42)

        assert inner_called == [42]


# ---------------------------------------------------------------------------
# trigger_sync_for_calendar — lock-contention guard
# ---------------------------------------------------------------------------


class TestTriggerSyncForCalendarLockGuard:
    @pytest.mark.asyncio
    async def test_skips_sync_when_already_in_progress(self, test_db, monkeypatch):
        """When the calendar lock is already held, a second call should return immediately."""
        from app.sync.engine import _get_calendar_lock, trigger_sync_for_calendar

        await _resume_sync()

        lock = await _get_calendar_lock("client:77")
        inner_called = []

        async def fake_inner(cal_id):
            inner_called.append(cal_id)

        monkeypatch.setattr("app.sync.engine._sync_client_calendar", fake_inner)

        async with lock:
            # Lock is now held — triggering should be a no-op
            await trigger_sync_for_calendar(client_calendar_id=77)

        assert inner_called == []


# ---------------------------------------------------------------------------
# trigger_sync_for_main_calendar — sync-paused guard
# ---------------------------------------------------------------------------


class TestTriggerSyncForMainCalendarPausedGuard:
    @pytest.mark.asyncio
    async def test_skips_sync_when_paused(self, test_db, monkeypatch):
        from app.sync.engine import trigger_sync_for_main_calendar

        await _pause_sync()
        inner_called = []

        async def fake_inner(uid):
            inner_called.append(uid)

        monkeypatch.setattr("app.sync.engine._sync_main_calendar", fake_inner)
        await trigger_sync_for_main_calendar(user_id=1)

        assert inner_called == []

    @pytest.mark.asyncio
    async def test_runs_sync_when_not_paused(self, test_db, monkeypatch):
        from app.sync.engine import trigger_sync_for_main_calendar

        await _resume_sync()
        inner_called = []

        async def fake_inner(uid):
            inner_called.append(uid)

        monkeypatch.setattr("app.sync.engine._sync_main_calendar", fake_inner)
        await trigger_sync_for_main_calendar(user_id=5)

        assert inner_called == [5]


# ---------------------------------------------------------------------------
# trigger_sync_for_main_calendar — lock-contention guard
# ---------------------------------------------------------------------------


class TestTriggerSyncForMainCalendarLockGuard:
    @pytest.mark.asyncio
    async def test_skips_when_lock_held(self, test_db, monkeypatch):
        from app.sync.engine import _get_calendar_lock, trigger_sync_for_main_calendar

        await _resume_sync()
        lock = await _get_calendar_lock("main:99")
        inner_called = []

        async def fake_inner(uid):
            inner_called.append(uid)

        monkeypatch.setattr("app.sync.engine._sync_main_calendar", fake_inner)

        async with lock:
            await trigger_sync_for_main_calendar(user_id=99)

        assert inner_called == []


# ---------------------------------------------------------------------------
# trigger_sync_for_user — sync-paused guard
# ---------------------------------------------------------------------------


class TestTriggerSyncForUserPausedGuard:
    @pytest.mark.asyncio
    async def test_skips_all_syncs_when_paused(self, test_db, monkeypatch):
        from app.sync.engine import trigger_sync_for_user

        await _pause_sync()
        user_id = await _insert_user("paused-user@example.com", "gid-paused")

        inner_called = []

        async def fake_trigger_cal(cal_id):
            inner_called.append(("cal", cal_id))

        async def fake_trigger_main(uid):
            inner_called.append(("main", uid))

        monkeypatch.setattr("app.sync.engine.trigger_sync_for_calendar", fake_trigger_cal)
        monkeypatch.setattr("app.sync.engine.trigger_sync_for_main_calendar", fake_trigger_main)

        await trigger_sync_for_user(user_id=user_id)

        assert inner_called == []

    @pytest.mark.asyncio
    async def test_syncs_all_calendars_when_not_paused(self, test_db, monkeypatch):
        from app.sync.engine import trigger_sync_for_user

        await _resume_sync()
        user_id = await _insert_user("active-user@example.com", "gid-active")
        token_id = await _insert_oauth_token(user_id, "cli@external.com")
        cal_id = await _insert_client_calendar(user_id, token_id, "active-cal@group.calendar.google.com")

        cal_syncs = []
        main_syncs = []

        async def fake_trigger_cal(cid):
            cal_syncs.append(cid)

        async def fake_trigger_main(uid):
            main_syncs.append(uid)

        monkeypatch.setattr("app.sync.engine.trigger_sync_for_calendar", fake_trigger_cal)
        monkeypatch.setattr("app.sync.engine.trigger_sync_for_main_calendar", fake_trigger_main)

        await trigger_sync_for_user(user_id=user_id)

        assert cal_id in cal_syncs
        assert user_id in main_syncs


# ---------------------------------------------------------------------------
# _summary_has_prefix (pure helper)
# ---------------------------------------------------------------------------


class TestSummaryHasPrefix:
    def test_returns_true_when_summary_starts_with_prefix(self):
        from app.sync.engine import _summary_has_prefix

        assert _summary_has_prefix("BUSY: Meeting", "BUSY:") is True

    def test_returns_false_when_summary_does_not_start_with_prefix(self):
        from app.sync.engine import _summary_has_prefix

        assert _summary_has_prefix("Regular Meeting", "BUSY:") is False

    def test_case_insensitive_comparison(self):
        from app.sync.engine import _summary_has_prefix

        assert _summary_has_prefix("busy: Meeting", "BUSY:") is True

    def test_returns_false_when_summary_is_none(self):
        from app.sync.engine import _summary_has_prefix

        assert _summary_has_prefix(None, "BUSY:") is False

    def test_returns_false_when_summary_is_empty_string(self):
        from app.sync.engine import _summary_has_prefix

        assert _summary_has_prefix("", "BUSY:") is False

    def test_returns_false_when_prefix_is_empty(self):
        from app.sync.engine import _summary_has_prefix

        assert _summary_has_prefix("Some Meeting", "") is False

    def test_returns_false_when_prefix_is_whitespace_only(self):
        from app.sync.engine import _summary_has_prefix

        assert _summary_has_prefix("Some Meeting", "   ") is False


# ---------------------------------------------------------------------------
# _get_calendar_lock — creates and reuses locks
# ---------------------------------------------------------------------------


class TestGetCalendarLock:
    @pytest.mark.asyncio
    async def test_same_key_returns_same_lock_instance(self, test_db):
        from app.sync.engine import _get_calendar_lock

        lock_a = await _get_calendar_lock("client:123")
        lock_b = await _get_calendar_lock("client:123")
        assert lock_a is lock_b

    @pytest.mark.asyncio
    async def test_different_keys_return_different_locks(self, test_db):
        from app.sync.engine import _get_calendar_lock

        lock_a = await _get_calendar_lock("client:200")
        lock_b = await _get_calendar_lock("client:201")
        assert lock_a is not lock_b
