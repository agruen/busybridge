"""Tests for app/sync/backup.py — backup creation, restore, retention, and helpers."""

from __future__ import annotations

import io
import json
import os
import sqlite3
import tempfile
import zipfile
from datetime import datetime
from typing import Optional
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from app.database import get_database


# ---------------------------------------------------------------------------
# Helpers shared across tests
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


def _make_backup_zip(
    metadata: dict,
    db_bytes: bytes = b"SQLite format 3",
    snapshots: Optional[dict[str, dict]] = None,
) -> bytes:
    """Build an in-memory backup ZIP and return its bytes."""
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as zf:
        zf.writestr("metadata.json", json.dumps(metadata))
        zf.writestr("database.db", db_bytes)
        for uid, snap in (snapshots or {}).items():
            zf.writestr(f"snapshots/{uid}.json", json.dumps(snap))
    return buf.getvalue()


# ---------------------------------------------------------------------------
# _classify_backup
# ---------------------------------------------------------------------------


class TestClassifyBackup:
    def test_first_of_month_is_monthly(self):
        from app.sync.backup import _classify_backup

        assert _classify_backup(datetime(2024, 3, 1, 10, 0)) == "monthly"

    def test_sunday_is_weekly(self):
        from app.sync.backup import _classify_backup

        # 2024-03-10 is a Sunday (weekday() == 6)
        assert _classify_backup(datetime(2024, 3, 10, 10, 0)) == "weekly"

    def test_sunday_that_is_also_first_classifies_as_monthly(self):
        from app.sync.backup import _classify_backup

        # day==1 check runs first, so 1st-of-month wins even when it's a Sunday
        # 2024-09-01 is a Sunday
        assert _classify_backup(datetime(2024, 9, 1, 10, 0)) == "monthly"

    def test_regular_day_is_daily(self):
        from app.sync.backup import _classify_backup

        # 2024-03-13 is a Wednesday
        assert _classify_backup(datetime(2024, 3, 13, 10, 0)) == "daily"


# ---------------------------------------------------------------------------
# _events_differ
# ---------------------------------------------------------------------------


class TestEventsDiffer:
    def test_identical_events_not_different(self):
        from app.sync.backup import _events_differ

        e = {"summary": "Meeting", "start": {"dateTime": "2024-01-01T10:00:00Z"}, "end": {"dateTime": "2024-01-01T11:00:00Z"}, "status": "confirmed"}
        assert _events_differ(e, e) is False

    def test_summary_change_detected(self):
        from app.sync.backup import _events_differ

        a = {"summary": "Old Title"}
        b = {"summary": "New Title"}
        assert _events_differ(a, b) is True

    def test_start_change_detected(self):
        from app.sync.backup import _events_differ

        a = {"start": {"dateTime": "2024-01-01T10:00:00Z"}}
        b = {"start": {"dateTime": "2024-01-01T11:00:00Z"}}
        assert _events_differ(a, b) is True

    def test_status_change_detected(self):
        from app.sync.backup import _events_differ

        a = {"status": "confirmed"}
        b = {"status": "cancelled"}
        assert _events_differ(a, b) is True

    def test_missing_field_vs_present_detected(self):
        from app.sync.backup import _events_differ

        a = {"colorId": "1"}
        b = {}
        assert _events_differ(a, b) is True

    def test_irrelevant_field_ignored(self):
        from app.sync.backup import _events_differ

        # 'attendees' is not in the compared field set
        a = {"summary": "Meeting", "attendees": [{"email": "a@b.com"}]}
        b = {"summary": "Meeting", "attendees": []}
        assert _events_differ(a, b) is False


# ---------------------------------------------------------------------------
# _diff_events
# ---------------------------------------------------------------------------


class TestDiffEvents:
    def test_create_when_event_only_in_backup(self):
        from app.sync.backup import _diff_events

        backup = [{"id": "evt1", "summary": "New"}]
        current = []
        diff = _diff_events(backup, current)
        assert len(diff["create"]) == 1
        assert diff["create"][0]["id"] == "evt1"
        assert diff["delete"] == []
        assert diff["update"] == []

    def test_delete_when_event_only_in_current(self):
        from app.sync.backup import _diff_events

        backup = []
        current = [{"id": "evt2", "summary": "Gone"}]
        diff = _diff_events(backup, current)
        assert diff["create"] == []
        assert len(diff["delete"]) == 1
        assert diff["delete"][0]["id"] == "evt2"

    def test_update_when_event_changed(self):
        from app.sync.backup import _diff_events

        backup = [{"id": "evt3", "summary": "Updated Title"}]
        current = [{"id": "evt3", "summary": "Old Title"}]
        diff = _diff_events(backup, current)
        assert diff["create"] == []
        assert diff["delete"] == []
        assert len(diff["update"]) == 1
        assert diff["update"][0]["summary"] == "Updated Title"

    def test_no_diff_when_identical(self):
        from app.sync.backup import _diff_events

        event = {"id": "evt4", "summary": "Same"}
        diff = _diff_events([event], [event])
        assert diff["create"] == []
        assert diff["delete"] == []
        assert diff["update"] == []

    def test_mixed_operations(self):
        from app.sync.backup import _diff_events

        backup = [
            {"id": "keep", "summary": "Keep"},
            {"id": "create_me", "summary": "Create"},
            {"id": "update_me", "summary": "New Summary"},
        ]
        current = [
            {"id": "keep", "summary": "Keep"},
            {"id": "delete_me", "summary": "Delete"},
            {"id": "update_me", "summary": "Old Summary"},
        ]
        diff = _diff_events(backup, current)
        assert [e["id"] for e in diff["create"]] == ["create_me"]
        assert [e["id"] for e in diff["delete"]] == ["delete_me"]
        assert [e["id"] for e in diff["update"]] == ["update_me"]


# ---------------------------------------------------------------------------
# _event_snapshot_fields
# ---------------------------------------------------------------------------


class TestEventSnapshotFields:
    def test_keeps_known_fields_only(self):
        from app.sync.backup import _event_snapshot_fields

        event = {
            "id": "evt1",
            "summary": "Meeting",
            "htmlLink": "https://calendar.google.com/...",  # should be stripped
            "created": "2024-01-01",  # should be stripped
            "start": {"dateTime": "2024-01-01T10:00:00Z"},
        }
        result = _event_snapshot_fields(event)
        assert "id" in result
        assert "summary" in result
        assert "start" in result
        assert "htmlLink" not in result
        assert "created" not in result

    def test_missing_fields_skipped_gracefully(self):
        from app.sync.backup import _event_snapshot_fields

        event = {"id": "evt1"}
        result = _event_snapshot_fields(event)
        assert result == {"id": "evt1"}


# ---------------------------------------------------------------------------
# get_backup_dir
# ---------------------------------------------------------------------------


class TestGetBackupDir:
    def test_creates_directory_if_missing(self, tmp_path, monkeypatch):
        from app.sync.backup import get_backup_dir

        target = str(tmp_path / "new_backups")
        monkeypatch.setenv("BACKUP_PATH", target)
        result = get_backup_dir()
        assert result == target
        assert os.path.isdir(target)


# ---------------------------------------------------------------------------
# list_backups / delete_backup
# ---------------------------------------------------------------------------


class TestListAndDeleteBackups:
    def test_list_returns_empty_when_no_backups(self, tmp_path, monkeypatch):
        from app.sync.backup import list_backups

        monkeypatch.setenv("BACKUP_PATH", str(tmp_path))
        assert list_backups() == []

    def test_list_returns_metadata_from_zip(self, tmp_path, monkeypatch):
        from app.sync.backup import list_backups

        monkeypatch.setenv("BACKUP_PATH", str(tmp_path))
        meta = {
            "backup_id": "backup-20240101-120000-daily",
            "backup_type": "daily",
            "created_at": "2024-01-01T12:00:00",
        }
        zip_data = _make_backup_zip(meta)
        (tmp_path / "backup-20240101-120000-daily.zip").write_bytes(zip_data)

        results = list_backups()
        assert len(results) == 1
        assert results[0]["backup_id"] == "backup-20240101-120000-daily"

    def test_list_ignores_non_backup_files(self, tmp_path, monkeypatch):
        from app.sync.backup import list_backups

        monkeypatch.setenv("BACKUP_PATH", str(tmp_path))
        (tmp_path / "README.txt").write_text("ignore me")
        (tmp_path / "other-20240101.zip").write_bytes(b"")
        assert list_backups() == []

    def test_list_corrupt_zip_falls_back_to_id_only(self, tmp_path, monkeypatch):
        from app.sync.backup import list_backups

        monkeypatch.setenv("BACKUP_PATH", str(tmp_path))
        (tmp_path / "backup-bad.zip").write_bytes(b"not a zip")
        results = list_backups()
        assert len(results) == 1
        assert results[0]["backup_id"] == "backup-bad"

    def test_list_sorted_newest_first(self, tmp_path, monkeypatch):
        from app.sync.backup import list_backups

        monkeypatch.setenv("BACKUP_PATH", str(tmp_path))
        for ts, btype in [("20240101-120000", "daily"), ("20240102-120000", "daily")]:
            bid = f"backup-{ts}-{btype}"
            meta = {"backup_id": bid, "backup_type": btype, "created_at": f"2024-01-{ts[:2]}-01T12:00:00"}
            (tmp_path / f"{bid}.zip").write_bytes(_make_backup_zip(meta))

        results = list_backups()
        assert results[0]["backup_id"] == "backup-20240102-120000-daily"

    def test_delete_returns_true_and_removes_file(self, tmp_path, monkeypatch):
        from app.sync.backup import delete_backup

        monkeypatch.setenv("BACKUP_PATH", str(tmp_path))
        bid = "backup-20240101-120000-daily"
        zip_path = tmp_path / f"{bid}.zip"
        zip_path.write_bytes(b"data")

        assert delete_backup(bid) is True
        assert not zip_path.exists()

    def test_delete_returns_false_when_not_found(self, tmp_path, monkeypatch):
        from app.sync.backup import delete_backup

        monkeypatch.setenv("BACKUP_PATH", str(tmp_path))
        assert delete_backup("backup-nonexistent") is False


# ---------------------------------------------------------------------------
# apply_retention_policy
# ---------------------------------------------------------------------------


class TestApplyRetentionPolicy:
    def _write_backup(self, tmp_path, created_at: str, btype: str) -> str:
        ts = created_at.replace("-", "").replace("T", "-").replace(":", "")[:15]
        bid = f"backup-{ts}-{btype}"
        meta = {"backup_id": bid, "backup_type": btype, "created_at": created_at}
        (tmp_path / f"{bid}.zip").write_bytes(_make_backup_zip(meta))
        return bid

    def test_keeps_within_limits(self, tmp_path, monkeypatch):
        from app.sync.backup import apply_retention_policy

        monkeypatch.setenv("BACKUP_PATH", str(tmp_path))
        # Create 3 daily backups (limit is 7) — all should be kept
        ids = []
        for d in range(1, 4):
            ids.append(self._write_backup(tmp_path, f"2024-03-{d+1:02d}T12:00:00", "daily"))

        result = apply_retention_policy()
        assert set(result["kept"]) == set(ids)
        assert result["deleted"] == []

    def test_deletes_excess_daily_backups(self, tmp_path, monkeypatch):
        from app.sync.backup import apply_retention_policy

        monkeypatch.setenv("BACKUP_PATH", str(tmp_path))
        # Create 9 daily backups (limit is 7)
        ids = []
        for d in range(2, 11):  # days 2-10 of March (all regular days)
            ids.append(self._write_backup(tmp_path, f"2024-03-{d:02d}T12:00:00", "daily"))

        result = apply_retention_policy()
        assert len(result["kept"]) == 7
        assert len(result["deleted"]) == 2
        # All files for deleted IDs should be gone
        for bid in result["deleted"]:
            assert not (tmp_path / f"{bid}.zip").exists()

    def test_deletes_excess_monthly_backups(self, tmp_path, monkeypatch):
        from app.sync.backup import apply_retention_policy

        monkeypatch.setenv("BACKUP_PATH", str(tmp_path))
        # Create 8 monthly backups (limit is 6)
        for m in range(1, 9):
            self._write_backup(tmp_path, f"2024-{m:02d}-01T12:00:00", "monthly")

        result = apply_retention_policy()
        assert len(result["kept"]) == 6
        assert len(result["deleted"]) == 2


# ---------------------------------------------------------------------------
# apply_startup_restore
# ---------------------------------------------------------------------------


class TestApplyStartupRestore:
    @pytest.mark.asyncio
    async def test_raises_on_missing_file(self, tmp_path):
        from app.sync.backup import apply_startup_restore

        with pytest.raises(FileNotFoundError):
            await apply_startup_restore(str(tmp_path / "nonexistent.zip"))

    @pytest.mark.asyncio
    async def test_raises_on_non_zip_file(self, tmp_path):
        from app.sync.backup import apply_startup_restore

        bad_file = tmp_path / "bad.zip"
        bad_file.write_bytes(b"not a zip")
        with pytest.raises(ValueError, match="Not a valid ZIP"):
            await apply_startup_restore(str(bad_file))

    @pytest.mark.asyncio
    async def test_raises_on_missing_metadata_json(self, tmp_path):
        from app.sync.backup import apply_startup_restore

        buf = io.BytesIO()
        with zipfile.ZipFile(buf, "w") as zf:
            zf.writestr("database.db", b"data")
        zip_path = tmp_path / "missing_meta.zip"
        zip_path.write_bytes(buf.getvalue())

        with pytest.raises(ValueError, match="missing metadata.json"):
            await apply_startup_restore(str(zip_path))

    @pytest.mark.asyncio
    async def test_raises_on_missing_database_db(self, tmp_path):
        from app.sync.backup import apply_startup_restore

        buf = io.BytesIO()
        with zipfile.ZipFile(buf, "w") as zf:
            zf.writestr("metadata.json", json.dumps({"backup_id": "x"}))
        zip_path = tmp_path / "missing_db.zip"
        zip_path.write_bytes(buf.getvalue())

        with pytest.raises(ValueError, match="missing database.db"):
            await apply_startup_restore(str(zip_path))

    @pytest.mark.asyncio
    async def test_successful_restore_returns_metadata(self, tmp_path, monkeypatch):
        from app.sync.backup import apply_startup_restore

        # Create a minimal SQLite DB for the restore source
        src_db = tmp_path / "source.db"
        conn = sqlite3.connect(str(src_db))
        conn.execute("CREATE TABLE _dummy (id INTEGER PRIMARY KEY)")
        conn.commit()
        conn.close()

        dst_db = tmp_path / "live.db"
        dst_conn = sqlite3.connect(str(dst_db))
        dst_conn.execute("CREATE TABLE _old (id INTEGER PRIMARY KEY)")
        dst_conn.commit()
        dst_conn.close()

        meta = {"backup_id": "backup-20240101-120000-daily", "created_at": "2024-01-01T12:00:00"}
        buf = io.BytesIO()
        with zipfile.ZipFile(buf, "w") as zf:
            zf.writestr("metadata.json", json.dumps(meta))
            zf.writestr("database.db", src_db.read_bytes())
        zip_path = tmp_path / "valid.zip"
        zip_path.write_bytes(buf.getvalue())

        from types import SimpleNamespace
        monkeypatch.setattr(
            "app.sync.backup.get_settings",
            lambda: SimpleNamespace(database_path=str(dst_db)),
        )

        result = await apply_startup_restore(str(zip_path))
        assert result["backup_id"] == "backup-20240101-120000-daily"


# ---------------------------------------------------------------------------
# create_backup
# ---------------------------------------------------------------------------


class TestCreateBackup:
    @pytest.mark.asyncio
    async def test_creates_zip_file_with_expected_contents(self, test_db, tmp_path, monkeypatch):
        from app.sync.backup import create_backup

        monkeypatch.setenv("BACKUP_PATH", str(tmp_path))

        user_id = await _insert_user("backup-user@example.com", "backup-google")

        # Mock the heavy async parts
        async def fake_snapshot_user(user: dict) -> dict:
            return {
                "user_id": user["id"],
                "user_email": user["email"],
                "main_calendar_id": user["main_calendar_id"],
                "main_calendar_events": [],
                "client_calendars": [],
                "errors": [],
            }

        # Point settings at an in-memory path so sqlite3.connect won't fail
        import types
        monkeypatch.setattr("app.sync.backup._snapshot_user", fake_snapshot_user)
        # Use a real temp file for the DB so the sqlite3 backup API works
        src_db_path = tmp_path / "test.db"
        conn = sqlite3.connect(str(src_db_path))
        conn.execute("CREATE TABLE _dummy (id INTEGER PRIMARY KEY)")
        conn.commit()
        conn.close()
        monkeypatch.setattr(
            "app.sync.backup.get_settings",
            lambda: types.SimpleNamespace(database_path=str(src_db_path)),
        )

        metadata = await create_backup()

        assert metadata["backup_id"].startswith("backup-")
        assert metadata["backup_type"] in ("daily", "weekly", "monthly")
        assert metadata["file_size_bytes"] > 0

        # Verify ZIP structure
        zip_path = tmp_path / f"{metadata['backup_id']}.zip"
        assert zip_path.exists()
        with zipfile.ZipFile(str(zip_path)) as zf:
            names = zf.namelist()
            assert "metadata.json" in names
            assert "database.db" in names

    @pytest.mark.asyncio
    async def test_creates_backup_with_specific_user_ids(self, test_db, tmp_path, monkeypatch):
        from app.sync.backup import create_backup
        import types

        monkeypatch.setenv("BACKUP_PATH", str(tmp_path))
        user_id = await _insert_user("specific-user@example.com", "specific-google")

        async def fake_snapshot_user(user: dict) -> dict:
            return {
                "user_id": user["id"],
                "user_email": user["email"],
                "main_calendar_id": user["main_calendar_id"],
                "main_calendar_events": [],
                "client_calendars": [],
                "errors": [],
            }

        monkeypatch.setattr("app.sync.backup._snapshot_user", fake_snapshot_user)
        src_db_path = tmp_path / "test.db"
        conn = sqlite3.connect(str(src_db_path))
        conn.execute("CREATE TABLE _dummy (id INTEGER PRIMARY KEY)")
        conn.commit()
        conn.close()
        monkeypatch.setattr(
            "app.sync.backup.get_settings",
            lambda: types.SimpleNamespace(database_path=str(src_db_path)),
        )

        metadata = await create_backup(user_ids=[user_id])
        assert user_id in metadata["user_ids_snapshotted"]


# ---------------------------------------------------------------------------
# restore_from_backup — error paths
# ---------------------------------------------------------------------------


class TestRestoreFromBackupErrors:
    @pytest.mark.asyncio
    async def test_raises_file_not_found_for_missing_backup(self, test_db, tmp_path, monkeypatch):
        from app.sync.backup import restore_from_backup

        monkeypatch.setenv("BACKUP_PATH", str(tmp_path))
        with pytest.raises(FileNotFoundError):
            await restore_from_backup("backup-nonexistent")


# ---------------------------------------------------------------------------
# restore_from_backup — dry_run
# ---------------------------------------------------------------------------


class TestRestoreFromBackupDryRun:
    @pytest.mark.asyncio
    async def test_dry_run_returns_planned_actions_without_modifying_db(
        self, test_db, tmp_path, monkeypatch
    ):
        from app.sync.backup import restore_from_backup

        monkeypatch.setenv("BACKUP_PATH", str(tmp_path))

        user_id = await _insert_user("dry-user@example.com", "dry-google")

        backup_snap = {
            "main_calendar_events": [{"id": "new-evt", "summary": "New Event"}],
            "client_calendars": [],
        }
        metadata = {
            "backup_id": "backup-20240101-120000-daily",
            "backup_type": "daily",
            "created_at": "2024-01-01T12:00:00",
            "user_ids_snapshotted": [user_id],
        }
        zip_data = _make_backup_zip(metadata, snapshots={str(user_id): backup_snap})
        bid = metadata["backup_id"]
        (tmp_path / f"{bid}.zip").write_bytes(zip_data)

        # Mock the live event fetch to return an empty list (event will show as "create")
        async def fake_fetch_current(*args, **kwargs):
            return []

        monkeypatch.setattr("app.sync.backup._fetch_current_busybridge_events", fake_fetch_current)

        result = await restore_from_backup(
            bid,
            restore_db=False,
            restore_calendars=True,
            dry_run=True,
        )

        assert result["dry_run"] is True
        assert isinstance(result["planned_actions"], list)
        # DB should not be flagged as restored in dry_run mode
        assert result["db_restored"] is False
