"""Calendar backup and rollback logic.

A backup is a ZIP file containing:
  - metadata.json         : version, timestamps, user list, event counts
  - database.db           : binary copy of the live SQLite database
  - snapshots/<uid>.json  : per-user Google Calendar event snapshots

Retention policy (applied automatically after every scheduled backup):
  - 7 daily   (most-recent backup from each of the last 7 days)
  - 2 weekly  (most-recent Sunday backup, last 2)
  - 6 monthly (most-recent 1st-of-month backup, last 6)
"""

import io
import json
import logging
import os
import sqlite3
import tempfile
import zipfile
from datetime import datetime
from typing import Optional

from app.config import get_settings
from app.database import get_database

logger = logging.getLogger(__name__)

BACKUP_VERSION = "1"


# ---------------------------------------------------------------------------
# Directory helpers
# ---------------------------------------------------------------------------

def get_backup_dir() -> str:
    """Return the backup directory, creating it if necessary."""
    path = os.environ.get("BACKUP_PATH", "/data/backups")
    os.makedirs(path, exist_ok=True)
    return path


def _backup_filepath(backup_id: str) -> str:
    return os.path.join(get_backup_dir(), f"{backup_id}.zip")


# ---------------------------------------------------------------------------
# Retention / classification helpers
# ---------------------------------------------------------------------------

def _classify_backup(dt: datetime) -> str:
    """Return 'monthly', 'weekly', or 'daily' for a backup datetime."""
    if dt.day == 1:
        return "monthly"
    if dt.weekday() == 6:  # Sunday
        return "weekly"
    return "daily"


def apply_retention_policy() -> dict:
    """
    Enforce 7 daily / 2 weekly / 6 monthly retention.

    Returns {'kept': [...], 'deleted': [...]} lists of backup IDs.
    """
    limits = {"daily": 7, "weekly": 2, "monthly": 6}
    backups = list_backups()  # newest-first

    by_type: dict[str, list] = {"daily": [], "weekly": [], "monthly": []}
    for b in backups:
        btype = b.get("backup_type", "daily")
        if btype in by_type:
            by_type[btype].append(b)

    to_delete: list[str] = []
    kept: list[str] = []

    for btype, limit in limits.items():
        for i, entry in enumerate(by_type[btype]):
            if i < limit:
                kept.append(entry["backup_id"])
            else:
                to_delete.append(entry["backup_id"])

    for backup_id in to_delete:
        path = _backup_filepath(backup_id)
        try:
            os.remove(path)
            logger.info(f"Retention: deleted backup {backup_id}")
        except OSError as e:
            logger.warning(f"Could not delete backup {backup_id}: {e}")

    return {"kept": kept, "deleted": to_delete}


# ---------------------------------------------------------------------------
# Event snapshot helpers
# ---------------------------------------------------------------------------

_SNAPSHOT_FIELDS = [
    "id", "summary", "description", "location", "status",
    "start", "end", "recurrence", "recurringEventId", "originalStartTime",
    "extendedProperties", "colorId", "transparency", "visibility",
    "attendees", "organizer", "guestsCanModify", "guestsCanInviteOthers",
    "guestsCanSeeOtherGuests", "reminders",
]


def _event_snapshot_fields(event: dict) -> dict:
    """Keep only the fields needed to store and later restore an event."""
    return {k: event[k] for k in _SNAPSHOT_FIELDS if k in event}


async def _snapshot_user(user: dict) -> dict:
    """Fetch all BusyBridge-managed events for one user across all calendars."""
    from app.auth.google import get_valid_access_token
    from app.sync.google_calendar import GoogleCalendarClient

    user_id = user["id"]
    settings = get_settings()

    result: dict = {
        "user_id": user_id,
        "user_email": user["email"],
        "main_calendar_id": user["main_calendar_id"],
        "main_calendar_events": [],
        "client_calendars": [],
        "errors": [],
    }

    # Main calendar
    try:
        token = await get_valid_access_token(user_id, user["email"])
        main_client = GoogleCalendarClient(token, settings)
        resp = main_client.list_events(user["main_calendar_id"], single_events=False)
        result["main_calendar_events"] = [
            _event_snapshot_fields(e)
            for e in resp.get("events", [])
            if main_client.is_our_event(e) and e.get("status") != "cancelled"
        ]
    except Exception as e:
        msg = f"main calendar snapshot failed: {e}"
        logger.error(f"User {user_id}: {msg}")
        result["errors"].append(msg)

    # Client calendars
    db = await get_database()
    cursor = await db.execute(
        """SELECT cc.id, cc.google_calendar_id, cc.display_name,
                  ot.google_account_email
           FROM client_calendars cc
           JOIN oauth_tokens ot ON cc.oauth_token_id = ot.id
           WHERE cc.user_id = ? AND cc.is_active = TRUE""",
        (user_id,),
    )
    client_cals = await cursor.fetchall()

    for cal in client_cals:
        cal_entry: dict = {
            "client_calendar_id": cal["id"],
            "calendar_id": cal["google_calendar_id"],
            "display_name": cal["display_name"],
            "events": [],
            "errors": [],
        }
        try:
            token = await get_valid_access_token(user_id, cal["google_account_email"])
            client = GoogleCalendarClient(token, settings)
            resp = client.list_events(cal["google_calendar_id"], single_events=False)
            cal_entry["events"] = [
                _event_snapshot_fields(e)
                for e in resp.get("events", [])
                if client.is_our_event(e) and e.get("status") != "cancelled"
            ]
        except Exception as e:
            msg = f"client calendar {cal['id']} snapshot failed: {e}"
            logger.error(f"User {user_id}: {msg}")
            cal_entry["errors"].append(msg)

        result["client_calendars"].append(cal_entry)

    return result


# ---------------------------------------------------------------------------
# Backup creation
# ---------------------------------------------------------------------------

async def create_backup(user_ids: Optional[list[int]] = None) -> dict:
    """Create a full backup ZIP.

    Args:
        user_ids: If provided, only snapshot these users' calendars. The
                  database dump is always the complete database.

    Returns metadata dict (backup_id, created_at, counts, etc.).
    """
    settings = get_settings()
    now = datetime.now()  # respects TZ env var
    backup_type = _classify_backup(now)
    backup_id = f"backup-{now.strftime('%Y%m%d-%H%M%S')}-{backup_type}"
    zip_path = _backup_filepath(backup_id)

    db = await get_database()

    # Which users to snapshot
    if user_ids:
        placeholders = ",".join("?" * len(user_ids))
        cursor = await db.execute(
            f"SELECT * FROM users WHERE id IN ({placeholders}) AND main_calendar_id IS NOT NULL",
            user_ids,
        )
    else:
        cursor = await db.execute(
            "SELECT * FROM users WHERE main_calendar_id IS NOT NULL"
        )
    users = await cursor.fetchall()

    snapshots: dict[str, dict] = {}
    total_events = 0
    snapshot_errors: list[str] = []

    for user in users:
        snap = await _snapshot_user(dict(user))
        snapshots[str(user["id"])] = snap
        total_events += len(snap["main_calendar_events"])
        total_events += sum(len(c["events"]) for c in snap["client_calendars"])
        snapshot_errors.extend(snap.get("errors", []))

    metadata = {
        "version": BACKUP_VERSION,
        "backup_id": backup_id,
        "backup_type": backup_type,
        "created_at": now.isoformat(),
        "user_ids_snapshotted": [u["id"] for u in users],
        "total_events_snapshotted": total_events,
        "snapshot_errors": snapshot_errors,
    }

    with zipfile.ZipFile(zip_path, "w", zipfile.ZIP_DEFLATED) as zf:
        zf.writestr("metadata.json", json.dumps(metadata, indent=2))

        # Consistent DB copy via sqlite3 backup API (WAL-safe)
        with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as tmp:
            tmp_path = tmp.name
        try:
            src_conn = sqlite3.connect(settings.database_path)
            dst_conn = sqlite3.connect(tmp_path)
            src_conn.backup(dst_conn)
            src_conn.close()
            dst_conn.close()
            with open(tmp_path, "rb") as f:
                zf.writestr("database.db", f.read())
        finally:
            os.unlink(tmp_path)

        for uid, snap in snapshots.items():
            zf.writestr(f"snapshots/{uid}.json", json.dumps(snap, indent=2))

    file_size = os.path.getsize(zip_path)
    metadata["file_size_bytes"] = file_size

    logger.info(
        f"Backup created: {backup_id} "
        f"({file_size} bytes, {total_events} events, {len(users)} users)"
    )
    return metadata


# ---------------------------------------------------------------------------
# Listing and deleting
# ---------------------------------------------------------------------------

def list_backups() -> list[dict]:
    """Return all backups sorted newest-first."""
    backup_dir = get_backup_dir()
    results: list[dict] = []

    for fname in os.listdir(backup_dir):
        if not (fname.startswith("backup-") and fname.endswith(".zip")):
            continue
        fpath = os.path.join(backup_dir, fname)
        backup_id = fname[: -len(".zip")]
        try:
            with zipfile.ZipFile(fpath, "r") as zf:
                with zf.open("metadata.json") as f:
                    meta = json.load(f)
        except Exception:
            meta = {"backup_id": backup_id}

        meta["file_size_bytes"] = os.path.getsize(fpath)
        results.append(meta)

    results.sort(key=lambda m: m.get("created_at", ""), reverse=True)
    return results


def delete_backup(backup_id: str) -> bool:
    """Delete a backup by ID. Returns True if deleted, False if not found."""
    path = _backup_filepath(backup_id)
    if not os.path.exists(path):
        return False
    os.remove(path)
    logger.info(f"Deleted backup {backup_id}")
    return True


# ---------------------------------------------------------------------------
# Startup restore (catastrophic recovery path)
# ---------------------------------------------------------------------------

async def apply_startup_restore(zip_path: str) -> dict:
    """Restore the database from a backup ZIP before the scheduler starts.

    This is the catastrophic recovery entry point.  It must be called
    BEFORE get_database() opens the aiosqlite connection so that aiosqlite
    sees the restored file when it first opens it.

    It does NOT reconcile Google Calendar events — that is left to the
    normal consistency check on the first scheduled run after startup.

    Returns the backup metadata dict.
    Raises on any validation or I/O error (caller should abort startup).
    """
    if not os.path.exists(zip_path):
        raise FileNotFoundError(f"Restore file not found: {zip_path}")
    if not zipfile.is_zipfile(zip_path):
        raise ValueError(f"Not a valid ZIP file: {zip_path}")

    with zipfile.ZipFile(zip_path, "r") as zf:
        names = zf.namelist()
        if "metadata.json" not in names:
            raise ValueError("Not a valid BusyBridge backup: missing metadata.json")
        if "database.db" not in names:
            raise ValueError("Not a valid BusyBridge backup: missing database.db")

        with zf.open("metadata.json") as f:
            metadata = json.load(f)

        # Replace the DB file at the filesystem level using sqlite3 — no
        # aiosqlite connection is open yet so this is safe.
        await _restore_full_db(zf)

    logger.info(
        f"Startup restore: database replaced with backup "
        f"'{metadata.get('backup_id', 'unknown')}' "
        f"(originally created {metadata.get('created_at', 'unknown')})"
    )
    return metadata


# ---------------------------------------------------------------------------
# Restore helpers
# ---------------------------------------------------------------------------

def _events_differ(a: dict, b: dict) -> bool:
    """Return True if two event dicts differ in any meaningful field."""
    for k in ("summary", "description", "start", "end", "status",
              "recurrence", "transparency", "colorId"):
        if a.get(k) != b.get(k):
            return True
    return False


def _diff_events(backup_events: list[dict], current_events: list[dict]) -> dict:
    """Compute create / update / delete lists.

    Returns:
        {
            "create": events from backup missing in current,
            "update": events in both but with different data,
            "delete": events in current not found in backup,
        }
    """
    backup_by_id = {e["id"]: e for e in backup_events}
    current_by_id = {e["id"]: e for e in current_events}

    return {
        "create": [e for eid, e in backup_by_id.items() if eid not in current_by_id],
        "delete": [e for eid, e in current_by_id.items() if eid not in backup_by_id],
        "update": [
            backup_by_id[eid]
            for eid in backup_by_id
            if eid in current_by_id and _events_differ(backup_by_id[eid], current_by_id[eid])
        ],
    }


async def _fetch_current_busybridge_events(
    user_id: int, calendar_id: str, email: str
) -> list[dict]:
    """Fetch all current BusyBridge-owned events from one Google calendar."""
    from app.auth.google import get_valid_access_token
    from app.sync.google_calendar import GoogleCalendarClient

    settings = get_settings()
    token = await get_valid_access_token(user_id, email)
    client = GoogleCalendarClient(token, settings)
    resp = client.list_events(calendar_id, single_events=False)
    return [
        _event_snapshot_fields(e)
        for e in resp.get("events", [])
        if client.is_our_event(e) and e.get("status") != "cancelled"
    ]


async def _apply_calendar_diff(
    user_id: int,
    calendar_id: str,
    email: str,
    backup_events: list[dict],
    current_events: list[dict],
    dry_run: bool,
) -> dict:
    """Apply the diff between backup and current events on one calendar.

    Returns action counts plus an id_remaps dict (old_id -> new_id) for
    events that were recreated and received a new Google-assigned ID.
    """
    from app.auth.google import get_valid_access_token
    from app.sync.google_calendar import GoogleCalendarClient

    diff = _diff_events(backup_events, current_events)

    if dry_run:
        actions = (
            [{"action": "delete", "event_id": e["id"], "summary": e.get("summary", "")}
             for e in diff["delete"]]
            + [{"action": "create", "event_id": e["id"], "summary": e.get("summary", "")}
               for e in diff["create"]]
            + [{"action": "update", "event_id": e["id"], "summary": e.get("summary", "")}
               for e in diff["update"]]
        )
        return {"planned_actions": actions, "id_remaps": {},
                "deleted": 0, "created": 0, "updated": 0, "errors": []}

    settings = get_settings()
    token = await get_valid_access_token(user_id, email)
    client = GoogleCalendarClient(token, settings)
    deleted = created = updated = 0
    errors: list[str] = []
    id_remaps: dict[str, str] = {}

    for event in diff["delete"]:
        try:
            client.delete_event(calendar_id, event["id"], send_notifications=False)
            deleted += 1
        except Exception as e:
            errors.append(f"delete {event['id']}: {e}")

    for event in diff["create"]:
        try:
            # Strip the old ID so Google assigns a fresh one
            payload = {k: v for k, v in event.items() if k != "id"}
            new_event = client.create_event(calendar_id, payload, send_notifications=False)
            id_remaps[event["id"]] = new_event["id"]
            created += 1
        except Exception as e:
            errors.append(f"create {event.get('id')}: {e}")

    for event in diff["update"]:
        try:
            client.update_event(calendar_id, event["id"], event, send_notifications=False)
            updated += 1
        except Exception as e:
            errors.append(f"update {event['id']}: {e}")

    return {"deleted": deleted, "created": created, "updated": updated,
            "errors": errors, "id_remaps": id_remaps}


async def _update_db_event_ids(
    user_id: int,
    id_remaps: dict[str, str],
    calendar_type: str,
    client_calendar_id: Optional[int] = None,
) -> None:
    """Patch event_mappings / busy_blocks after events are recreated with new Google IDs."""
    if not id_remaps:
        return
    db = await get_database()
    if calendar_type == "main":
        for old_id, new_id in id_remaps.items():
            await db.execute(
                "UPDATE event_mappings SET main_event_id = ? WHERE main_event_id = ? AND user_id = ?",
                (new_id, old_id, user_id),
            )
    elif calendar_type == "client" and client_calendar_id is not None:
        for old_id, new_id in id_remaps.items():
            await db.execute(
                """UPDATE busy_blocks SET busy_event_id = ?
                   WHERE busy_event_id = ? AND client_calendar_id = ?""",
                (new_id, old_id, client_calendar_id),
            )
    await db.commit()


# FK-safe delete order for per-user rows
_USER_DELETE_ORDER: list[tuple[str, str]] = [
    ("busy_blocks",
     "event_mapping_id IN (SELECT id FROM event_mappings WHERE user_id = ?)"),
    ("webhook_channels",
     "client_calendar_id IN (SELECT id FROM client_calendars WHERE user_id = ?)"),
    ("calendar_sync_state",
     "client_calendar_id IN (SELECT id FROM client_calendars WHERE user_id = ?)"),
    ("event_mappings",            "user_id = ?"),
    ("main_calendar_sync_state",  "user_id = ?"),
    ("sync_log",                  "user_id = ?"),
    ("client_calendars",          "user_id = ?"),
    ("oauth_tokens",              "user_id = ?"),
    ("users",                     "id = ?"),
]

# FK-safe insert order (reverse of delete)
_USER_INSERT_ORDER = [
    "users", "oauth_tokens", "client_calendars",
    "calendar_sync_state", "main_calendar_sync_state",
    "event_mappings", "busy_blocks", "webhook_channels",
    "sync_log",
]

# How to filter backup rows by user_id for each table.
# None means use the subquery in _USER_SUBQUERY instead.
_USER_COLUMN: dict[str, Optional[str]] = {
    "users":                    "id",
    "oauth_tokens":             "user_id",
    "client_calendars":         "user_id",
    "calendar_sync_state":      None,
    "main_calendar_sync_state": "user_id",
    "event_mappings":           "user_id",
    "busy_blocks":              None,
    "webhook_channels":         None,
    "sync_log":                 "user_id",
}

_USER_SUBQUERY: dict[str, str] = {
    "calendar_sync_state":
        "client_calendar_id IN (SELECT id FROM client_calendars WHERE user_id = ?)",
    "busy_blocks":
        "event_mapping_id IN (SELECT id FROM event_mappings WHERE user_id = ?)",
    "webhook_channels":
        "client_calendar_id IN (SELECT id FROM client_calendars WHERE user_id = ?)",
}


def _fetch_backup_user_rows(bk_conn: sqlite3.Connection, table: str, user_id: int) -> list:
    """Fetch rows belonging to user_id from the backup (read-only) sqlite3 connection."""
    bk_conn.row_factory = sqlite3.Row
    cur = bk_conn.cursor()
    subquery = _USER_SUBQUERY.get(table)
    if subquery:
        cur.execute(f"SELECT * FROM {table} WHERE {subquery}", (user_id,))
    else:
        col = _USER_COLUMN.get(table)
        if not col:
            return []
        cur.execute(f"SELECT * FROM {table} WHERE {col} = ?", (user_id,))
    return cur.fetchall()


async def _restore_single_user(live_db, bk_conn: sqlite3.Connection, user_id: int) -> None:
    """Delete a user's live rows and replace them with rows from the backup DB."""
    # Delete in FK-safe order
    for table, where in _USER_DELETE_ORDER:
        await live_db.execute(f"DELETE FROM {table} WHERE {where}", (user_id,))

    # Insert in FK-safe order
    for table in _USER_INSERT_ORDER:
        rows = _fetch_backup_user_rows(bk_conn, table, user_id)
        if not rows:
            continue
        col_names = list(rows[0].keys())
        placeholders = ", ".join("?" * len(col_names))
        cols_str = ", ".join(col_names)
        sql = f"INSERT OR REPLACE INTO {table} ({cols_str}) VALUES ({placeholders})"
        for row in rows:
            await live_db.execute(sql, list(row))


async def _restore_db_for_users(backup_zip: zipfile.ZipFile, user_ids: list[int]) -> None:
    """Restore database rows for specific users from the backup ZIP."""
    with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as tmp:
        tmp_path = tmp.name
    try:
        with backup_zip.open("database.db") as src:
            with open(tmp_path, "wb") as dst:
                dst.write(src.read())

        bk_conn = sqlite3.connect(f"file:{tmp_path}?mode=ro", uri=True)
        live_db = await get_database()

        for uid in user_ids:
            await _restore_single_user(live_db, bk_conn, uid)

        bk_conn.close()
        await live_db.commit()
    finally:
        os.unlink(tmp_path)


async def _restore_full_db(backup_zip: zipfile.ZipFile) -> None:
    """Replace the entire live database with the backup DB (sqlite3 backup API)."""
    settings = get_settings()
    with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as tmp:
        tmp_path = tmp.name
    try:
        with backup_zip.open("database.db") as src:
            with open(tmp_path, "wb") as dst:
                dst.write(src.read())

        src_conn = sqlite3.connect(tmp_path)
        dst_conn = sqlite3.connect(settings.database_path)
        src_conn.backup(dst_conn)
        src_conn.close()
        dst_conn.close()
    finally:
        os.unlink(tmp_path)


async def _clear_sync_tokens(user_ids: Optional[list[int]] = None) -> None:
    """Null out sync tokens so the next sync does a clean full re-fetch."""
    db = await get_database()
    if user_ids:
        placeholders = ",".join("?" * len(user_ids))
        await db.execute(
            f"""UPDATE calendar_sync_state SET sync_token = NULL
                WHERE client_calendar_id IN (
                    SELECT id FROM client_calendars WHERE user_id IN ({placeholders})
                )""",
            user_ids,
        )
        await db.execute(
            f"UPDATE main_calendar_sync_state SET sync_token = NULL WHERE user_id IN ({placeholders})",
            user_ids,
        )
    else:
        await db.execute("UPDATE calendar_sync_state SET sync_token = NULL")
        await db.execute("UPDATE main_calendar_sync_state SET sync_token = NULL")
    await db.commit()


# ---------------------------------------------------------------------------
# Main restore entry point
# ---------------------------------------------------------------------------

async def restore_from_backup(
    backup_id: str,
    user_ids: Optional[list[int]] = None,
    restore_db: bool = True,
    restore_calendars: bool = True,
    dry_run: bool = False,
) -> dict:
    """Restore from a backup.

    Args:
        backup_id:          ID of the backup to restore from.
        user_ids:           Users to restore. None = all users in the backup.
        restore_db:         Whether to restore the database.
        restore_calendars:  Whether to reconcile Google Calendar events.
        dry_run:            Preview changes without applying them.

    Returns a summary dict.
    """
    from app.database import get_setting, set_setting

    zip_path = _backup_filepath(backup_id)
    if not os.path.exists(zip_path):
        raise FileNotFoundError(f"Backup not found: {backup_id}")

    with zipfile.ZipFile(zip_path, "r") as zf:
        with zf.open("metadata.json") as f:
            metadata = json.load(f)

    backup_user_ids: list[int] = metadata.get("user_ids_snapshotted", [])
    target_user_ids: list[int] = user_ids if user_ids else backup_user_ids

    summary: dict = {
        "backup_id": backup_id,
        "dry_run": dry_run,
        "users_restored": [],
        "db_restored": False,
        "events_deleted": 0,
        "events_created": 0,
        "events_updated": 0,
        "errors": [],
        "planned_actions": [] if dry_run else None,
    }

    # Pause sync for the duration of a real restore
    originally_paused = False
    if not dry_run:
        paused_setting = await get_setting("sync_paused")
        originally_paused = bool(
            paused_setting and paused_setting.get("value_plain") == "true"
        )
        if not originally_paused:
            await set_setting("sync_paused", "true")
            logger.info("Restore: sync paused")

    try:
        # Step 1: Snapshot current Google state (before any DB changes)
        current_snapshots: dict[int, dict] = {}
        if restore_calendars:
            db = await get_database()
            for uid in target_user_ids:
                cursor = await db.execute("SELECT * FROM users WHERE id = ?", (uid,))
                user = await cursor.fetchone()
                if not user or not user["main_calendar_id"]:
                    continue
                try:
                    current_snapshots[uid] = await _snapshot_user(dict(user))
                except Exception as e:
                    msg = f"Pre-restore snapshot failed for user {uid}: {e}"
                    logger.error(msg)
                    summary["errors"].append(msg)

        # Step 2: Restore database
        if restore_db and not dry_run:
            restore_all_users = set(target_user_ids) == set(backup_user_ids)
            with zipfile.ZipFile(zip_path, "r") as zf:
                if restore_all_users:
                    await _restore_full_db(zf)
                    logger.info("Restore: full database restored")
                else:
                    await _restore_db_for_users(zf, target_user_ids)
                    logger.info(f"Restore: per-user DB rows restored for {target_user_ids}")
            summary["db_restored"] = True

        # Step 3: Reconcile Google Calendar events
        if restore_calendars:
            with zipfile.ZipFile(zip_path, "r") as zf:
                for uid in target_user_ids:
                    snap_name = f"snapshots/{uid}.json"
                    if snap_name not in zf.namelist():
                        continue
                    with zf.open(snap_name) as f:
                        backup_snap = json.load(f)

                    user_result = await _restore_user_calendars(
                        uid,
                        backup_snap,
                        current_snapshots.get(uid, {}),
                        dry_run,
                    )

                    summary["events_deleted"] += user_result.get("deleted", 0)
                    summary["events_created"] += user_result.get("created", 0)
                    summary["events_updated"] += user_result.get("updated", 0)
                    summary["errors"].extend(user_result.get("errors", []))
                    if dry_run:
                        summary["planned_actions"].extend(
                            user_result.get("planned_actions", [])
                        )
                    summary["users_restored"].append(uid)

        # Step 4: Clear sync tokens so the next sync does a full re-fetch
        if not dry_run:
            await _clear_sync_tokens(target_user_ids if user_ids else None)

        # Step 5: Audit log
        if not dry_run:
            db = await get_database()
            await db.execute(
                """INSERT INTO sync_log (action, status, details)
                   VALUES ('backup_restore', 'success', ?)""",
                (json.dumps({
                    "backup_id": backup_id,
                    "users": target_user_ids,
                    "events_deleted": summary["events_deleted"],
                    "events_created": summary["events_created"],
                    "events_updated": summary["events_updated"],
                }),),
            )
            await db.commit()

    finally:
        if not dry_run and not originally_paused:
            await set_setting("sync_paused", "false")
            logger.info("Restore: sync resumed")

    return summary


async def _restore_user_calendars(
    user_id: int,
    backup_snap: dict,
    current_snap: dict,
    dry_run: bool,
) -> dict:
    """Reconcile Google Calendar events for one user."""
    db = await get_database()
    cursor = await db.execute("SELECT * FROM users WHERE id = ?", (user_id,))
    user = await cursor.fetchone()
    if not user or not user["main_calendar_id"]:
        return {"errors": [f"User {user_id} not found or has no main calendar"]}

    result: dict = {
        "deleted": 0, "created": 0, "updated": 0,
        "errors": [], "planned_actions": [],
    }

    user_email = user["email"]
    main_calendar_id = user["main_calendar_id"]

    # Main calendar
    try:
        if dry_run:
            current_main = await _fetch_current_busybridge_events(
                user_id, main_calendar_id, user_email
            )
        else:
            current_main = current_snap.get("main_calendar_events", [])

        main_result = await _apply_calendar_diff(
            user_id, main_calendar_id, user_email,
            backup_snap.get("main_calendar_events", []),
            current_main,
            dry_run,
        )
        _merge_cal_result(result, main_result, "main", dry_run)
        if not dry_run:
            await _update_db_event_ids(user_id, main_result.get("id_remaps", {}), "main")
    except Exception as e:
        result["errors"].append(f"Main calendar restore error: {e}")

    # Client calendars
    backup_clients = {c["client_calendar_id"]: c
                      for c in backup_snap.get("client_calendars", [])}
    current_clients = {c["client_calendar_id"]: c
                       for c in current_snap.get("client_calendars", [])}

    cursor = await db.execute(
        """SELECT cc.id, cc.google_calendar_id, ot.google_account_email
           FROM client_calendars cc
           JOIN oauth_tokens ot ON cc.oauth_token_id = ot.id
           WHERE cc.user_id = ? AND cc.is_active = TRUE""",
        (user_id,),
    )
    client_cals = await cursor.fetchall()

    for cal in client_cals:
        ccid = cal["id"]
        backup_cal = backup_clients.get(ccid)
        if backup_cal is None:
            continue  # calendar didn't exist at backup time

        try:
            if dry_run:
                current_events = await _fetch_current_busybridge_events(
                    user_id, cal["google_calendar_id"], cal["google_account_email"]
                )
            else:
                current_events = current_clients.get(ccid, {}).get("events", [])

            cal_result = await _apply_calendar_diff(
                user_id, cal["google_calendar_id"], cal["google_account_email"],
                backup_cal.get("events", []),
                current_events,
                dry_run,
            )
            _merge_cal_result(result, cal_result, f"client:{ccid}", dry_run)
            if not dry_run:
                await _update_db_event_ids(
                    user_id, cal_result.get("id_remaps", {}),
                    "client", client_calendar_id=ccid,
                )
        except Exception as e:
            result["errors"].append(f"Client calendar {ccid} restore error: {e}")

    return result


def _merge_cal_result(result: dict, cal_result: dict, label: str, dry_run: bool) -> None:
    """Merge a per-calendar result into the user-level result dict."""
    if dry_run:
        result["planned_actions"].extend(
            [{"calendar": label, **a} for a in cal_result.get("planned_actions", [])]
        )
    else:
        result["deleted"] += cal_result.get("deleted", 0)
        result["created"] += cal_result.get("created", 0)
        result["updated"] += cal_result.get("updated", 0)
        result["errors"].extend(cal_result.get("errors", []))
