"""Database connection and schema management."""

import asyncio
import logging
from contextlib import asynccontextmanager
from datetime import datetime
from typing import AsyncGenerator, Optional

import aiosqlite

from app.config import get_settings

logger = logging.getLogger(__name__)

# Global database connection pool
_db_connection: Optional[aiosqlite.Connection] = None
_db_lock = asyncio.Lock()


SCHEMA = """
-- The home organization (single row)
CREATE TABLE IF NOT EXISTS organization (
    id INTEGER PRIMARY KEY,
    google_workspace_domain TEXT NOT NULL UNIQUE,
    google_client_id_encrypted BLOB NOT NULL,
    google_client_secret_encrypted BLOB NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP
);

-- System settings
CREATE TABLE IF NOT EXISTS settings (
    key TEXT PRIMARY KEY,
    value_encrypted BLOB,
    value_plain TEXT,
    is_sensitive BOOLEAN DEFAULT FALSE,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Users in the home org
CREATE TABLE IF NOT EXISTS users (
    id INTEGER PRIMARY KEY,
    email TEXT NOT NULL UNIQUE,
    google_user_id TEXT NOT NULL UNIQUE,
    display_name TEXT,
    main_calendar_id TEXT,
    is_admin BOOLEAN DEFAULT FALSE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    last_login_at TIMESTAMP
);

-- OAuth tokens (encrypted at rest)
CREATE TABLE IF NOT EXISTS oauth_tokens (
    id INTEGER PRIMARY KEY,
    user_id INTEGER NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    account_type TEXT NOT NULL,
    google_account_email TEXT NOT NULL,
    access_token_encrypted BLOB NOT NULL,
    refresh_token_encrypted BLOB NOT NULL,
    token_expiry TIMESTAMP,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP,
    UNIQUE(user_id, google_account_email)
);

-- Connected client calendars
CREATE TABLE IF NOT EXISTS client_calendars (
    id INTEGER PRIMARY KEY,
    user_id INTEGER NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    oauth_token_id INTEGER NOT NULL REFERENCES oauth_tokens(id),
    google_calendar_id TEXT NOT NULL,
    display_name TEXT,
    is_active BOOLEAN DEFAULT TRUE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    disconnected_at TIMESTAMP
);

-- Tracks sync state for each calendar
CREATE TABLE IF NOT EXISTS calendar_sync_state (
    id INTEGER PRIMARY KEY,
    client_calendar_id INTEGER NOT NULL REFERENCES client_calendars(id) ON DELETE CASCADE,
    sync_token TEXT,
    last_full_sync TIMESTAMP,
    last_incremental_sync TIMESTAMP,
    consecutive_failures INTEGER DEFAULT 0,
    last_error TEXT,
    UNIQUE(client_calendar_id)
);

-- The core event mapping table
CREATE TABLE IF NOT EXISTS event_mappings (
    id INTEGER PRIMARY KEY,
    user_id INTEGER NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    origin_type TEXT NOT NULL,
    origin_calendar_id INTEGER REFERENCES client_calendars(id),
    origin_event_id TEXT NOT NULL,
    origin_recurring_event_id TEXT,
    main_event_id TEXT,
    event_start TIMESTAMP,
    event_end TIMESTAMP,
    is_all_day BOOLEAN DEFAULT FALSE,
    is_recurring BOOLEAN DEFAULT FALSE,
    user_can_edit BOOLEAN DEFAULT TRUE,
    deleted_at TIMESTAMP,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP,
    UNIQUE(user_id, origin_calendar_id, origin_event_id)
);

-- Index for retention cleanup
CREATE INDEX IF NOT EXISTS idx_event_mappings_cleanup
    ON event_mappings(is_recurring, event_end, deleted_at);

-- Busy blocks created on client calendars
CREATE TABLE IF NOT EXISTS busy_blocks (
    id INTEGER PRIMARY KEY,
    event_mapping_id INTEGER NOT NULL REFERENCES event_mappings(id) ON DELETE CASCADE,
    client_calendar_id INTEGER NOT NULL REFERENCES client_calendars(id),
    busy_block_event_id TEXT NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(event_mapping_id, client_calendar_id)
);

-- Audit log
CREATE TABLE IF NOT EXISTS sync_log (
    id INTEGER PRIMARY KEY,
    user_id INTEGER REFERENCES users(id),
    calendar_id INTEGER REFERENCES client_calendars(id),
    action TEXT NOT NULL,
    status TEXT NOT NULL,
    details TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_sync_log_created ON sync_log(created_at);
CREATE INDEX IF NOT EXISTS idx_sync_log_user ON sync_log(user_id, created_at);

-- Webhook registrations
CREATE TABLE IF NOT EXISTS webhook_channels (
    id INTEGER PRIMARY KEY,
    user_id INTEGER NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    calendar_type TEXT NOT NULL,
    client_calendar_id INTEGER REFERENCES client_calendars(id),
    channel_id TEXT NOT NULL UNIQUE,
    resource_id TEXT NOT NULL,
    expiration TIMESTAMP NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_webhook_expiration ON webhook_channels(expiration);

-- Email alert queue
CREATE TABLE IF NOT EXISTS alert_queue (
    id INTEGER PRIMARY KEY,
    alert_type TEXT NOT NULL,
    recipient_email TEXT NOT NULL,
    subject TEXT NOT NULL,
    body TEXT NOT NULL,
    attempts INTEGER DEFAULT 0,
    last_attempt TIMESTAMP,
    sent_at TIMESTAMP,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Job locking
CREATE TABLE IF NOT EXISTS job_locks (
    job_name TEXT PRIMARY KEY,
    locked_at TIMESTAMP,
    locked_by TEXT
);

-- Main calendar sync state (for tracking main calendar changes)
CREATE TABLE IF NOT EXISTS main_calendar_sync_state (
    id INTEGER PRIMARY KEY,
    user_id INTEGER NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    sync_token TEXT,
    last_full_sync TIMESTAMP,
    last_incremental_sync TIMESTAMP,
    consecutive_failures INTEGER DEFAULT 0,
    last_error TEXT,
    UNIQUE(user_id)
);

-- OAuth state storage (replaces in-memory dict)
CREATE TABLE IF NOT EXISTS oauth_states (
    state TEXT PRIMARY KEY,
    state_type TEXT NOT NULL,
    user_id INTEGER,
    next_url TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    expires_at TIMESTAMP NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_oauth_states_expiry ON oauth_states(expires_at);
"""


async def get_database() -> aiosqlite.Connection:
    """Get the database connection, creating it if necessary."""
    global _db_connection

    async with _db_lock:
        if _db_connection is None:
            settings = get_settings()
            _db_connection = await aiosqlite.connect(settings.database_path)
            _db_connection.row_factory = aiosqlite.Row
            await _db_connection.execute("PRAGMA foreign_keys = ON")
            await _db_connection.execute("PRAGMA journal_mode = WAL")
            await init_schema(_db_connection)
        return _db_connection


async def init_schema(db: aiosqlite.Connection) -> None:
    """Initialize database schema."""
    await db.executescript(SCHEMA)
    await db.commit()
    logger.info("Database schema initialized")


async def close_database() -> None:
    """Close the database connection."""
    global _db_connection

    async with _db_lock:
        if _db_connection is not None:
            await _db_connection.close()
            _db_connection = None
            logger.info("Database connection closed")


@asynccontextmanager
async def get_db() -> AsyncGenerator[aiosqlite.Connection, None]:
    """Context manager for getting database connection."""
    db = await get_database()
    try:
        yield db
    finally:
        pass  # Connection is managed globally


async def is_oobe_completed() -> bool:
    """Check if the Out-of-Box Experience has been completed."""
    db = await get_database()
    cursor = await db.execute("SELECT COUNT(*) FROM organization")
    row = await cursor.fetchone()
    return row[0] > 0


async def get_organization() -> Optional[dict]:
    """Get the organization configuration."""
    db = await get_database()
    cursor = await db.execute("SELECT * FROM organization LIMIT 1")
    row = await cursor.fetchone()
    if row:
        return dict(row)
    return None


async def get_setting(key: str) -> Optional[dict]:
    """Get a setting by key."""
    db = await get_database()
    cursor = await db.execute(
        "SELECT * FROM settings WHERE key = ?", (key,)
    )
    row = await cursor.fetchone()
    if row:
        return dict(row)
    return None


async def set_setting(
    key: str,
    value: str,
    is_sensitive: bool = False,
    encrypt_func=None
) -> None:
    """Set a setting value."""
    db = await get_database()
    now = datetime.utcnow().isoformat()

    if is_sensitive and encrypt_func:
        value_encrypted = encrypt_func(value)
        await db.execute(
            """INSERT INTO settings (key, value_encrypted, is_sensitive, updated_at)
               VALUES (?, ?, TRUE, ?)
               ON CONFLICT(key) DO UPDATE SET
               value_encrypted = excluded.value_encrypted,
               is_sensitive = TRUE,
               updated_at = excluded.updated_at""",
            (key, value_encrypted, now)
        )
    else:
        await db.execute(
            """INSERT INTO settings (key, value_plain, is_sensitive, updated_at)
               VALUES (?, ?, FALSE, ?)
               ON CONFLICT(key) DO UPDATE SET
               value_plain = excluded.value_plain,
               is_sensitive = FALSE,
               updated_at = excluded.updated_at""",
            (key, value, now)
        )
    await db.commit()
