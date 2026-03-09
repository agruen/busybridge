"""Read-only SQLite access to the shared BusyBridge database."""

from __future__ import annotations

import logging
from typing import Optional

import aiosqlite

logger = logging.getLogger(__name__)


class DBReader:
    """Read-only database access. Never writes to the shared DB."""

    def __init__(self, db_path: str):
        self._uri = f"file:{db_path}?mode=ro"
        self._db: Optional[aiosqlite.Connection] = None

    async def connect(self) -> None:
        self._db = await aiosqlite.connect(self._uri, uri=True)
        self._db.row_factory = aiosqlite.Row
        logger.info("Connected to database (read-only)")

    async def close(self) -> None:
        if self._db:
            await self._db.close()
            self._db = None

    @property
    def db(self) -> aiosqlite.Connection:
        if self._db is None:
            raise RuntimeError("Database not connected")
        return self._db

    async def get_users(self) -> list[dict]:
        """Get all users."""
        async with self.db.execute("SELECT * FROM users") as cur:
            rows = await cur.fetchall()
            return [dict(r) for r in rows]

    async def get_admin_user(self) -> Optional[dict]:
        """Get the first admin user."""
        async with self.db.execute(
            "SELECT * FROM users WHERE is_admin = 1 LIMIT 1"
        ) as cur:
            row = await cur.fetchone()
            return dict(row) if row else None

    async def get_oauth_tokens(self, user_id: Optional[int] = None) -> list[dict]:
        """Get OAuth tokens, optionally filtered by user."""
        if user_id is not None:
            sql = "SELECT * FROM oauth_tokens WHERE user_id = ?"
            params = (user_id,)
        else:
            sql = "SELECT * FROM oauth_tokens"
            params = ()
        async with self.db.execute(sql, params) as cur:
            rows = await cur.fetchall()
            return [dict(r) for r in rows]

    async def get_client_calendars(
        self, user_id: Optional[int] = None, active_only: bool = True
    ) -> list[dict]:
        """Get client calendars."""
        conditions = []
        params: list = []
        if user_id is not None:
            conditions.append("user_id = ?")
            params.append(user_id)
        if active_only:
            conditions.append("is_active = 1")
        where = " WHERE " + " AND ".join(conditions) if conditions else ""
        async with self.db.execute(
            f"SELECT * FROM client_calendars{where}", params
        ) as cur:
            rows = await cur.fetchall()
            return [dict(r) for r in rows]

    async def get_event_mappings(
        self, user_id: Optional[int] = None
    ) -> list[dict]:
        """Get event mappings."""
        if user_id is not None:
            sql = "SELECT * FROM event_mappings WHERE user_id = ?"
            params = (user_id,)
        else:
            sql = "SELECT * FROM event_mappings"
            params = ()
        async with self.db.execute(sql, params) as cur:
            rows = await cur.fetchall()
            return [dict(r) for r in rows]

    async def get_busy_blocks(
        self, event_mapping_id: Optional[int] = None
    ) -> list[dict]:
        """Get busy blocks."""
        if event_mapping_id is not None:
            sql = "SELECT * FROM busy_blocks WHERE event_mapping_id = ?"
            params = (event_mapping_id,)
        else:
            sql = "SELECT * FROM busy_blocks"
            params = ()
        async with self.db.execute(sql, params) as cur:
            rows = await cur.fetchall()
            return [dict(r) for r in rows]

    async def get_organization(self) -> Optional[dict]:
        """Get organization config (single row)."""
        async with self.db.execute("SELECT * FROM organization LIMIT 1") as cur:
            row = await cur.fetchone()
            return dict(row) if row else None

    async def get_setting(self, key: str) -> Optional[str]:
        """Get a plain-text setting value."""
        async with self.db.execute(
            "SELECT value_plain FROM settings WHERE key = ?", (key,)
        ) as cur:
            row = await cur.fetchone()
            return row["value_plain"] if row else None
