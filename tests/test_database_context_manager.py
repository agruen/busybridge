"""Coverage test for database get_db context manager."""

from __future__ import annotations

import pytest

from app.database import get_database, get_db


@pytest.mark.asyncio
async def test_get_db_context_manager_returns_connection(test_db):
    """get_db should yield the shared DB connection without closing it."""
    db = await get_database()

    async with get_db() as ctx_db:
        assert ctx_db is db
