"""Tests for database module."""

import pytest

from app.database import (
    get_database,
    is_oobe_completed,
    get_organization,
    get_setting,
    set_setting,
)


@pytest.mark.asyncio
async def test_database_connection(test_db):
    """Test database connection."""
    db = await get_database()
    assert db is not None

    # Test basic query
    cursor = await db.execute("SELECT 1")
    result = await cursor.fetchone()
    assert result[0] == 1


@pytest.mark.asyncio
async def test_schema_tables_exist(test_db):
    """Test that all required tables exist."""
    db = await get_database()

    tables = [
        "organization",
        "settings",
        "users",
        "oauth_tokens",
        "client_calendars",
        "calendar_sync_state",
        "event_mappings",
        "busy_blocks",
        "sync_log",
        "webhook_channels",
        "alert_queue",
        "job_locks",
        "main_calendar_sync_state",
    ]

    for table in tables:
        cursor = await db.execute(
            f"SELECT name FROM sqlite_master WHERE type='table' AND name=?",
            (table,)
        )
        result = await cursor.fetchone()
        assert result is not None, f"Table {table} does not exist"


@pytest.mark.asyncio
async def test_oobe_not_completed(test_db):
    """Test OOBE check when not completed."""
    result = await is_oobe_completed()
    assert result is False


@pytest.mark.asyncio
async def test_oobe_completed(test_db):
    """Test OOBE check when completed."""
    db = await get_database()

    # Insert organization
    await db.execute(
        """INSERT INTO organization
           (google_workspace_domain, google_client_id_encrypted, google_client_secret_encrypted)
           VALUES (?, ?, ?)""",
        ("example.com", b"encrypted_id", b"encrypted_secret")
    )
    await db.commit()

    result = await is_oobe_completed()
    assert result is True


@pytest.mark.asyncio
async def test_get_organization_empty(test_db):
    """Test getting organization when empty."""
    result = await get_organization()
    assert result is None


@pytest.mark.asyncio
async def test_get_organization(test_db):
    """Test getting organization."""
    db = await get_database()

    await db.execute(
        """INSERT INTO organization
           (google_workspace_domain, google_client_id_encrypted, google_client_secret_encrypted)
           VALUES (?, ?, ?)""",
        ("example.com", b"encrypted_id", b"encrypted_secret")
    )
    await db.commit()

    result = await get_organization()
    assert result is not None
    assert result["google_workspace_domain"] == "example.com"


@pytest.mark.asyncio
async def test_settings(test_db):
    """Test settings get/set."""
    # Get non-existent setting
    result = await get_setting("test_key")
    assert result is None

    # Set plain text setting
    await set_setting("test_key", "test_value")

    result = await get_setting("test_key")
    assert result is not None
    assert result["value_plain"] == "test_value"
    assert result["is_sensitive"] == 0


@pytest.mark.asyncio
async def test_settings_update(test_db):
    """Test updating a setting."""
    await set_setting("test_key", "initial_value")

    result = await get_setting("test_key")
    assert result["value_plain"] == "initial_value"

    await set_setting("test_key", "updated_value")

    result = await get_setting("test_key")
    assert result["value_plain"] == "updated_value"


@pytest.mark.asyncio
async def test_user_crud(test_db):
    """Test user CRUD operations."""
    db = await get_database()

    # Create user
    cursor = await db.execute(
        """INSERT INTO users (email, google_user_id, display_name, is_admin)
           VALUES (?, ?, ?, ?)
           RETURNING id""",
        ("test@example.com", "google123", "Test User", True)
    )
    row = await cursor.fetchone()
    user_id = row["id"]
    await db.commit()

    # Read user
    cursor = await db.execute("SELECT * FROM users WHERE id = ?", (user_id,))
    user = await cursor.fetchone()

    assert user["email"] == "test@example.com"
    assert user["google_user_id"] == "google123"
    assert user["display_name"] == "Test User"
    assert user["is_admin"] == 1

    # Update user
    await db.execute(
        "UPDATE users SET display_name = ? WHERE id = ?",
        ("Updated Name", user_id)
    )
    await db.commit()

    cursor = await db.execute("SELECT display_name FROM users WHERE id = ?", (user_id,))
    user = await cursor.fetchone()
    assert user["display_name"] == "Updated Name"

    # Delete user
    await db.execute("DELETE FROM users WHERE id = ?", (user_id,))
    await db.commit()

    cursor = await db.execute("SELECT * FROM users WHERE id = ?", (user_id,))
    user = await cursor.fetchone()
    assert user is None


@pytest.mark.asyncio
async def test_foreign_key_cascade(test_db):
    """Test foreign key cascade on delete."""
    db = await get_database()

    # Create user
    cursor = await db.execute(
        """INSERT INTO users (email, google_user_id, display_name)
           VALUES (?, ?, ?)
           RETURNING id""",
        ("test@example.com", "google123", "Test User")
    )
    user_id = (await cursor.fetchone())["id"]

    # Create oauth token
    cursor = await db.execute(
        """INSERT INTO oauth_tokens
           (user_id, account_type, google_account_email,
            access_token_encrypted, refresh_token_encrypted)
           VALUES (?, ?, ?, ?, ?)
           RETURNING id""",
        (user_id, "home", "test@example.com", b"access", b"refresh")
    )
    token_id = (await cursor.fetchone())["id"]
    await db.commit()

    # Verify token exists
    cursor = await db.execute("SELECT * FROM oauth_tokens WHERE id = ?", (token_id,))
    assert await cursor.fetchone() is not None

    # Delete user (should cascade to tokens)
    await db.execute("DELETE FROM users WHERE id = ?", (user_id,))
    await db.commit()

    # Verify token was deleted
    cursor = await db.execute("SELECT * FROM oauth_tokens WHERE id = ?", (token_id,))
    assert await cursor.fetchone() is None
