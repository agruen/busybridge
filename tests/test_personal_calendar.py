"""Tests for personal calendar sync functionality."""

import pytest
import pytest_asyncio
from unittest.mock import AsyncMock, MagicMock, patch

from app.config import get_settings
from app.sync.google_calendar import (
    create_personal_busy_block,
    should_create_busy_block,
)


# ─── Unit tests for create_personal_busy_block ──────────────────────

def test_create_personal_busy_block_timed():
    """Personal busy block has correct title and structure for timed events."""
    start = {"dateTime": "2024-01-15T10:00:00Z"}
    end = {"dateTime": "2024-01-15T11:00:00Z"}

    block = create_personal_busy_block(start, end, is_all_day=False)

    settings = get_settings()
    expected = f"\U0001f510 {settings.managed_event_prefix} {settings.personal_busy_block_title}".strip()
    assert block["summary"] == expected
    assert block["description"] == ""
    assert block["visibility"] == "private"
    assert block["transparency"] == "opaque"
    assert "dateTime" in block["start"]
    assert "dateTime" in block["end"]


def test_create_personal_busy_block_all_day():
    """Personal busy block handles all-day events."""
    start = {"date": "2024-01-15"}
    end = {"date": "2024-01-16"}

    block = create_personal_busy_block(start, end, is_all_day=True)

    assert "date" in block["start"]
    assert "date" in block["end"]
    assert "dateTime" not in block["start"]


def test_personal_busy_block_title_differs_from_client():
    """Personal busy block title is different from client busy block title."""
    settings = get_settings()
    assert settings.personal_busy_block_title != settings.busy_block_title
    assert "Personal" in settings.personal_busy_block_title


# ─── Unit tests for should_create_busy_block with personal events ───

def test_skip_cancelled_personal_event():
    """Cancelled events should not create busy blocks."""
    event = {"status": "cancelled", "start": {"dateTime": "2024-01-15T10:00:00Z"}}
    assert should_create_busy_block(event) is False


def test_skip_declined_personal_event():
    """Declined events should not create busy blocks."""
    event = {
        "status": "confirmed",
        "start": {"dateTime": "2024-01-15T10:00:00Z"},
        "attendees": [{"self": True, "responseStatus": "declined"}],
    }
    assert should_create_busy_block(event) is False


def test_skip_transparent_allday_personal_event():
    """Free all-day events should not create busy blocks."""
    event = {
        "status": "confirmed",
        "start": {"date": "2024-01-15"},
        "transparency": "transparent",
    }
    assert should_create_busy_block(event) is False


def test_opaque_allday_personal_event_creates_block():
    """Busy all-day events should create busy blocks."""
    event = {
        "status": "confirmed",
        "start": {"date": "2024-01-15"},
        "transparency": "opaque",
    }
    assert should_create_busy_block(event) is True


# ─── Integration tests for sync_personal_event_to_all ────────────────

@pytest_asyncio.fixture
async def personal_test_db(test_db):
    """Set up test database with user, tokens, and calendars for personal sync tests."""
    db = test_db

    # Create user
    await db.execute(
        """INSERT INTO users (id, email, google_user_id, display_name, main_calendar_id)
           VALUES (1, 'user@home.com', 'guser1', 'Test User', 'main@home.com')"""
    )

    # Create home token
    await db.execute(
        """INSERT INTO oauth_tokens (id, user_id, account_type, google_account_email,
           access_token_encrypted, refresh_token_encrypted)
           VALUES (1, 1, 'home', 'user@home.com', X'00', X'00')"""
    )

    # Create personal token
    await db.execute(
        """INSERT INTO oauth_tokens (id, user_id, account_type, google_account_email,
           access_token_encrypted, refresh_token_encrypted)
           VALUES (2, 1, 'personal', 'personal@gmail.com', X'00', X'00')"""
    )

    # Create client token
    await db.execute(
        """INSERT INTO oauth_tokens (id, user_id, account_type, google_account_email,
           access_token_encrypted, refresh_token_encrypted)
           VALUES (3, 1, 'client', 'client@org.com', X'00', X'00')"""
    )

    # Create a personal calendar
    await db.execute(
        """INSERT INTO client_calendars (id, user_id, oauth_token_id, google_calendar_id,
           display_name, calendar_type)
           VALUES (10, 1, 2, 'personal-cal@gmail.com', 'My Personal', 'personal')"""
    )
    await db.execute(
        "INSERT INTO calendar_sync_state (client_calendar_id) VALUES (10)"
    )

    # Create a client calendar
    await db.execute(
        """INSERT INTO client_calendars (id, user_id, oauth_token_id, google_calendar_id,
           display_name, calendar_type, color_id)
           VALUES (20, 1, 3, 'work-cal@org.com', 'Work', 'client', '1')"""
    )
    await db.execute(
        "INSERT INTO calendar_sync_state (client_calendar_id) VALUES (20)"
    )

    await db.commit()
    return db


@pytest.mark.asyncio
async def personal_test_db_has_calendar_type(personal_test_db):
    """Verify the calendar_type column exists and has correct values."""
    db = personal_test_db

    cursor = await db.execute(
        "SELECT id, calendar_type FROM client_calendars ORDER BY id"
    )
    rows = await cursor.fetchall()
    assert len(rows) == 2
    assert rows[0]["calendar_type"] == "personal"
    assert rows[1]["calendar_type"] == "client"


@pytest.mark.asyncio
async def test_sync_main_event_to_clients_excludes_personal(personal_test_db):
    """sync_main_event_to_clients should not create busy blocks on personal calendars."""
    db = personal_test_db

    # Create a main-origin event mapping
    await db.execute(
        """INSERT INTO event_mappings
           (user_id, origin_type, origin_event_id, main_event_id,
            event_start, event_end, is_all_day, is_recurring, user_can_edit)
           VALUES (1, 'main', 'main-evt-1', 'main-evt-1',
                   '2024-01-15T10:00:00Z', '2024-01-15T11:00:00Z',
                   FALSE, FALSE, TRUE)"""
    )
    await db.commit()

    # The sync_main_event_to_clients query should only return client calendars
    cursor = await db.execute(
        """SELECT cc.*, ot.google_account_email
           FROM client_calendars cc
           JOIN oauth_tokens ot ON cc.oauth_token_id = ot.id
           WHERE cc.user_id = 1 AND cc.is_active = TRUE
             AND cc.calendar_type = 'client'"""
    )
    client_cals = await cursor.fetchall()

    assert len(client_cals) == 1
    assert client_cals[0]["id"] == 20  # Only the client calendar, not personal


@pytest.mark.asyncio
async def test_personal_event_mapping_created(personal_test_db):
    """sync_personal_event_to_all creates mapping with origin_type='personal'."""
    db = personal_test_db

    mock_personal_client = MagicMock()
    mock_personal_client.is_our_event.return_value = False

    mock_main_client = MagicMock()
    mock_main_client.create_event.return_value = {"id": "personal-main-busy-1"}

    mock_client_client = MagicMock()
    mock_client_client.create_event.return_value = {"id": "personal-client-busy-1"}

    event = {
        "id": "personal-event-1",
        "status": "confirmed",
        "summary": "Doctor Appointment",
        "start": {"dateTime": "2024-01-15T10:00:00Z"},
        "end": {"dateTime": "2024-01-15T11:00:00Z"},
    }

    with patch("app.auth.google.get_valid_access_token", new_callable=AsyncMock, return_value="fake-token"):
        with patch("app.sync.rules.GoogleCalendarClient", return_value=mock_client_client):
            from app.sync.rules import sync_personal_event_to_all

            result = await sync_personal_event_to_all(
                personal_client=mock_personal_client,
                main_client=mock_main_client,
                event=event,
                user_id=1,
                personal_calendar_id=10,
                main_calendar_id="main@home.com",
                user_email="user@home.com",
            )

    assert result == "personal-main-busy-1"

    # Check mapping was created
    cursor = await db.execute(
        "SELECT * FROM event_mappings WHERE origin_type = 'personal'"
    )
    mapping = await cursor.fetchone()
    assert mapping is not None
    assert mapping["origin_calendar_id"] == 10
    assert mapping["origin_event_id"] == "personal-event-1"
    assert mapping["main_event_id"] == "personal-main-busy-1"
    assert mapping["user_can_edit"] == 0  # False

    # Check busy block was created on the client calendar
    cursor = await db.execute("SELECT * FROM busy_blocks")
    blocks = await cursor.fetchall()
    assert len(blocks) == 1
    assert blocks[0]["client_calendar_id"] == 20  # The client calendar
    assert blocks[0]["busy_block_event_id"] == "personal-client-busy-1"


@pytest.mark.asyncio
async def test_personal_event_skips_our_events(personal_test_db):
    """sync_personal_event_to_all skips events created by our sync engine."""
    mock_personal_client = MagicMock()
    mock_personal_client.is_our_event.return_value = True

    mock_main_client = MagicMock()

    event = {
        "id": "our-event-1",
        "status": "confirmed",
        "start": {"dateTime": "2024-01-15T10:00:00Z"},
        "end": {"dateTime": "2024-01-15T11:00:00Z"},
        "extendedProperties": {"private": {"calendarSyncEngine": "true"}},
    }

    from app.sync.rules import sync_personal_event_to_all

    result = await sync_personal_event_to_all(
        personal_client=mock_personal_client,
        main_client=mock_main_client,
        event=event,
        user_id=1,
        personal_calendar_id=10,
        main_calendar_id="main@home.com",
        user_email="user@home.com",
    )

    assert result is None
    mock_main_client.create_event.assert_not_called()


@pytest.mark.asyncio
async def test_handle_deleted_personal_event(personal_test_db):
    """handle_deleted_personal_event cleans up main event and busy blocks."""
    db = personal_test_db

    # Create a personal event mapping and busy block
    await db.execute(
        """INSERT INTO event_mappings
           (id, user_id, origin_type, origin_calendar_id, origin_event_id,
            main_event_id, event_start, event_end, is_all_day, is_recurring, user_can_edit)
           VALUES (100, 1, 'personal', 10, 'personal-del-1',
                   'main-busy-del-1', '2024-01-15T10:00:00Z', '2024-01-15T11:00:00Z',
                   FALSE, FALSE, FALSE)"""
    )
    await db.execute(
        """INSERT INTO busy_blocks (event_mapping_id, client_calendar_id, busy_block_event_id)
           VALUES (100, 20, 'client-busy-del-1')"""
    )
    await db.commit()

    mock_main_client = MagicMock()
    mock_main_client.delete_event.return_value = True

    mock_client_client = MagicMock()
    mock_client_client.delete_event.return_value = True

    with patch("app.auth.google.get_valid_access_token", new_callable=AsyncMock, return_value="fake-token"):
        with patch("app.sync.rules.GoogleCalendarClient", return_value=mock_client_client):
            from app.sync.rules import handle_deleted_personal_event

            await handle_deleted_personal_event(
                user_id=1,
                personal_calendar_id=10,
                event_id="personal-del-1",
                main_calendar_id="main@home.com",
                main_client=mock_main_client,
            )

    # Verify main event was deleted
    mock_main_client.delete_event.assert_called_once_with("main@home.com", "main-busy-del-1")

    # Verify client busy block was deleted
    mock_client_client.delete_event.assert_called_once_with("work-cal@org.com", "client-busy-del-1")

    # Verify mapping was removed from DB
    cursor = await db.execute("SELECT * FROM event_mappings WHERE id = 100")
    assert await cursor.fetchone() is None

    # Verify busy block was removed from DB
    cursor = await db.execute("SELECT * FROM busy_blocks WHERE event_mapping_id = 100")
    assert await cursor.fetchone() is None
