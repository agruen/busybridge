"""Comprehensive tests for personal calendar sync feature."""

import json
import pytest
import pytest_asyncio
import os
import tempfile
from unittest.mock import MagicMock, AsyncMock, patch, PropertyMock

os.environ["DATABASE_PATH"] = ":memory:"
os.environ["ENCRYPTION_KEY_FILE"] = "/tmp/test_encryption.key"
os.environ["PUBLIC_URL"] = "http://localhost:3000"


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest_asyncio.fixture
async def db():
    """Create an in-memory test database with schema initialized."""
    from app.database import get_database, init_schema, close_database
    import app.database as db_module

    db_module._db_connection = None
    database = await get_database()
    await init_schema(database)
    yield database
    await close_database()
    db_module._db_connection = None


@pytest_asyncio.fixture
async def setup_user(db):
    """Create a test user, org, and encryption key."""
    from app.encryption import generate_encryption_key, get_encryption_manager

    key = generate_encryption_key()
    with tempfile.NamedTemporaryFile(mode="wb", delete=False, suffix=".key") as f:
        f.write(key)
        key_path = f.name
    os.environ["ENCRYPTION_KEY_FILE"] = key_path

    # Reset cached settings so the new key path is picked up
    from app.config import get_settings
    get_settings.cache_clear()

    # Reset encryption manager
    import app.encryption
    app.encryption._manager = None

    enc = get_encryption_manager()

    # Create organization
    await db.execute(
        """INSERT INTO organization
           (google_workspace_domain, google_client_id_encrypted, google_client_secret_encrypted)
           VALUES (?, ?, ?)""",
        ("example.com", enc.encrypt("client-id.apps.googleusercontent.com"), enc.encrypt("secret"))
    )

    # Create user
    await db.execute(
        """INSERT INTO users (email, google_user_id, display_name, main_calendar_id, is_admin)
           VALUES (?, ?, ?, ?, TRUE)""",
        ("user@example.com", "google-user-1", "Test User", "main@example.com")
    )
    await db.commit()

    yield {
        "user_id": 1,
        "email": "user@example.com",
        "main_calendar_id": "main@example.com",
        "key_path": key_path,
    }

    if os.path.exists(key_path):
        os.remove(key_path)


@pytest_asyncio.fixture
async def setup_personal_calendar(db, setup_user):
    """Create a personal calendar connection with token and sync state."""
    from app.encryption import encrypt_value

    # Create OAuth token for personal account
    await db.execute(
        """INSERT INTO oauth_tokens
           (user_id, account_type, google_account_email,
            access_token_encrypted, refresh_token_encrypted, token_expiry)
           VALUES (?, ?, ?, ?, ?, ?)""",
        (1, "personal", "personal@gmail.com",
         encrypt_value("access-token"), encrypt_value("refresh-token"),
         "2030-01-01T00:00:00")
    )
    personal_token_id = 1

    # Create personal calendar entry
    await db.execute(
        """INSERT INTO client_calendars
           (user_id, oauth_token_id, google_calendar_id, display_name, calendar_type, is_active)
           VALUES (?, ?, ?, ?, 'personal', TRUE)""",
        (1, personal_token_id, "personal-cal@gmail.com", "My Personal Calendar")
    )
    personal_cal_id = 1

    # Create calendar sync state
    await db.execute(
        "INSERT INTO calendar_sync_state (client_calendar_id) VALUES (?)",
        (personal_cal_id,)
    )
    await db.commit()

    return {
        "personal_cal_id": personal_cal_id,
        "personal_token_id": personal_token_id,
        "personal_email": "personal@gmail.com",
        "personal_calendar_id": "personal-cal@gmail.com",
    }


@pytest_asyncio.fixture
async def setup_client_calendar(db, setup_user):
    """Create a client calendar connection."""
    from app.encryption import encrypt_value

    await db.execute(
        """INSERT INTO oauth_tokens
           (user_id, account_type, google_account_email,
            access_token_encrypted, refresh_token_encrypted, token_expiry)
           VALUES (?, ?, ?, ?, ?, ?)""",
        (1, "client", "client@company.com",
         encrypt_value("client-access-token"), encrypt_value("client-refresh-token"),
         "2030-01-01T00:00:00")
    )
    client_token_id = 2

    await db.execute(
        """INSERT INTO client_calendars
           (user_id, oauth_token_id, google_calendar_id, display_name, calendar_type, is_active)
           VALUES (?, ?, ?, ?, 'client', TRUE)""",
        (1, client_token_id, "work@company.com", "Company Calendar")
    )
    client_cal_id = 2

    await db.execute(
        "INSERT INTO calendar_sync_state (client_calendar_id) VALUES (?)",
        (client_cal_id,)
    )
    await db.commit()

    return {
        "client_cal_id": client_cal_id,
        "client_token_id": client_token_id,
        "client_email": "client@company.com",
        "client_calendar_id": "work@company.com",
    }


def _make_fake_google_client(created_event_id="new-event-123"):
    """Create a mock GoogleCalendarClient."""
    client = MagicMock()
    client.is_our_event.return_value = False
    client.create_event.return_value = {"id": created_event_id}
    client.update_event.return_value = {"id": created_event_id}
    client.get_event.return_value = {"id": "existing-event", "status": "confirmed"}
    client.delete_event.return_value = None
    client.list_events.return_value = {"events": [], "next_sync_token": "token-123"}
    return client


# ---------------------------------------------------------------------------
# Unit tests for create_personal_busy_block
# ---------------------------------------------------------------------------

class TestCreatePersonalBusyBlock:
    def test_timed_event(self):
        """Personal busy block for a timed event uses distinct title."""
        from app.sync.google_calendar import create_personal_busy_block
        from app.config import get_settings

        start = {"dateTime": "2026-01-15T10:00:00Z"}
        end = {"dateTime": "2026-01-15T11:00:00Z"}

        block = create_personal_busy_block(start, end, is_all_day=False)

        settings = get_settings()
        expected = f"{settings.managed_event_prefix} {settings.personal_busy_block_title}".strip()
        assert block["summary"] == expected
        assert block["transparency"] == "opaque"
        assert block["visibility"] == "private"
        assert block["description"] == ""
        assert "dateTime" in block["start"]
        assert "dateTime" in block["end"]

    def test_all_day_event(self):
        """Personal busy block for all-day event."""
        from app.sync.google_calendar import create_personal_busy_block

        start = {"date": "2026-01-15"}
        end = {"date": "2026-01-16"}

        block = create_personal_busy_block(start, end, is_all_day=True)

        assert "date" in block["start"]
        assert "date" in block["end"]
        assert "dateTime" not in block["start"]

    def test_distinct_from_client_busy_block(self):
        """Personal busy block has different title than client busy block."""
        from app.sync.google_calendar import create_busy_block, create_personal_busy_block

        start = {"dateTime": "2026-01-15T10:00:00Z"}
        end = {"dateTime": "2026-01-15T11:00:00Z"}

        client_block = create_busy_block(start, end)
        personal_block = create_personal_busy_block(start, end)

        assert client_block["summary"] != personal_block["summary"]
        assert "Personal" in personal_block["summary"]

    def test_timezone_preserved(self):
        """Named timezone is passed through."""
        from app.sync.google_calendar import create_personal_busy_block

        start = {"dateTime": "2026-01-05T10:00:00-05:00", "timeZone": "America/New_York"}
        end = {"dateTime": "2026-01-05T11:00:00-05:00", "timeZone": "America/New_York"}

        block = create_personal_busy_block(start, end, is_all_day=False)

        assert block["start"]["timeZone"] == "America/New_York"
        assert block["end"]["timeZone"] == "America/New_York"


# ---------------------------------------------------------------------------
# Unit tests for sync_personal_event_to_all
# ---------------------------------------------------------------------------

class TestSyncPersonalEventToAll:
    @pytest.mark.asyncio
    async def test_skips_our_own_events(self, db, setup_user, setup_personal_calendar):
        """Events created by BusyBridge on the personal calendar are skipped."""
        from app.sync.rules import sync_personal_event_to_all

        personal_client = MagicMock()
        personal_client.is_our_event.return_value = True
        main_client = _make_fake_google_client()

        event = {"id": "e1", "start": {"dateTime": "2026-01-15T10:00:00Z"},
                 "end": {"dateTime": "2026-01-15T11:00:00Z"}}

        result = await sync_personal_event_to_all(
            personal_client=personal_client,
            main_client=main_client,
            event=event,
            user_id=1,
            personal_calendar_id=setup_personal_calendar["personal_cal_id"],
            main_calendar_id="main@example.com",
            user_email="user@example.com",
        )
        assert result is None
        main_client.create_event.assert_not_called()

    @pytest.mark.asyncio
    async def test_skips_cancelled_events(self, db, setup_user, setup_personal_calendar):
        """Cancelled events are skipped."""
        from app.sync.rules import sync_personal_event_to_all

        personal_client = _make_fake_google_client()
        main_client = _make_fake_google_client()

        event = {"id": "e1", "status": "cancelled"}

        result = await sync_personal_event_to_all(
            personal_client=personal_client,
            main_client=main_client,
            event=event,
            user_id=1,
            personal_calendar_id=setup_personal_calendar["personal_cal_id"],
            main_calendar_id="main@example.com",
            user_email="user@example.com",
        )
        assert result is None

    @pytest.mark.asyncio
    async def test_skips_declined_events(self, db, setup_user, setup_personal_calendar):
        """Declined events are skipped."""
        from app.sync.rules import sync_personal_event_to_all

        personal_client = _make_fake_google_client()
        main_client = _make_fake_google_client()

        event = {
            "id": "e1",
            "status": "confirmed",
            "start": {"dateTime": "2026-01-15T10:00:00Z"},
            "end": {"dateTime": "2026-01-15T11:00:00Z"},
            "attendees": [
                {"email": "me@example.com", "self": True, "responseStatus": "declined"}
            ],
        }

        result = await sync_personal_event_to_all(
            personal_client=personal_client,
            main_client=main_client,
            event=event,
            user_id=1,
            personal_calendar_id=setup_personal_calendar["personal_cal_id"],
            main_calendar_id="main@example.com",
            user_email="user@example.com",
        )
        assert result is None

    @pytest.mark.asyncio
    async def test_creates_busy_block_on_main(self, db, setup_user, setup_personal_calendar):
        """Creates a personal busy block on the main calendar."""
        from app.sync.rules import sync_personal_event_to_all

        personal_client = _make_fake_google_client()
        main_client = _make_fake_google_client("main-busy-123")

        event = {
            "id": "personal-event-1",
            "status": "confirmed",
            "start": {"dateTime": "2026-01-15T10:00:00Z"},
            "end": {"dateTime": "2026-01-15T11:00:00Z"},
        }

        result = await sync_personal_event_to_all(
            personal_client=personal_client,
            main_client=main_client,
            event=event,
            user_id=1,
            personal_calendar_id=setup_personal_calendar["personal_cal_id"],
            main_calendar_id="main@example.com",
            user_email="user@example.com",
        )

        assert result == "main-busy-123"
        main_client.create_event.assert_called_once()

        # Check the event data passed to create_event
        call_args = main_client.create_event.call_args
        assert call_args[0][0] == "main@example.com"  # calendar_id
        event_data = call_args[0][1]
        assert "Personal" in event_data["summary"]
        assert event_data["transparency"] == "opaque"

        # Check mapping was created
        cursor = await db.execute(
            "SELECT * FROM event_mappings WHERE user_id = 1 AND origin_type = 'personal'"
        )
        mapping = await cursor.fetchone()
        assert mapping is not None
        assert mapping["origin_event_id"] == "personal-event-1"
        assert mapping["main_event_id"] == "main-busy-123"
        assert mapping["user_can_edit"] == 0  # FALSE

    @pytest.mark.asyncio
    async def test_creates_busy_blocks_on_client_calendars(
        self, db, setup_user, setup_personal_calendar, setup_client_calendar
    ):
        """Creates busy blocks on client calendars for personal events."""
        from app.sync.rules import sync_personal_event_to_all

        personal_client = _make_fake_google_client()
        main_client = _make_fake_google_client("main-busy-456")

        event = {
            "id": "personal-event-2",
            "status": "confirmed",
            "start": {"dateTime": "2026-01-15T14:00:00Z"},
            "end": {"dateTime": "2026-01-15T15:00:00Z"},
        }

        with patch("app.auth.google.get_valid_access_token", new_callable=AsyncMock) as mock_token:
            mock_token.return_value = "fake-client-token"

            with patch("app.sync.rules.GoogleCalendarClient") as MockClient:
                mock_cal_client = _make_fake_google_client("client-busy-789")
                MockClient.return_value = mock_cal_client

                result = await sync_personal_event_to_all(
                    personal_client=personal_client,
                    main_client=main_client,
                    event=event,
                    user_id=1,
                    personal_calendar_id=setup_personal_calendar["personal_cal_id"],
                    main_calendar_id="main@example.com",
                    user_email="user@example.com",
                )

        assert result == "main-busy-456"

        # Check busy block was created on client calendar
        cursor = await db.execute("SELECT * FROM busy_blocks")
        blocks = await cursor.fetchall()
        assert len(blocks) == 1
        assert blocks[0]["client_calendar_id"] == setup_client_calendar["client_cal_id"]
        assert blocks[0]["busy_block_event_id"] == "client-busy-789"

    @pytest.mark.asyncio
    async def test_does_not_create_blocks_on_personal_calendars(
        self, db, setup_user, setup_personal_calendar
    ):
        """Personal calendars are excluded from receiving busy blocks."""
        from app.sync.rules import sync_personal_event_to_all
        from app.encryption import encrypt_value

        # Add a second personal calendar
        await db.execute(
            """INSERT INTO oauth_tokens
               (user_id, account_type, google_account_email,
                access_token_encrypted, refresh_token_encrypted, token_expiry)
               VALUES (?, ?, ?, ?, ?, ?)""",
            (1, "personal", "personal2@gmail.com",
             encrypt_value("token-2"), encrypt_value("refresh-2"),
             "2030-01-01T00:00:00")
        )
        await db.execute(
            """INSERT INTO client_calendars
               (user_id, oauth_token_id, google_calendar_id, display_name, calendar_type, is_active)
               VALUES (?, ?, ?, ?, 'personal', TRUE)""",
            (1, 2, "personal2-cal@gmail.com", "Personal 2")
        )
        await db.commit()

        personal_client = _make_fake_google_client()
        main_client = _make_fake_google_client("main-busy-abc")

        event = {
            "id": "personal-event-3",
            "status": "confirmed",
            "start": {"dateTime": "2026-01-15T10:00:00Z"},
            "end": {"dateTime": "2026-01-15T11:00:00Z"},
        }

        result = await sync_personal_event_to_all(
            personal_client=personal_client,
            main_client=main_client,
            event=event,
            user_id=1,
            personal_calendar_id=setup_personal_calendar["personal_cal_id"],
            main_calendar_id="main@example.com",
            user_email="user@example.com",
        )

        assert result == "main-busy-abc"

        # No busy blocks should be created (no client calendars exist)
        cursor = await db.execute("SELECT * FROM busy_blocks")
        blocks = await cursor.fetchall()
        assert len(blocks) == 0

    @pytest.mark.asyncio
    async def test_updates_existing_mapping(self, db, setup_user, setup_personal_calendar):
        """Updates an existing mapping on re-sync."""
        from app.sync.rules import sync_personal_event_to_all

        # Create initial mapping
        await db.execute(
            """INSERT INTO event_mappings
               (user_id, origin_type, origin_calendar_id, origin_event_id,
                main_event_id, event_start, event_end, is_all_day, is_recurring, user_can_edit)
               VALUES (?, 'personal', ?, ?, ?, ?, ?, ?, ?, FALSE)""",
            (1, setup_personal_calendar["personal_cal_id"], "personal-event-4",
             "old-main-id", "2026-01-15T10:00:00Z", "2026-01-15T11:00:00Z", False, False)
        )
        await db.commit()

        personal_client = _make_fake_google_client()
        main_client = _make_fake_google_client()

        event = {
            "id": "personal-event-4",
            "status": "confirmed",
            "start": {"dateTime": "2026-01-15T10:00:00Z"},
            "end": {"dateTime": "2026-01-15T12:00:00Z"},  # Changed end time
        }

        result = await sync_personal_event_to_all(
            personal_client=personal_client,
            main_client=main_client,
            event=event,
            user_id=1,
            personal_calendar_id=setup_personal_calendar["personal_cal_id"],
            main_calendar_id="main@example.com",
            user_email="user@example.com",
        )

        # Should have updated (not created a new one)
        main_client.update_event.assert_called_once()

        cursor = await db.execute(
            "SELECT * FROM event_mappings WHERE origin_event_id = 'personal-event-4'"
        )
        mapping = await cursor.fetchone()
        assert mapping["event_end"] == "2026-01-15T12:00:00Z"

    @pytest.mark.asyncio
    async def test_recurring_events_copy_recurrence(
        self, db, setup_user, setup_personal_calendar
    ):
        """Recurring event recurrence rules are copied to busy blocks."""
        from app.sync.rules import sync_personal_event_to_all

        personal_client = _make_fake_google_client()
        main_client = _make_fake_google_client("main-recurring-123")

        event = {
            "id": "recurring-personal-1",
            "status": "confirmed",
            "start": {"dateTime": "2026-01-15T10:00:00Z"},
            "end": {"dateTime": "2026-01-15T10:30:00Z"},
            "recurrence": ["RRULE:FREQ=WEEKLY;BYDAY=MO"],
        }

        result = await sync_personal_event_to_all(
            personal_client=personal_client,
            main_client=main_client,
            event=event,
            user_id=1,
            personal_calendar_id=setup_personal_calendar["personal_cal_id"],
            main_calendar_id="main@example.com",
            user_email="user@example.com",
        )

        assert result == "main-recurring-123"

        call_args = main_client.create_event.call_args
        event_data = call_args[0][1]
        assert "recurrence" in event_data
        assert event_data["recurrence"] == ["RRULE:FREQ=WEEKLY;BYDAY=MO"]


# ---------------------------------------------------------------------------
# Unit tests for handle_deleted_personal_event
# ---------------------------------------------------------------------------

class TestHandleDeletedPersonalEvent:
    @pytest.mark.asyncio
    async def test_deletes_main_busy_block(self, db, setup_user, setup_personal_calendar):
        """Deleting a personal event removes the busy block from main calendar."""
        from app.sync.rules import handle_deleted_personal_event

        # Create mapping
        await db.execute(
            """INSERT INTO event_mappings
               (user_id, origin_type, origin_calendar_id, origin_event_id,
                main_event_id, event_start, event_end, is_all_day, is_recurring, user_can_edit)
               VALUES (?, 'personal', ?, ?, ?, ?, ?, ?, ?, FALSE)""",
            (1, setup_personal_calendar["personal_cal_id"], "del-event-1",
             "main-busy-del-1", "2026-01-15T10:00:00Z", "2026-01-15T11:00:00Z", False, False)
        )
        await db.commit()

        main_client = _make_fake_google_client()

        await handle_deleted_personal_event(
            user_id=1,
            personal_calendar_id=setup_personal_calendar["personal_cal_id"],
            event_id="del-event-1",
            main_calendar_id="main@example.com",
            main_client=main_client,
        )

        # Verify main busy block was deleted
        main_client.delete_event.assert_called_once_with("main@example.com", "main-busy-del-1")

        # Verify mapping was cleaned up
        cursor = await db.execute(
            "SELECT * FROM event_mappings WHERE origin_event_id = 'del-event-1'"
        )
        mapping = await cursor.fetchone()
        assert mapping is None

    @pytest.mark.asyncio
    async def test_deletes_client_busy_blocks(
        self, db, setup_user, setup_personal_calendar, setup_client_calendar
    ):
        """Deleting a personal event also removes busy blocks from client calendars."""
        from app.sync.rules import handle_deleted_personal_event

        # Create mapping
        await db.execute(
            """INSERT INTO event_mappings
               (user_id, origin_type, origin_calendar_id, origin_event_id,
                main_event_id, event_start, event_end, is_all_day, is_recurring, user_can_edit)
               VALUES (?, 'personal', ?, ?, ?, ?, ?, ?, ?, FALSE)""",
            (1, setup_personal_calendar["personal_cal_id"], "del-event-2",
             "main-busy-del-2", "2026-01-15T10:00:00Z", "2026-01-15T11:00:00Z", False, False)
        )
        await db.commit()

        # Get mapping id
        cursor = await db.execute(
            "SELECT id FROM event_mappings WHERE origin_event_id = 'del-event-2'"
        )
        mapping = await cursor.fetchone()

        # Create busy block on client calendar
        await db.execute(
            """INSERT INTO busy_blocks (event_mapping_id, client_calendar_id, busy_block_event_id)
               VALUES (?, ?, ?)""",
            (mapping["id"], setup_client_calendar["client_cal_id"], "client-busy-del-2")
        )
        await db.commit()

        main_client = _make_fake_google_client()

        with patch("app.auth.google.get_valid_access_token", new_callable=AsyncMock) as mock_token:
            mock_token.return_value = "fake-token"
            with patch("app.sync.rules.GoogleCalendarClient") as MockClient:
                mock_cal_client = _make_fake_google_client()
                MockClient.return_value = mock_cal_client

                await handle_deleted_personal_event(
                    user_id=1,
                    personal_calendar_id=setup_personal_calendar["personal_cal_id"],
                    event_id="del-event-2",
                    main_calendar_id="main@example.com",
                    main_client=main_client,
                )

        # Verify busy block was deleted
        cursor = await db.execute("SELECT * FROM busy_blocks")
        blocks = await cursor.fetchall()
        assert len(blocks) == 0

        # Verify mapping was cleaned up
        cursor = await db.execute(
            "SELECT * FROM event_mappings WHERE origin_event_id = 'del-event-2'"
        )
        assert await cursor.fetchone() is None

    @pytest.mark.asyncio
    async def test_no_mapping_is_noop(self, db, setup_user, setup_personal_calendar):
        """Deleting a non-tracked event is a no-op."""
        from app.sync.rules import handle_deleted_personal_event

        main_client = _make_fake_google_client()

        await handle_deleted_personal_event(
            user_id=1,
            personal_calendar_id=setup_personal_calendar["personal_cal_id"],
            event_id="nonexistent-event",
            main_calendar_id="main@example.com",
            main_client=main_client,
        )

        main_client.delete_event.assert_not_called()

    @pytest.mark.asyncio
    async def test_recurring_deletion_soft_deletes(
        self, db, setup_user, setup_personal_calendar
    ):
        """Recurring event deletion uses soft-delete (deleted_at)."""
        from app.sync.rules import handle_deleted_personal_event

        await db.execute(
            """INSERT INTO event_mappings
               (user_id, origin_type, origin_calendar_id, origin_event_id,
                main_event_id, event_start, event_end, is_all_day, is_recurring, user_can_edit)
               VALUES (?, 'personal', ?, ?, ?, ?, ?, ?, ?, FALSE)""",
            (1, setup_personal_calendar["personal_cal_id"], "recurring-del-1",
             "main-recurring-del", "2026-01-15T10:00:00Z", "2026-01-15T11:00:00Z", False, True)
        )
        await db.commit()

        main_client = _make_fake_google_client()

        await handle_deleted_personal_event(
            user_id=1,
            personal_calendar_id=setup_personal_calendar["personal_cal_id"],
            event_id="recurring-del-1",
            main_calendar_id="main@example.com",
            main_client=main_client,
        )

        cursor = await db.execute(
            "SELECT * FROM event_mappings WHERE origin_event_id = 'recurring-del-1'"
        )
        mapping = await cursor.fetchone()
        assert mapping is not None
        assert mapping["deleted_at"] is not None


# ---------------------------------------------------------------------------
# Tests for sync_main_event_to_clients excluding personal calendars
# ---------------------------------------------------------------------------

class TestMainSyncExcludesPersonal:
    @pytest.mark.asyncio
    async def test_personal_calendars_excluded_from_busy_blocks(
        self, db, setup_user, setup_personal_calendar, setup_client_calendar
    ):
        """sync_main_event_to_clients should only target client calendars."""
        from app.sync.rules import sync_main_event_to_clients

        main_client = _make_fake_google_client()

        event = {
            "id": "main-event-1",
            "status": "confirmed",
            "start": {"dateTime": "2026-01-15T10:00:00Z"},
            "end": {"dateTime": "2026-01-15T11:00:00Z"},
        }

        with patch("app.auth.google.get_valid_access_token", new_callable=AsyncMock) as mock_token:
            mock_token.return_value = "fake-token"
            with patch("app.sync.rules.GoogleCalendarClient") as MockClient:
                mock_cal = _make_fake_google_client("block-on-client")
                MockClient.return_value = mock_cal

                result = await sync_main_event_to_clients(
                    main_client=main_client,
                    event=event,
                    user_id=1,
                    main_calendar_id="main@example.com",
                    user_email="user@example.com",
                )

        # Only the client calendar should get a busy block (not the personal one)
        cursor = await db.execute("SELECT * FROM busy_blocks")
        blocks = await cursor.fetchall()
        assert len(blocks) == 1
        assert blocks[0]["client_calendar_id"] == setup_client_calendar["client_cal_id"]


# ---------------------------------------------------------------------------
# Tests for database schema
# ---------------------------------------------------------------------------

class TestDatabaseSchema:
    @pytest.mark.asyncio
    async def test_calendar_type_column_exists(self, db):
        """The calendar_type column exists with default 'client'."""
        await db.execute(
            """INSERT INTO users (email, google_user_id, display_name)
               VALUES ('test@test.com', 'gid', 'Test')"""
        )
        await db.execute(
            """INSERT INTO oauth_tokens
               (user_id, account_type, google_account_email,
                access_token_encrypted, refresh_token_encrypted)
               VALUES (1, 'client', 'email@test.com', 'enc-tok', 'enc-ref')"""
        )
        await db.execute(
            """INSERT INTO client_calendars
               (user_id, oauth_token_id, google_calendar_id, display_name)
               VALUES (1, 1, 'cal-id', 'Test Cal')"""
        )
        await db.commit()

        cursor = await db.execute("SELECT calendar_type FROM client_calendars WHERE id = 1")
        row = await cursor.fetchone()
        assert row["calendar_type"] == "client"

    @pytest.mark.asyncio
    async def test_personal_calendar_type(self, db):
        """Can create calendar with type 'personal'."""
        await db.execute(
            """INSERT INTO users (email, google_user_id, display_name)
               VALUES ('test@test.com', 'gid', 'Test')"""
        )
        await db.execute(
            """INSERT INTO oauth_tokens
               (user_id, account_type, google_account_email,
                access_token_encrypted, refresh_token_encrypted)
               VALUES (1, 'personal', 'personal@gmail.com', 'enc-tok', 'enc-ref')"""
        )
        await db.execute(
            """INSERT INTO client_calendars
               (user_id, oauth_token_id, google_calendar_id, display_name, calendar_type)
               VALUES (1, 1, 'personal-cal', 'Personal', 'personal')"""
        )
        await db.commit()

        cursor = await db.execute("SELECT calendar_type FROM client_calendars WHERE id = 1")
        row = await cursor.fetchone()
        assert row["calendar_type"] == "personal"


# ---------------------------------------------------------------------------
# Tests for config
# ---------------------------------------------------------------------------

class TestConfig:
    def test_personal_busy_block_title_default(self):
        """The personal_busy_block_title setting has correct default."""
        from app.config import get_settings
        settings = get_settings()
        assert settings.personal_busy_block_title == "Busy (Personal)"


# ---------------------------------------------------------------------------
# Tests for engine
# ---------------------------------------------------------------------------

class TestSyncEngine:
    @pytest.mark.asyncio
    async def test_trigger_sync_for_user_includes_personal(
        self, db, setup_user, setup_personal_calendar, setup_client_calendar
    ):
        """trigger_sync_for_user syncs both client and personal calendars."""
        from app.sync.engine import trigger_sync_for_user

        with patch("app.sync.engine.trigger_sync_for_calendar", new_callable=AsyncMock) as mock_client_sync:
            with patch("app.sync.engine.trigger_sync_for_personal_calendar", new_callable=AsyncMock) as mock_personal_sync:
                with patch("app.sync.engine.trigger_sync_for_main_calendar", new_callable=AsyncMock) as mock_main_sync:
                    await trigger_sync_for_user(1)

        # Personal calendar should use the personal sync function
        mock_personal_sync.assert_called_once_with(
            setup_personal_calendar["personal_cal_id"]
        )
        # Client calendar should use the client sync function
        mock_client_sync.assert_called_once_with(
            setup_client_calendar["client_cal_id"]
        )
        # Main calendar should also be synced
        mock_main_sync.assert_called_once_with(1)


# ---------------------------------------------------------------------------
# Tests for cleanup
# ---------------------------------------------------------------------------

class TestCleanup:
    @pytest.mark.asyncio
    async def test_cleanup_includes_personal_mappings(self, db, setup_user, setup_personal_calendar):
        """cleanup_managed_events_for_user includes personal-origin mappings."""
        from app.sync.engine import cleanup_managed_events_for_user

        # Create a personal-origin mapping with a main event
        await db.execute(
            """INSERT INTO event_mappings
               (user_id, origin_type, origin_calendar_id, origin_event_id,
                main_event_id, event_start, event_end, is_all_day, is_recurring, user_can_edit)
               VALUES (?, 'personal', ?, ?, ?, ?, ?, ?, ?, FALSE)""",
            (1, setup_personal_calendar["personal_cal_id"], "cleanup-event-1",
             "main-cleanup-1", "2026-01-15T10:00:00Z", "2026-01-15T11:00:00Z", False, False)
        )
        await db.commit()

        with patch("app.auth.google.get_valid_access_token", new_callable=AsyncMock) as mock_token:
            mock_token.return_value = "fake-token"
            with patch("app.sync.engine.GoogleCalendarClient") as MockClient:
                mock_client = _make_fake_google_client()
                mock_client.search_events.return_value = []
                MockClient.return_value = mock_client

                summary = await cleanup_managed_events_for_user(1)

        # The personal mapping should be in the cleanup count
        assert summary["db_client_mappings_checked"] >= 1
        assert summary["db_main_events_deleted"] >= 1


# ---------------------------------------------------------------------------
# Tests for periodic sync job
# ---------------------------------------------------------------------------

class TestPeriodicSync:
    @pytest.mark.asyncio
    async def test_periodic_sync_dispatches_personal(
        self, db, setup_user, setup_personal_calendar, setup_client_calendar
    ):
        """run_periodic_sync uses the correct sync function for each calendar type."""
        from app.jobs.sync_job import run_periodic_sync

        with patch("app.sync.engine.trigger_sync_for_calendar", new_callable=AsyncMock) as mock_client:
            with patch("app.sync.engine.trigger_sync_for_personal_calendar", new_callable=AsyncMock) as mock_personal:
                with patch("app.sync.engine.trigger_sync_for_main_calendar", new_callable=AsyncMock):
                    await run_periodic_sync()

        mock_personal.assert_called_once_with(
            setup_personal_calendar["personal_cal_id"]
        )
        mock_client.assert_called_once_with(
            setup_client_calendar["client_cal_id"]
        )
