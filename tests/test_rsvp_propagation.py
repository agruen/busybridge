"""Tests for RSVP propagation feature.

Covers:
- copy_event_for_main: attendee injection with main_email / current_rsvp_status
- sync_client_event_to_main: rsvp_status DB tracking on insert and update
- propagate_rsvp_to_client: patching client event and persisting new status
- _sync_main_calendar: RSVP detection and propagation dispatch
"""

from __future__ import annotations

import json

import pytest

from app.database import get_database
from app.sync.google_calendar import copy_event_for_main


# ---------------------------------------------------------------------------
# Helpers (same pattern as other test files)
# ---------------------------------------------------------------------------

async def _insert_user(
    email: str = "user@example.com",
    google_user_id: str = "google-user-1",
) -> int:
    db = await get_database()
    cursor = await db.execute(
        """INSERT INTO users (email, google_user_id, display_name, main_calendar_id)
           VALUES (?, ?, ?, ?)
           RETURNING id""",
        (email, google_user_id, "User", "main-cal"),
    )
    row = await cursor.fetchone()
    await db.commit()
    return row["id"]


async def _insert_token(user_id: int, email: str, account_type: str = "client") -> int:
    db = await get_database()
    cursor = await db.execute(
        """INSERT INTO oauth_tokens
           (user_id, account_type, google_account_email, access_token_encrypted, refresh_token_encrypted)
           VALUES (?, ?, ?, ?, ?)
           RETURNING id""",
        (user_id, account_type, email, b"a", b"r"),
    )
    row = await cursor.fetchone()
    await db.commit()
    return row["id"]


async def _insert_calendar(user_id: int, token_id: int, calendar_id: str) -> int:
    db = await get_database()
    cursor = await db.execute(
        """INSERT INTO client_calendars
           (user_id, oauth_token_id, google_calendar_id, display_name, is_active)
           VALUES (?, ?, ?, ?, TRUE)
           RETURNING id""",
        (user_id, token_id, calendar_id, calendar_id),
    )
    row = await cursor.fetchone()
    await db.commit()
    return row["id"]


# ---------------------------------------------------------------------------
# copy_event_for_main — attendee injection
# ---------------------------------------------------------------------------

class TestCopyEventForMainAttendees:
    """Tests for the attendee injection logic in copy_event_for_main."""

    def _source_event_with_attendees(self) -> dict:
        return {
            "summary": "Team Standup",
            "description": "Daily check-in",
            "start": {"dateTime": "2026-03-01T10:00:00Z"},
            "end": {"dateTime": "2026-03-01T10:30:00Z"},
            "attendees": [
                {"email": "organizer@client.com", "responseStatus": "accepted"},
                {"email": "colleague@client.com", "responseStatus": "needsAction"},
            ],
        }

    def test_injects_attendee_when_main_email_provided(self):
        """When source has attendees and main_email is given, inject main_email as sole attendee."""
        source = self._source_event_with_attendees()
        result = copy_event_for_main(source, main_email="main@home.com")

        assert "attendees" in result
        assert len(result["attendees"]) == 1
        assert result["attendees"][0]["email"] == "main@home.com"
        assert result["attendees"][0]["responseStatus"] == "needsAction"
        assert result["attendees"][0]["self"] is True

    def test_uses_current_rsvp_status_on_update(self):
        """On update, current_rsvp_status from DB should be preserved, not reset to needsAction."""
        source = self._source_event_with_attendees()
        result = copy_event_for_main(
            source, main_email="main@home.com", current_rsvp_status="accepted"
        )

        assert result["attendees"][0]["responseStatus"] == "accepted"

    def test_no_attendees_without_main_email(self):
        """Without main_email, no attendees should be injected even if source has them."""
        source = self._source_event_with_attendees()
        result = copy_event_for_main(source)

        assert "attendees" not in result

    def test_no_attendees_when_source_has_none(self):
        """When source event has no attendees, no injection even with main_email."""
        source = {
            "summary": "Solo Event",
            "start": {"dateTime": "2026-03-01T10:00:00Z"},
            "end": {"dateTime": "2026-03-01T10:30:00Z"},
        }
        result = copy_event_for_main(source, main_email="main@home.com")

        assert "attendees" not in result

    def test_tentative_rsvp_status_preserved(self):
        """Tentative status should be preserved on update."""
        source = self._source_event_with_attendees()
        result = copy_event_for_main(
            source, main_email="main@home.com", current_rsvp_status="tentative"
        )

        assert result["attendees"][0]["responseStatus"] == "tentative"


# ---------------------------------------------------------------------------
# sync_client_event_to_main — rsvp_status DB tracking
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_new_invite_sets_rsvp_status_needs_action(test_db):
    """New invite event with attendees should set rsvp_status='needsAction' in DB."""
    from app.sync.rules import sync_client_event_to_main

    user_id = await _insert_user(email="rsvp-new@example.com", google_user_id="rsvp-new-g")
    token_id = await _insert_token(user_id, "client-rsvp@example.com")
    cal_id = await _insert_calendar(user_id, token_id, "client-rsvp-cal")

    class FakeMainClient:
        def create_event(self, _cal, _data, **kwargs):
            return {"id": "main-rsvp-1"}

    client = type("C", (), {"is_our_event": lambda self, _: False})()

    event = {
        "id": "invite-1",
        "summary": "Team Meeting",
        "start": {"dateTime": "2026-03-01T10:00:00Z"},
        "end": {"dateTime": "2026-03-01T11:00:00Z"},
        "organizer": {"email": "organizer@client.com"},
        "attendees": [
            {"email": "organizer@client.com", "responseStatus": "accepted"},
            {"email": "client-rsvp@example.com", "responseStatus": "needsAction"},
        ],
    }

    await sync_client_event_to_main(
        client=client,
        main_client=FakeMainClient(),
        event=event,
        user_id=user_id,
        client_calendar_id=cal_id,
        main_calendar_id="main-cal",
        client_email="client-rsvp@example.com",
        main_email="rsvp-new@example.com",
    )

    db = await get_database()
    cursor = await db.execute(
        "SELECT rsvp_status FROM event_mappings WHERE user_id = ? AND origin_event_id = ?",
        (user_id, "invite-1"),
    )
    row = await cursor.fetchone()
    assert row["rsvp_status"] == "needsAction"


@pytest.mark.asyncio
async def test_new_non_invite_sets_rsvp_status_null(test_db):
    """Non-invite event (no attendees) should set rsvp_status=NULL."""
    from app.sync.rules import sync_client_event_to_main

    user_id = await _insert_user(email="rsvp-none@example.com", google_user_id="rsvp-none-g")
    token_id = await _insert_token(user_id, "client-no-invite@example.com")
    cal_id = await _insert_calendar(user_id, token_id, "client-no-invite-cal")

    class FakeMainClient:
        def create_event(self, _cal, _data, **kwargs):
            return {"id": "main-no-rsvp-1"}

    client = type("C", (), {"is_our_event": lambda self, _: False})()

    event = {
        "id": "solo-1",
        "summary": "Focus Time",
        "start": {"dateTime": "2026-03-01T14:00:00Z"},
        "end": {"dateTime": "2026-03-01T15:00:00Z"},
        "organizer": {"email": "client-no-invite@example.com"},
    }

    await sync_client_event_to_main(
        client=client,
        main_client=FakeMainClient(),
        event=event,
        user_id=user_id,
        client_calendar_id=cal_id,
        main_calendar_id="main-cal",
        client_email="client-no-invite@example.com",
        main_email="rsvp-none@example.com",
    )

    db = await get_database()
    cursor = await db.execute(
        "SELECT rsvp_status FROM event_mappings WHERE user_id = ? AND origin_event_id = ?",
        (user_id, "solo-1"),
    )
    row = await cursor.fetchone()
    assert row["rsvp_status"] is None


@pytest.mark.asyncio
async def test_update_preserves_existing_rsvp_status(test_db):
    """Updating an existing invite should NOT overwrite rsvp_status if already set."""
    from app.sync.rules import sync_client_event_to_main

    user_id = await _insert_user(email="rsvp-upd@example.com", google_user_id="rsvp-upd-g")
    token_id = await _insert_token(user_id, "client-upd@example.com")
    cal_id = await _insert_calendar(user_id, token_id, "client-upd-cal")
    db = await get_database()

    # Pre-insert a mapping with rsvp_status = "accepted"
    await db.execute(
        """INSERT INTO event_mappings
           (user_id, origin_type, origin_calendar_id, origin_event_id,
            main_event_id, event_start, event_end, is_all_day, is_recurring,
            user_can_edit, rsvp_status)
           VALUES (?, 'client', ?, 'invite-upd', 'main-upd', '2026-03-01T10:00:00Z',
                   '2026-03-01T11:00:00Z', FALSE, FALSE, TRUE, 'accepted')""",
        (user_id, cal_id),
    )
    await db.commit()

    class FakeMainClient:
        def update_event(self, _cal, _eid, _data, **kwargs):
            return {"id": "main-upd"}

    client = type("C", (), {"is_our_event": lambda self, _: False})()

    event = {
        "id": "invite-upd",
        "summary": "Team Meeting (updated)",
        "start": {"dateTime": "2026-03-01T10:00:00Z"},
        "end": {"dateTime": "2026-03-01T11:30:00Z"},
        "organizer": {"email": "org@client.com"},
        "attendees": [
            {"email": "org@client.com", "responseStatus": "accepted"},
            {"email": "client-upd@example.com", "responseStatus": "needsAction"},
        ],
    }

    await sync_client_event_to_main(
        client=client,
        main_client=FakeMainClient(),
        event=event,
        user_id=user_id,
        client_calendar_id=cal_id,
        main_calendar_id="main-cal",
        client_email="client-upd@example.com",
        main_email="rsvp-upd@example.com",
    )

    cursor = await db.execute(
        "SELECT rsvp_status FROM event_mappings WHERE user_id = ? AND origin_event_id = ?",
        (user_id, "invite-upd"),
    )
    row = await cursor.fetchone()
    assert row["rsvp_status"] == "accepted", "Existing RSVP status should not be overwritten"


@pytest.mark.asyncio
async def test_update_promotes_null_to_needs_action_when_attendees_added(test_db):
    """If event transitions from non-invite to invite, rsvp_status should go from NULL to needsAction."""
    from app.sync.rules import sync_client_event_to_main

    user_id = await _insert_user(email="rsvp-promo@example.com", google_user_id="rsvp-promo-g")
    token_id = await _insert_token(user_id, "client-promo@example.com")
    cal_id = await _insert_calendar(user_id, token_id, "client-promo-cal")
    db = await get_database()

    # Pre-insert a mapping with rsvp_status = NULL (was not an invite)
    await db.execute(
        """INSERT INTO event_mappings
           (user_id, origin_type, origin_calendar_id, origin_event_id,
            main_event_id, event_start, event_end, is_all_day, is_recurring,
            user_can_edit, rsvp_status)
           VALUES (?, 'client', ?, 'solo-promo', 'main-promo', '2026-03-01T14:00:00Z',
                   '2026-03-01T15:00:00Z', FALSE, FALSE, TRUE, NULL)""",
        (user_id, cal_id),
    )
    await db.commit()

    class FakeMainClient:
        def update_event(self, _cal, _eid, _data, **kwargs):
            return {"id": "main-promo"}

    client = type("C", (), {"is_our_event": lambda self, _: False})()

    # Event now has attendees (turned into an invite)
    event = {
        "id": "solo-promo",
        "summary": "Focus Time (now a meeting)",
        "start": {"dateTime": "2026-03-01T14:00:00Z"},
        "end": {"dateTime": "2026-03-01T15:00:00Z"},
        "organizer": {"email": "org@client.com"},
        "attendees": [
            {"email": "org@client.com", "responseStatus": "accepted"},
            {"email": "client-promo@example.com", "responseStatus": "needsAction"},
        ],
    }

    await sync_client_event_to_main(
        client=client,
        main_client=FakeMainClient(),
        event=event,
        user_id=user_id,
        client_calendar_id=cal_id,
        main_calendar_id="main-cal",
        client_email="client-promo@example.com",
        main_email="rsvp-promo@example.com",
    )

    cursor = await db.execute(
        "SELECT rsvp_status FROM event_mappings WHERE user_id = ? AND origin_event_id = ?",
        (user_id, "solo-promo"),
    )
    row = await cursor.fetchone()
    assert row["rsvp_status"] == "needsAction"


# ---------------------------------------------------------------------------
# propagate_rsvp_to_client
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_propagate_rsvp_patches_client_and_updates_db(test_db, monkeypatch):
    """propagate_rsvp_to_client should patch the client event and update DB status."""
    from app.sync.rules import propagate_rsvp_to_client

    user_id = await _insert_user(email="prop@example.com", google_user_id="prop-g")
    token_id = await _insert_token(user_id, "prop-client@example.com")
    cal_id = await _insert_calendar(user_id, token_id, "prop-client-cal")
    db = await get_database()

    cursor = await db.execute(
        """INSERT INTO event_mappings
           (user_id, origin_type, origin_calendar_id, origin_event_id,
            main_event_id, event_start, event_end, is_all_day, is_recurring,
            user_can_edit, rsvp_status)
           VALUES (?, 'client', ?, 'client-evt-1', 'main-evt-1', '2026-03-01T10:00:00Z',
                   '2026-03-01T11:00:00Z', FALSE, FALSE, TRUE, 'needsAction')
           RETURNING id""",
        (user_id, cal_id),
    )
    mapping_row = await cursor.fetchone()
    mapping_id = mapping_row["id"]
    await db.commit()

    # Read the full mapping
    cursor = await db.execute("SELECT * FROM event_mappings WHERE id = ?", (mapping_id,))
    mapping = dict(await cursor.fetchone())

    patched_calls = []

    async def fake_get_valid_access_token(_uid, _email):
        return "token"

    class FakeClient:
        def __init__(self, _token):
            pass

        def patch_event(self, calendar_id, event_id, body, **kwargs):
            patched_calls.append((calendar_id, event_id, body, kwargs))
            return {"id": event_id}

    monkeypatch.setattr("app.auth.google.get_valid_access_token", fake_get_valid_access_token)
    monkeypatch.setattr("app.sync.rules.GoogleCalendarClient", FakeClient)

    result = await propagate_rsvp_to_client(
        user_id=user_id,
        main_event={"id": "main-evt-1"},
        mapping=mapping,
        new_rsvp_status="accepted",
    )

    assert result is True
    assert len(patched_calls) == 1
    cal_id_arg, evt_id_arg, body_arg, kwargs_arg = patched_calls[0]
    assert cal_id_arg == "prop-client-cal"
    assert evt_id_arg == "client-evt-1"
    assert body_arg["attendees"][0]["email"] == "prop-client@example.com"
    assert body_arg["attendees"][0]["responseStatus"] == "accepted"
    assert kwargs_arg.get("send_updates") == "none"

    # Verify DB was updated
    cursor = await db.execute(
        "SELECT rsvp_status FROM event_mappings WHERE id = ?", (mapping_id,)
    )
    row = await cursor.fetchone()
    assert row["rsvp_status"] == "accepted"


@pytest.mark.asyncio
async def test_propagate_rsvp_returns_false_on_missing_calendar(test_db):
    """propagate_rsvp_to_client should return False when client calendar is not found."""
    from app.sync.rules import propagate_rsvp_to_client

    user_id = await _insert_user(email="prop-miss@example.com", google_user_id="prop-miss-g")

    # Mapping references a non-existent calendar
    mapping = {
        "id": 9999,
        "origin_calendar_id": 9999,
        "origin_event_id": "missing-evt",
        "rsvp_status": "needsAction",
    }

    result = await propagate_rsvp_to_client(
        user_id=user_id,
        main_event={"id": "main-missing"},
        mapping=mapping,
        new_rsvp_status="accepted",
    )

    assert result is False


@pytest.mark.asyncio
async def test_propagate_rsvp_returns_false_on_patch_error(test_db, monkeypatch):
    """propagate_rsvp_to_client should return False and not update DB on patch failure."""
    from app.sync.rules import propagate_rsvp_to_client

    user_id = await _insert_user(email="prop-err@example.com", google_user_id="prop-err-g")
    token_id = await _insert_token(user_id, "prop-err-client@example.com")
    cal_id = await _insert_calendar(user_id, token_id, "prop-err-cal")
    db = await get_database()

    cursor = await db.execute(
        """INSERT INTO event_mappings
           (user_id, origin_type, origin_calendar_id, origin_event_id,
            main_event_id, event_start, event_end, is_all_day, is_recurring,
            user_can_edit, rsvp_status)
           VALUES (?, 'client', ?, 'err-evt', 'main-err-evt', '2026-03-01T10:00:00Z',
                   '2026-03-01T11:00:00Z', FALSE, FALSE, TRUE, 'needsAction')
           RETURNING id""",
        (user_id, cal_id),
    )
    mapping_id = (await cursor.fetchone())["id"]
    await db.commit()

    cursor = await db.execute("SELECT * FROM event_mappings WHERE id = ?", (mapping_id,))
    mapping = dict(await cursor.fetchone())

    async def fake_get_valid_access_token(_uid, _email):
        return "token"

    class FailingClient:
        def __init__(self, _token):
            pass

        def patch_event(self, *args, **kwargs):
            raise RuntimeError("API error")

    monkeypatch.setattr("app.auth.google.get_valid_access_token", fake_get_valid_access_token)
    monkeypatch.setattr("app.sync.rules.GoogleCalendarClient", FailingClient)

    result = await propagate_rsvp_to_client(
        user_id=user_id,
        main_event={"id": "main-err-evt"},
        mapping=mapping,
        new_rsvp_status="accepted",
    )

    assert result is False

    # DB should still show old status
    cursor = await db.execute(
        "SELECT rsvp_status FROM event_mappings WHERE id = ?", (mapping_id,)
    )
    row = await cursor.fetchone()
    assert row["rsvp_status"] == "needsAction"


# ---------------------------------------------------------------------------
# Engine: RSVP detection in _sync_main_calendar
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_main_calendar_sync_detects_rsvp_change(test_db, monkeypatch):
    """_sync_main_calendar should detect RSVP change and call propagate_rsvp_to_client."""
    from app.sync.engine import trigger_sync_for_main_calendar

    user_id = await _insert_user(
        email="engine-rsvp@example.com", google_user_id="engine-rsvp-g"
    )
    await _insert_token(user_id, "engine-rsvp@example.com", account_type="home")
    token_id = await _insert_token(user_id, "engine-client@example.com")
    cal_id = await _insert_calendar(user_id, token_id, "engine-client-cal")
    db = await get_database()

    # Insert a mapping with rsvp_status = "needsAction"
    await db.execute(
        """INSERT INTO event_mappings
           (user_id, origin_type, origin_calendar_id, origin_event_id,
            main_event_id, event_start, event_end, is_all_day, is_recurring,
            user_can_edit, rsvp_status)
           VALUES (?, 'client', ?, 'client-evt-rsvp', 'main-evt-rsvp',
                   '2026-03-01T10:00:00Z', '2026-03-01T11:00:00Z',
                   FALSE, FALSE, TRUE, 'needsAction')""",
        (user_id, cal_id),
    )
    await db.commit()

    async def fake_get_valid_access_token(_uid, _email):
        return "token"

    class FakeGoogleCalendarClient:
        def __init__(self, _token):
            pass

        def list_events(self, _cal, sync_token=None):
            return {
                "events": [{
                    "id": "main-evt-rsvp",
                    "status": "confirmed",
                    "start": {"dateTime": "2026-03-01T10:00:00Z"},
                    "end": {"dateTime": "2026-03-01T11:00:00Z"},
                    "attendees": [
                        {"email": "engine-rsvp@example.com", "self": True, "responseStatus": "accepted"},
                    ],
                }],
                "next_sync_token": "new-token",
            }

    propagated_calls = []

    async def fake_propagate_rsvp_to_client(**kwargs):
        propagated_calls.append(kwargs)
        return True

    synced_to_clients = []

    async def fake_sync_main_event_to_clients(**kwargs):
        synced_to_clients.append(kwargs["event"]["id"])
        return []

    monkeypatch.setattr("app.auth.google.get_valid_access_token", fake_get_valid_access_token)
    monkeypatch.setattr("app.sync.engine.GoogleCalendarClient", FakeGoogleCalendarClient)
    monkeypatch.setattr("app.sync.engine.propagate_rsvp_to_client", fake_propagate_rsvp_to_client)
    monkeypatch.setattr("app.sync.engine.sync_main_event_to_clients", fake_sync_main_event_to_clients)

    await trigger_sync_for_main_calendar(user_id)

    # RSVP should have been detected and propagated
    assert len(propagated_calls) == 1
    assert propagated_calls[0]["new_rsvp_status"] == "accepted"
    assert propagated_calls[0]["user_id"] == user_id

    # sync_main_event_to_clients should still be called afterward
    assert synced_to_clients == ["main-evt-rsvp"]


@pytest.mark.asyncio
async def test_main_calendar_sync_skips_unchanged_rsvp(test_db, monkeypatch):
    """_sync_main_calendar should NOT call propagate when RSVP hasn't changed."""
    from app.sync.engine import trigger_sync_for_main_calendar

    user_id = await _insert_user(
        email="engine-nochange@example.com", google_user_id="engine-nochange-g"
    )
    await _insert_token(user_id, "engine-nochange@example.com", account_type="home")
    token_id = await _insert_token(user_id, "engine-nc-client@example.com")
    cal_id = await _insert_calendar(user_id, token_id, "engine-nc-client-cal")
    db = await get_database()

    # Mapping already says "accepted"
    await db.execute(
        """INSERT INTO event_mappings
           (user_id, origin_type, origin_calendar_id, origin_event_id,
            main_event_id, event_start, event_end, is_all_day, is_recurring,
            user_can_edit, rsvp_status)
           VALUES (?, 'client', ?, 'nc-client-evt', 'nc-main-evt',
                   '2026-03-01T10:00:00Z', '2026-03-01T11:00:00Z',
                   FALSE, FALSE, TRUE, 'accepted')""",
        (user_id, cal_id),
    )
    await db.commit()

    async def fake_get_valid_access_token(_uid, _email):
        return "token"

    class FakeGoogleCalendarClient:
        def __init__(self, _token):
            pass

        def list_events(self, _cal, sync_token=None):
            return {
                "events": [{
                    "id": "nc-main-evt",
                    "status": "confirmed",
                    "start": {"dateTime": "2026-03-01T10:00:00Z"},
                    "end": {"dateTime": "2026-03-01T11:00:00Z"},
                    "attendees": [
                        {"email": "engine-nochange@example.com", "self": True, "responseStatus": "accepted"},
                    ],
                }],
                "next_sync_token": "nc-token",
            }

    propagated_calls = []

    async def fake_propagate_rsvp_to_client(**kwargs):
        propagated_calls.append(kwargs)
        return True

    async def fake_sync_main_event_to_clients(**kwargs):
        return []

    monkeypatch.setattr("app.auth.google.get_valid_access_token", fake_get_valid_access_token)
    monkeypatch.setattr("app.sync.engine.GoogleCalendarClient", FakeGoogleCalendarClient)
    monkeypatch.setattr("app.sync.engine.propagate_rsvp_to_client", fake_propagate_rsvp_to_client)
    monkeypatch.setattr("app.sync.engine.sync_main_event_to_clients", fake_sync_main_event_to_clients)

    await trigger_sync_for_main_calendar(user_id)

    # No propagation should happen
    assert len(propagated_calls) == 0


@pytest.mark.asyncio
async def test_main_calendar_sync_skips_null_rsvp(test_db, monkeypatch):
    """_sync_main_calendar should skip RSVP detection for non-invite events (rsvp_status=NULL)."""
    from app.sync.engine import trigger_sync_for_main_calendar

    user_id = await _insert_user(
        email="engine-null@example.com", google_user_id="engine-null-g"
    )
    await _insert_token(user_id, "engine-null@example.com", account_type="home")
    token_id = await _insert_token(user_id, "engine-null-client@example.com")
    cal_id = await _insert_calendar(user_id, token_id, "engine-null-client-cal")
    db = await get_database()

    # Mapping with rsvp_status = NULL (not an invite)
    await db.execute(
        """INSERT INTO event_mappings
           (user_id, origin_type, origin_calendar_id, origin_event_id,
            main_event_id, event_start, event_end, is_all_day, is_recurring,
            user_can_edit, rsvp_status)
           VALUES (?, 'client', ?, 'null-client-evt', 'null-main-evt',
                   '2026-03-01T10:00:00Z', '2026-03-01T11:00:00Z',
                   FALSE, FALSE, TRUE, NULL)""",
        (user_id, cal_id),
    )
    await db.commit()

    async def fake_get_valid_access_token(_uid, _email):
        return "token"

    class FakeGoogleCalendarClient:
        def __init__(self, _token):
            pass

        def list_events(self, _cal, sync_token=None):
            return {
                "events": [{
                    "id": "null-main-evt",
                    "status": "confirmed",
                    "start": {"dateTime": "2026-03-01T10:00:00Z"},
                    "end": {"dateTime": "2026-03-01T11:00:00Z"},
                }],
                "next_sync_token": "null-token",
            }

    propagated_calls = []

    async def fake_propagate_rsvp_to_client(**kwargs):
        propagated_calls.append(kwargs)
        return True

    async def fake_sync_main_event_to_clients(**kwargs):
        return []

    monkeypatch.setattr("app.auth.google.get_valid_access_token", fake_get_valid_access_token)
    monkeypatch.setattr("app.sync.engine.GoogleCalendarClient", FakeGoogleCalendarClient)
    monkeypatch.setattr("app.sync.engine.propagate_rsvp_to_client", fake_propagate_rsvp_to_client)
    monkeypatch.setattr("app.sync.engine.sync_main_event_to_clients", fake_sync_main_event_to_clients)

    await trigger_sync_for_main_calendar(user_id)

    assert len(propagated_calls) == 0


# ---------------------------------------------------------------------------
# Database migration
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_rsvp_status_column_exists(test_db):
    """The rsvp_status column should exist in event_mappings after schema init."""
    db = await get_database()
    cursor = await db.execute("PRAGMA table_info(event_mappings)")
    columns = {row[1] for row in await cursor.fetchall()}
    assert "rsvp_status" in columns
