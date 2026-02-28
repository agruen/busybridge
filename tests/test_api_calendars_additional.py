"""Additional coverage tests for api.calendars branches."""

from __future__ import annotations

import pytest
from fastapi import HTTPException

from app.auth.session import User
from app.database import get_database


async def _insert_user(email: str, google_user_id: str) -> int:
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


async def _insert_token(user_id: int, email: str) -> int:
    db = await get_database()
    cursor = await db.execute(
        """INSERT INTO oauth_tokens
           (user_id, account_type, google_account_email, access_token_encrypted, refresh_token_encrypted)
           VALUES (?, 'client', ?, ?, ?)
           RETURNING id""",
        (user_id, email, b"a", b"r"),
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


def _user_model(user_id: int, email: str) -> User:
    return User(
        id=user_id,
        email=email,
        google_user_id=f"{email}-google",
        display_name="User",
        main_calendar_id="main-cal",
        is_admin=False,
    )


@pytest.mark.asyncio
async def test_list_client_calendars_warning_and_error_statuses(test_db):
    """List endpoint should classify warning/error statuses from failure counts."""
    from app.api.calendars import list_client_calendars

    user_id = await _insert_user("status@example.com", "status-google")
    token_id = await _insert_token(user_id, "status-client@example.com")
    cal_ok = await _insert_calendar(user_id, token_id, "status-ok")
    cal_warn = await _insert_calendar(user_id, token_id, "status-warn")
    cal_err = await _insert_calendar(user_id, token_id, "status-err")
    db = await get_database()
    await db.execute(
        "INSERT INTO calendar_sync_state (client_calendar_id, consecutive_failures) VALUES (?, ?)",
        (cal_ok, 0),
    )
    await db.execute(
        "INSERT INTO calendar_sync_state (client_calendar_id, consecutive_failures) VALUES (?, ?)",
        (cal_warn, 1),
    )
    await db.execute(
        "INSERT INTO calendar_sync_state (client_calendar_id, consecutive_failures) VALUES (?, ?)",
        (cal_err, 5),
    )
    await db.commit()

    calendars = await list_client_calendars(user=_user_model(user_id, "status@example.com"))
    by_id = {c.id: c.sync_status for c in calendars}
    assert by_id[cal_warn] == "warning"
    assert by_id[cal_err] == "error"


@pytest.mark.asyncio
async def test_connect_calendar_token_missing_and_calendar_verify_failure(test_db, monkeypatch):
    """Connect endpoint should return 404 for missing token and 400 for calendar verification errors."""
    from app.api.calendars import ConnectCalendarRequest, connect_client_calendar

    user_id = await _insert_user("connect@example.com", "connect-google")
    user = _user_model(user_id, "connect@example.com")

    with pytest.raises(HTTPException) as missing_exc:
        await connect_client_calendar(
            request=ConnectCalendarRequest(token_id=999999, calendar_id="cal-1"),
            user=user,
        )
    assert missing_exc.value.status_code == 404

    token_id = await _insert_token(user_id, "connect-client@example.com")

    async def fake_get_valid_access_token(_user_id: int, _email: str) -> str:
        return "token"

    def exploding_build(*_args, **_kwargs):
        raise RuntimeError("google unavailable")

    monkeypatch.setattr("app.api.calendars.get_valid_access_token", fake_get_valid_access_token)
    monkeypatch.setattr("googleapiclient.discovery.build", exploding_build)

    with pytest.raises(HTTPException) as verify_exc:
        await connect_client_calendar(
            request=ConnectCalendarRequest(token_id=token_id, calendar_id="cal-2"),
            user=user,
        )
    assert verify_exc.value.status_code == 400
    assert "Cannot access calendar" in verify_exc.value.detail


@pytest.mark.asyncio
async def test_color_auto_assignment_on_connect(test_db, monkeypatch):
    """Connecting calendars should auto-assign distinct Google Calendar color IDs."""
    from app.api.calendars import ConnectCalendarRequest, connect_client_calendar

    user_id = await _insert_user("color@example.com", "color-google")
    token_id = await _insert_token(user_id, "color-client@example.com")
    user = _user_model(user_id, "color@example.com")

    async def fake_get_valid_access_token(_user_id, _email):
        return "token"

    cal_counter = {"n": 0}

    def fake_build(*_a, **_kw):
        svc = type("Svc", (), {})()

        def calendars():
            c = type("C", (), {})()

            def get(calendarId=""):
                g = type("G", (), {})()
                cal_counter["n"] += 1
                g.execute = lambda: {"summary": f"Cal {cal_counter['n']}"}
                return g
            c.get = get
            return c
        svc.calendars = calendars
        return svc

    monkeypatch.setattr("app.api.calendars.get_valid_access_token", fake_get_valid_access_token)
    monkeypatch.setattr("googleapiclient.discovery.build", fake_build)
    monkeypatch.setattr("app.utils.tasks.create_background_task", lambda coro, *a, **kw: coro.close())

    # Connect three calendars
    await connect_client_calendar(
        request=ConnectCalendarRequest(token_id=token_id, calendar_id="cal-a"),
        user=user,
    )
    await connect_client_calendar(
        request=ConnectCalendarRequest(token_id=token_id, calendar_id="cal-b"),
        user=user,
    )
    await connect_client_calendar(
        request=ConnectCalendarRequest(token_id=token_id, calendar_id="cal-c"),
        user=user,
    )

    # Verify they got different colors assigned in the DB
    db = await get_database()
    cursor = await db.execute(
        "SELECT color_id FROM client_calendars WHERE user_id = ? AND is_active = TRUE ORDER BY id",
        (user_id,),
    )
    colors = [row["color_id"] for row in await cursor.fetchall()]
    assert len(colors) == 3
    assert len(set(colors)) == 3  # All different
    assert all(c in [str(i) for i in range(1, 12)] for c in colors)


@pytest.mark.asyncio
async def test_calendar_sync_and_status_not_found_paths(test_db):
    """Manual sync and status endpoints should 404 for unknown calendars."""
    from app.api.calendars import get_calendar_status, trigger_calendar_sync

    user_id = await _insert_user("notfound@example.com", "notfound-google")
    user = _user_model(user_id, "notfound@example.com")

    with pytest.raises(HTTPException) as sync_exc:
        await trigger_calendar_sync(calendar_id=99999, user=user)
    assert sync_exc.value.status_code == 404

    with pytest.raises(HTTPException) as status_exc:
        await get_calendar_status(calendar_id=99999, user=user)
    assert status_exc.value.status_code == 404
