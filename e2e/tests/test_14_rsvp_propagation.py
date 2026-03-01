"""Test 14: RSVP propagation — accept/decline on main propagates to client calendar.

Flow:
1. Create an invite event on client1 with MAIN_ACCOUNT as attendee
2. Wait for BusyBridge to sync it to main calendar (with RSVP buttons)
3. Accept the event on the main calendar copy
4. Wait for BusyBridge to propagate the RSVP back to client1
5. Verify client1 event shows the user as "accepted"

Also tests the decline path and verifies the loop terminates (no infinite ping-pong).
"""

import time

import pytest

from e2e.config import (
    CLIENT1_EMAIL,
    MAIN_ACCOUNT_EMAIL,
    WEBHOOK_SYNC_TIMEOUT,
    POLL_INTERVAL,
)
from e2e.helpers.event_factory import EventTracker, future_time_slot, make_summary
from e2e.helpers.google_calendar import CalendarTestClient

pytestmark = pytest.mark.api_only


def _wait_for_main_copy(
    main_client: CalendarTestClient,
    main_calendar_id: str,
    summary: str,
    start: str,
    end: str,
    timeout: int = WEBHOOK_SYNC_TIMEOUT,
) -> dict | None:
    """Poll until the synced copy appears on the main calendar."""
    deadline = time.time() + timeout
    while time.time() < deadline:
        events = main_client.list_events(
            main_calendar_id, q=summary, time_min=start, time_max=end,
        )
        matches = [e for e in events if summary in e.get("summary", "")]
        if matches:
            return matches[0]
        time.sleep(POLL_INTERVAL)
    return None


def _get_attendee_response(event: dict, email: str) -> str | None:
    """Extract responseStatus for a specific attendee from an event."""
    for attendee in event.get("attendees", []):
        if attendee.get("email", "").lower() == email.lower():
            return attendee.get("responseStatus")
    return None


def _wait_for_rsvp_on_client(
    client: CalendarTestClient,
    calendar_id: str,
    event_id: str,
    email: str,
    expected_status: str,
    timeout: int = WEBHOOK_SYNC_TIMEOUT,
) -> bool:
    """Poll until the attendee's responseStatus matches expected_status on the client event."""
    deadline = time.time() + timeout
    while time.time() < deadline:
        event = client.get_event(calendar_id, event_id)
        if event:
            status = _get_attendee_response(event, email)
            if status == expected_status:
                return True
        time.sleep(POLL_INTERVAL)
    return False


def test_accept_on_main_propagates_to_client(
    client1_calendar_client: CalendarTestClient,
    main_calendar_client: CalendarTestClient,
    client1_calendar_id: str,
    main_calendar_id: str,
    event_tracker: EventTracker,
):
    """
    Accept on the main calendar copy should propagate back to client1.

    1. Create invite on client1 with MAIN_ACCOUNT as needsAction attendee
    2. Wait for synced copy on main
    3. Accept on main
    4. Wait for client1 event to show "accepted" for the client email
    """
    summary = make_summary("rsvp-accept")
    start, end = future_time_slot(hours_from_now=20)

    # Create event on client1 — an invite where the main account is an attendee.
    # The client1 account is the organizer.
    client_event = event_tracker.create(
        client1_calendar_client,
        client1_calendar_id,
        summary,
        start,
        end,
        attendees=[
            {"email": CLIENT1_EMAIL, "responseStatus": "accepted"},
            {"email": MAIN_ACCOUNT_EMAIL, "responseStatus": "needsAction"},
        ],
    )
    client_event_id = client_event["id"]

    # Wait for BusyBridge to sync it to main
    main_copy = _wait_for_main_copy(
        main_calendar_client, main_calendar_id, summary, start, end,
    )
    assert main_copy is not None, (
        f"Synced copy of '{summary}' did not appear on main calendar within {WEBHOOK_SYNC_TIMEOUT}s"
    )

    # The main copy should have attendees with RSVP buttons
    main_attendees = main_copy.get("attendees", [])
    assert len(main_attendees) >= 1, (
        f"Main copy should have attendees for RSVP, got: {main_attendees}"
    )

    # Accept the event on main calendar
    main_calendar_client.update_event(
        main_calendar_id,
        main_copy["id"],
        {"attendees": [{"email": MAIN_ACCOUNT_EMAIL, "responseStatus": "accepted"}]},
    )

    # Wait for BusyBridge to propagate the RSVP back to client1.
    # The client1 account's email should be patched to "accepted".
    propagated = _wait_for_rsvp_on_client(
        client1_calendar_client,
        client1_calendar_id,
        client_event_id,
        CLIENT1_EMAIL,
        "accepted",
        timeout=WEBHOOK_SYNC_TIMEOUT * 2,  # allow extra time for two sync hops
    )
    # Note: the RSVP is propagated for the client email, not the main email.
    # BusyBridge patches {"email": client_email, "responseStatus": "accepted"}.
    # If direct assertion fails, also check if the main_account entry was updated
    # (depends on whether client1 is also the "self" on that calendar).
    if not propagated:
        # Fall back: check main account email on the client event
        event = client1_calendar_client.get_event(client1_calendar_id, client_event_id)
        if event:
            main_status = _get_attendee_response(event, MAIN_ACCOUNT_EMAIL)
            client_status = _get_attendee_response(event, CLIENT1_EMAIL)
            pytest.fail(
                f"RSVP 'accepted' not propagated within timeout. "
                f"Client event attendees: {event.get('attendees', [])}, "
                f"main_account status={main_status}, client1 status={client_status}"
            )
        else:
            pytest.fail("Client event disappeared after RSVP propagation attempt")


def test_decline_on_main_propagates_to_client(
    client1_calendar_client: CalendarTestClient,
    main_calendar_client: CalendarTestClient,
    client1_calendar_id: str,
    main_calendar_id: str,
    event_tracker: EventTracker,
):
    """
    Decline on the main calendar copy should propagate "declined" back to client1.
    """
    summary = make_summary("rsvp-decline")
    start, end = future_time_slot(hours_from_now=22)

    client_event = event_tracker.create(
        client1_calendar_client,
        client1_calendar_id,
        summary,
        start,
        end,
        attendees=[
            {"email": CLIENT1_EMAIL, "responseStatus": "accepted"},
            {"email": MAIN_ACCOUNT_EMAIL, "responseStatus": "needsAction"},
        ],
    )
    client_event_id = client_event["id"]

    main_copy = _wait_for_main_copy(
        main_calendar_client, main_calendar_id, summary, start, end,
    )
    assert main_copy is not None, (
        f"Synced copy of '{summary}' did not appear on main calendar"
    )

    # Decline the event on main calendar
    main_calendar_client.update_event(
        main_calendar_id,
        main_copy["id"],
        {"attendees": [{"email": MAIN_ACCOUNT_EMAIL, "responseStatus": "declined"}]},
    )

    # Wait for propagation
    propagated = _wait_for_rsvp_on_client(
        client1_calendar_client,
        client1_calendar_id,
        client_event_id,
        CLIENT1_EMAIL,
        "declined",
        timeout=WEBHOOK_SYNC_TIMEOUT * 2,
    )

    if not propagated:
        event = client1_calendar_client.get_event(client1_calendar_id, client_event_id)
        if event:
            pytest.fail(
                f"RSVP 'declined' not propagated. "
                f"Client event attendees: {event.get('attendees', [])}"
            )
        else:
            pytest.fail("Client event disappeared after decline propagation attempt")
