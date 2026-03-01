"""Wrapper around the Google Calendar API for E2E tests."""

from __future__ import annotations

import json
import logging
from typing import Optional

from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build

from e2e.config import CALENDAR_TOKENS_FILE

logger = logging.getLogger(__name__)

# Cache of service objects keyed by email
_services: dict[str, "CalendarTestClient"] = {}


class CalendarTestClient:
    """Thin wrapper around the Google Calendar v3 API for one account."""

    def __init__(self, email: str, credentials: Credentials):
        self.email = email
        self.service = build("calendar", "v3", credentials=credentials)

    # ── Events ────────────────────────────────────────────────────────────

    def create_event(
        self,
        calendar_id: str,
        summary: str,
        start: str,
        end: str,
        *,
        description: str = "",
        location: str = "",
        all_day: bool = False,
        transparency: str = "opaque",
        attendees: Optional[list[dict]] = None,
        recurrence: Optional[list[str]] = None,
    ) -> dict:
        """Create a calendar event and return the API response."""
        if all_day:
            time_body = {"start": {"date": start}, "end": {"date": end}}
        else:
            time_body = {
                "start": {"dateTime": start, "timeZone": "America/New_York"},
                "end": {"dateTime": end, "timeZone": "America/New_York"},
            }

        body = {
            "summary": summary,
            "description": description,
            "location": location,
            "transparency": transparency,
            **time_body,
        }
        if attendees:
            body["attendees"] = attendees
        if recurrence:
            body["recurrence"] = recurrence

        event = self.service.events().insert(
            calendarId=calendar_id, body=body
        ).execute()

        logger.info("Created event %s on %s (%s)", event["id"], calendar_id, summary)
        return event

    def get_event(self, calendar_id: str, event_id: str) -> Optional[dict]:
        """Get a single event, or None if 404/cancelled."""
        try:
            event = self.service.events().get(
                calendarId=calendar_id, eventId=event_id
            ).execute()
            if event.get("status") == "cancelled":
                return None
            return event
        except Exception:
            return None

    def list_events(
        self,
        calendar_id: str,
        *,
        q: Optional[str] = None,
        time_min: Optional[str] = None,
        time_max: Optional[str] = None,
        max_results: int = 50,
        single_events: bool = True,
    ) -> list[dict]:
        """List events, optionally filtering by query string or time range."""
        kwargs: dict = {
            "calendarId": calendar_id,
            "maxResults": max_results,
            "singleEvents": single_events,
            "orderBy": "startTime" if single_events else "updated",
        }
        if q:
            kwargs["q"] = q
        if time_min:
            kwargs["timeMin"] = time_min
        if time_max:
            kwargs["timeMax"] = time_max

        result = self.service.events().list(**kwargs).execute()
        return [e for e in result.get("items", []) if e.get("status") != "cancelled"]

    def update_event(
        self, calendar_id: str, event_id: str, updates: dict
    ) -> dict:
        """Patch an event with partial updates."""
        event = self.service.events().patch(
            calendarId=calendar_id, eventId=event_id, body=updates
        ).execute()
        logger.info("Updated event %s on %s", event_id, calendar_id)
        return event

    def delete_event(self, calendar_id: str, event_id: str) -> None:
        """Delete an event."""
        self.service.events().delete(
            calendarId=calendar_id, eventId=event_id
        ).execute()
        logger.info("Deleted event %s on %s", event_id, calendar_id)

    def find_events_by_prefix(
        self,
        calendar_id: str,
        prefix: str,
        *,
        time_min: Optional[str] = None,
        time_max: Optional[str] = None,
    ) -> list[dict]:
        """Find events whose summary starts with a prefix."""
        events = self.list_events(
            calendar_id, q=prefix, time_min=time_min, time_max=time_max
        )
        return [e for e in events if e.get("summary", "").startswith(prefix)]


def load_client(email: str) -> CalendarTestClient:
    """Load a CalendarTestClient for the given email from saved tokens."""
    if email in _services:
        return _services[email]

    tokens = json.loads(CALENDAR_TOKENS_FILE.read_text())
    if email not in tokens:
        raise RuntimeError(
            f"No saved token for {email}. Run: python -m e2e.auth.get_calendar_tokens"
        )

    cred_data = tokens[email]
    credentials = Credentials(
        token=cred_data.get("token"),
        refresh_token=cred_data["refresh_token"],
        token_uri=cred_data["token_uri"],
        client_id=cred_data["client_id"],
        client_secret=cred_data["client_secret"],
        scopes=cred_data.get("scopes"),
    )

    client = CalendarTestClient(email, credentials)
    _services[email] = client
    return client
