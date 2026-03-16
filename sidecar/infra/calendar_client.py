"""Google Calendar API wrapper for test verification."""

from __future__ import annotations

import logging
from datetime import datetime, timedelta, timezone
from typing import Optional

from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

logger = logging.getLogger(__name__)


class CalendarTestClient:
    """Thin wrapper around Google Calendar v3 API for one account."""

    def __init__(self, email: str, credentials: Credentials):
        self.email = email
        self.service = build(
            "calendar", "v3", credentials=credentials, cache_discovery=False
        )

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
        timezone: str = "America/New_York",
    ) -> dict:
        """Create a calendar event."""
        if all_day:
            time_body = {"start": {"date": start}, "end": {"date": end}}
        else:
            time_body = {
                "start": {"dateTime": start, "timeZone": timezone},
                "end": {"dateTime": end, "timeZone": timezone},
            }

        body: dict = {
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

        event = (
            self.service.events()
            .insert(calendarId=calendar_id, body=body)
            .execute()
        )
        logger.info("Created event %s on %s (%s)", event["id"], calendar_id, summary)
        return event

    def get_event(self, calendar_id: str, event_id: str) -> Optional[dict]:
        """Get a single event, or None if not found / cancelled."""
        try:
            event = (
                self.service.events()
                .get(calendarId=calendar_id, eventId=event_id)
                .execute()
            )
            if event.get("status") == "cancelled":
                return None
            return event
        except HttpError as e:
            if e.resp.status in (404, 410):
                return None
            raise

    def list_events(
        self,
        calendar_id: str,
        *,
        q: Optional[str] = None,
        time_min: Optional[str] = None,
        time_max: Optional[str] = None,
        max_results: int = 100,
        single_events: bool = True,
    ) -> list[dict]:
        """List events, optionally filtered."""
        kwargs: dict = {
            "calendarId": calendar_id,
            "maxResults": max_results,
            "singleEvents": single_events,
            "orderBy": "startTime" if single_events else "updated",
        }
        if q:
            kwargs["q"] = q
        # Default timeMin to 24h ago to avoid fetching years of history
        # when maxResults caps results before reaching recent events.
        effective_time_min = time_min
        if not effective_time_min and single_events:
            effective_time_min = (
                datetime.now(timezone.utc) - timedelta(hours=24)
            ).isoformat()
        if effective_time_min:
            kwargs["timeMin"] = effective_time_min
        if time_max:
            kwargs["timeMax"] = time_max

        result = self.service.events().list(**kwargs).execute()
        return [
            e for e in result.get("items", []) if e.get("status") != "cancelled"
        ]

    def update_event(
        self, calendar_id: str, event_id: str, updates: dict
    ) -> dict:
        """Patch an event with partial updates."""
        event = (
            self.service.events()
            .patch(calendarId=calendar_id, eventId=event_id, body=updates)
            .execute()
        )
        logger.info("Updated event %s on %s", event_id, calendar_id)
        return event

    def delete_event(self, calendar_id: str, event_id: str) -> bool:
        """Delete an event. Returns True on success or already-gone."""
        try:
            self.service.events().delete(
                calendarId=calendar_id, eventId=event_id
            ).execute()
            logger.info("Deleted event %s on %s", event_id, calendar_id)
            return True
        except HttpError as e:
            if e.resp.status in (404, 410):
                return True
            raise

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
