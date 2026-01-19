"""Google Calendar API wrapper."""

import logging
from datetime import datetime, timedelta
from typing import Optional

from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

from app.config import get_settings

logger = logging.getLogger(__name__)


class GoogleCalendarClient:
    """Wrapper around Google Calendar API."""

    def __init__(self, access_token: str):
        """Initialize with access token."""
        self.credentials = Credentials(token=access_token)
        self.service = build("calendar", "v3", credentials=self.credentials)
        self.settings = get_settings()

    def list_events(
        self,
        calendar_id: str,
        sync_token: Optional[str] = None,
        time_min: Optional[datetime] = None,
        time_max: Optional[datetime] = None,
        max_results: int = 2500,
        single_events: bool = False,
    ) -> dict:
        """
        List events from a calendar.

        If sync_token is provided, does incremental sync.
        Otherwise, does full sync with time range.
        """
        try:
            request_params = {
                "calendarId": calendar_id,
                "maxResults": max_results,
                "singleEvents": single_events,
            }

            if sync_token:
                request_params["syncToken"] = sync_token
            else:
                # Full sync - get events from last month to next year
                if not time_min:
                    time_min = datetime.utcnow() - timedelta(days=30)
                if not time_max:
                    time_max = datetime.utcnow() + timedelta(days=365)

                request_params["timeMin"] = time_min.isoformat() + "Z"
                request_params["timeMax"] = time_max.isoformat() + "Z"

            all_events = []
            page_token = None

            while True:
                if page_token:
                    request_params["pageToken"] = page_token

                result = self.service.events().list(**request_params).execute()
                all_events.extend(result.get("items", []))

                page_token = result.get("nextPageToken")
                if not page_token:
                    break

            return {
                "events": all_events,
                "next_sync_token": result.get("nextSyncToken"),
            }

        except HttpError as e:
            if e.resp.status == 410:
                # Sync token expired, need full sync
                logger.info(f"Sync token expired for calendar {calendar_id}")
                return {"events": [], "sync_token_expired": True}
            raise

    def get_event(self, calendar_id: str, event_id: str) -> Optional[dict]:
        """Get a single event."""
        try:
            return self.service.events().get(
                calendarId=calendar_id,
                eventId=event_id,
            ).execute()
        except HttpError as e:
            if e.resp.status == 404:
                return None
            raise

    def create_event(
        self,
        calendar_id: str,
        event_data: dict,
        send_notifications: bool = False,
    ) -> dict:
        """Create an event on a calendar."""
        # Add our sync tag to identify events we created
        if "extendedProperties" not in event_data:
            event_data["extendedProperties"] = {}
        if "private" not in event_data["extendedProperties"]:
            event_data["extendedProperties"]["private"] = {}
        event_data["extendedProperties"]["private"][self.settings.calendar_sync_tag] = "true"

        return self.service.events().insert(
            calendarId=calendar_id,
            body=event_data,
            sendNotifications=send_notifications,
        ).execute()

    def update_event(
        self,
        calendar_id: str,
        event_id: str,
        event_data: dict,
        send_notifications: bool = False,
    ) -> dict:
        """Update an event."""
        return self.service.events().update(
            calendarId=calendar_id,
            eventId=event_id,
            body=event_data,
            sendNotifications=send_notifications,
        ).execute()

    def patch_event(
        self,
        calendar_id: str,
        event_id: str,
        event_patch: dict,
        send_notifications: bool = False,
    ) -> dict:
        """Patch (partial update) an event."""
        return self.service.events().patch(
            calendarId=calendar_id,
            eventId=event_id,
            body=event_patch,
            sendNotifications=send_notifications,
        ).execute()

    def delete_event(
        self,
        calendar_id: str,
        event_id: str,
        send_notifications: bool = False,
    ) -> bool:
        """Delete an event."""
        try:
            self.service.events().delete(
                calendarId=calendar_id,
                eventId=event_id,
                sendNotifications=send_notifications,
            ).execute()
            return True
        except HttpError as e:
            if e.resp.status == 404:
                # Already deleted
                return True
            if e.resp.status == 410:
                # Event was deleted (gone)
                return True
            raise

    def is_our_event(self, event: dict) -> bool:
        """Check if an event was created by our sync engine."""
        ext_props = event.get("extendedProperties", {})
        private_props = ext_props.get("private", {})
        return private_props.get(self.settings.calendar_sync_tag) == "true"

    def list_calendars(self) -> list[dict]:
        """List all calendars the user has access to."""
        result = self.service.calendarList().list().execute()
        return result.get("items", [])

    def get_calendar(self, calendar_id: str) -> Optional[dict]:
        """Get calendar metadata."""
        try:
            return self.service.calendars().get(calendarId=calendar_id).execute()
        except HttpError as e:
            if e.resp.status == 404:
                return None
            raise


def create_busy_block(
    start: dict,
    end: dict,
    is_all_day: bool = False,
) -> dict:
    """
    Create a busy block event structure.

    Args:
        start: Event start (dict with dateTime or date)
        end: Event end (dict with dateTime or date)
        is_all_day: Whether this is an all-day event
    """
    settings = get_settings()

    event = {
        "summary": settings.busy_block_title,
        "description": "",
        "visibility": "private",
        "transparency": "opaque",  # Show as busy
    }

    if is_all_day:
        event["start"] = {"date": start.get("date")}
        event["end"] = {"date": end.get("date")}
    else:
        event["start"] = {"dateTime": start.get("dateTime"), "timeZone": start.get("timeZone", "UTC")}
        event["end"] = {"dateTime": end.get("dateTime"), "timeZone": end.get("timeZone", "UTC")}

    return event


def copy_event_for_main(source_event: dict) -> dict:
    """
    Create a copy of an event suitable for the main calendar.

    Includes full details (title, description, location, etc.)
    """
    event = {
        "summary": source_event.get("summary", "Untitled Event"),
        "description": source_event.get("description", ""),
        "location": source_event.get("location", ""),
        "start": source_event["start"],
        "end": source_event["end"],
        "transparency": source_event.get("transparency", "opaque"),
    }

    # Copy recurrence rules if present
    if "recurrence" in source_event:
        event["recurrence"] = source_event["recurrence"]

    # Copy attendees for reference (but don't actually invite them)
    if "attendees" in source_event:
        # Store as description instead of actual attendees
        attendee_list = [a.get("email", "") for a in source_event["attendees"]]
        if attendee_list:
            event["description"] = (
                event.get("description", "") +
                f"\n\nOriginal attendees: {', '.join(attendee_list)}"
            ).strip()

    return event


def should_create_busy_block(event: dict) -> bool:
    """
    Determine if we should create a busy block for this event.

    Rules:
    - Skip declined events
    - Skip "Free" all-day events
    - Skip cancelled events
    """
    # Skip cancelled events
    if event.get("status") == "cancelled":
        return False

    # Check if user declined
    for attendee in event.get("attendees", []):
        if attendee.get("self") and attendee.get("responseStatus") == "declined":
            return False

    # For all-day events, only create busy block if marked as "busy"
    is_all_day = "date" in event.get("start", {})
    if is_all_day:
        # transparency == "transparent" means "Free"
        # transparency == "opaque" means "Busy"
        if event.get("transparency") == "transparent":
            return False

    return True


def can_user_edit_event(event: dict, user_email: str) -> bool:
    """
    Determine if user can edit this event.

    User can edit if:
    - They are the organizer
    - guestsCanModify is true
    - They have writer access
    """
    # Check if user is organizer
    organizer = event.get("organizer", {})
    if organizer.get("email", "").lower() == user_email.lower():
        return True
    if organizer.get("self"):
        return True

    # Check guestsCanModify
    if event.get("guestsCanModify"):
        return True

    # For events where user is the creator
    creator = event.get("creator", {})
    if creator.get("email", "").lower() == user_email.lower():
        return True
    if creator.get("self"):
        return True

    return False
