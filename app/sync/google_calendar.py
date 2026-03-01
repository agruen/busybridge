"""Google Calendar API wrapper."""

import logging
from datetime import datetime, timedelta, timezone
from typing import Optional

from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

from app.config import get_settings

logger = logging.getLogger(__name__)


class GoogleCalendarClient:
    """Wrapper around Google Calendar API."""

    def __init__(self, access_token: str, settings=None, timeout: int = 30):
        """
        Initialize with access token.

        Args:
            access_token: Google OAuth access token
            settings: Settings object (optional, will use get_settings() if not provided)
            timeout: Request timeout in seconds (default: 30)
        """
        self.credentials = Credentials(token=access_token)
        # Build service with timeout support
        import httplib2
        http = httplib2.Http(timeout=timeout)
        http = self.credentials.authorize(http)
        self.service = build("calendar", "v3", http=http)
        self.settings = settings or get_settings()

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
            elif e.resp.status == 403:
                # Permission denied - calendar access revoked
                logger.error(f"Permission denied for calendar {calendar_id}")
                raise PermissionError(f"Access to calendar {calendar_id} was revoked")
            elif e.resp.status == 404:
                # Calendar deleted
                logger.warning(f"Calendar {calendar_id} not found (may have been deleted)")
                raise FileNotFoundError(f"Calendar {calendar_id} not found")
            else:
                # Other HTTP errors
                logger.error(f"HTTP error {e.resp.status} fetching events for calendar {calendar_id}")
                raise
        except Exception as e:
            # Network errors, timeouts, etc.
            logger.error(f"Network error fetching events for calendar {calendar_id}: {type(e).__name__}")
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

    def search_events(
        self,
        calendar_id: str,
        query: str,
        max_results: int = 2500,
        single_events: bool = True,
    ) -> list[dict]:
        """
        Search events by free-text query.

        Uses pagination and returns all matching items from Google.
        """
        try:
            request_params = {
                "calendarId": calendar_id,
                "q": query,
                "maxResults": max_results,
                "singleEvents": single_events,
                "showDeleted": False,
            }

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

            return all_events

        except HttpError as e:
            if e.resp.status == 403:
                logger.error(f"Permission denied searching events for calendar {calendar_id}")
                raise PermissionError(f"Access to calendar {calendar_id} was revoked")
            if e.resp.status == 404:
                logger.warning(f"Calendar {calendar_id} not found during event search")
                raise FileNotFoundError(f"Calendar {calendar_id} not found")
            logger.error(f"HTTP error {e.resp.status} searching events for calendar {calendar_id}")
            raise
        except Exception as e:
            logger.error(f"Network error searching events for calendar {calendar_id}: {type(e).__name__}")
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
        # Re-stamp our sync tag -- events().update() is a full replacement,
        # so without this the extendedProperties (and our tag) get stripped.
        if "extendedProperties" not in event_data:
            event_data["extendedProperties"] = {}
        if "private" not in event_data["extendedProperties"]:
            event_data["extendedProperties"]["private"] = {}
        event_data["extendedProperties"]["private"][self.settings.calendar_sync_tag] = "true"

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


def _build_timed_dt(src: dict) -> dict:
    """Build a dateTime start/end dict, preserving the source timezone correctly.

    If the source has a named ``timeZone`` field, it is passed through as-is.
    If the ``dateTime`` ends with ``Z`` (UTC), we explicitly label it as UTC.
    If the ``dateTime`` carries a fixed offset (e.g. ``-05:00``) but no named
    timezone, we omit the ``timeZone`` field entirely so that Google Calendar
    uses the embedded offset rather than defaulting to UTC.  This is critical
    for recurring events: setting ``timeZone: "UTC"`` would anchor the
    recurrence pattern to UTC wall-clock time, causing every instance to drift
    by one hour after a DST transition.
    """
    dt = src.get("dateTime")
    tz = src.get("timeZone")
    result: dict = {"dateTime": dt}
    if tz:
        result["timeZone"] = tz
    elif dt and dt.endswith("Z"):
        result["timeZone"] = "UTC"
    # Fixed-offset dateTime (e.g. "…-05:00"): omit timeZone and let Google
    # honour the embedded offset for RRULE expansion.
    return result


def derive_instance_event_id(parent_event_id: str, original_start_time: dict) -> str:
    """Construct the Google Calendar instance event ID for one occurrence.

    Google formats recurring-event instance IDs as::

        "<parentId>_<utcTimestamp>"

    where *utcTimestamp* is ``YYYYMMDDTHHmmssZ`` for timed events or
    ``YYYYMMDD`` for all-day events.

    Args:
        parent_event_id: The ``id`` of the parent recurring event.
        original_start_time: The ``originalStartTime`` dict returned by the
            Google Calendar API for the instance (contains either a ``date``
            or a ``dateTime`` field).

    Returns:
        The full instance event ID string.

    Raises:
        ValueError: If *original_start_time* has neither ``date`` nor
            ``dateTime``.
    """
    if "date" in original_start_time:
        date_suffix = original_start_time["date"].replace("-", "")
        return f"{parent_event_id}_{date_suffix}"

    dt_str = original_start_time.get("dateTime", "")
    if not dt_str:
        raise ValueError(
            "originalStartTime has neither 'date' nor 'dateTime': "
            f"{original_start_time!r}"
        )

    if dt_str.endswith("Z"):
        dt = datetime.fromisoformat(dt_str[:-1]).replace(tzinfo=timezone.utc)
    else:
        dt = datetime.fromisoformat(dt_str)

    dt_utc = dt.astimezone(timezone.utc)
    suffix = dt_utc.strftime("%Y%m%dT%H%M%S") + "Z"
    return f"{parent_event_id}_{suffix}"


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

    summary = settings.busy_block_title
    prefix = (settings.managed_event_prefix or "").strip()
    if prefix:
        summary = f"{prefix} {summary}".strip()

    event = {
        "summary": summary,
        "description": "",
        "visibility": "private",
        "transparency": "opaque",  # Show as busy
    }

    if is_all_day:
        event["start"] = {"date": start.get("date")}
        event["end"] = {"date": end.get("date")}
    else:
        event["start"] = _build_timed_dt(start)
        event["end"] = _build_timed_dt(end)

    return event


def create_personal_busy_block(
    start: dict,
    end: dict,
    is_all_day: bool = False,
) -> dict:
    """
    Create a busy block for a personal calendar event.

    Uses the distinct personal title (e.g. "[BusyBridge] Busy (Personal)")
    so these blocks are visually distinguishable from client-origin busy
    blocks and can be identified during manual cleanup.
    """
    settings = get_settings()

    summary = settings.personal_busy_block_title
    prefix = (settings.managed_event_prefix or "").strip()
    if prefix:
        summary = f"{prefix} {summary}".strip()

    event = {
        "summary": summary,
        "description": "",
        "visibility": "private",
        "transparency": "opaque",  # Show as busy
    }

    if is_all_day:
        event["start"] = {"date": start.get("date")}
        event["end"] = {"date": end.get("date")}
    else:
        event["start"] = _build_timed_dt(start)
        event["end"] = _build_timed_dt(end)

    return event


def copy_event_for_main(
    source_event: dict,
    source_label: Optional[str] = None,
    color_id: Optional[str] = None,
) -> dict:
    """
    Create a copy of an event suitable for the main calendar.

    Includes full details (title, description, location, etc.)
    """
    settings = get_settings()

    base_summary = source_event.get("summary", "Untitled Event")
    prefix = (settings.managed_event_prefix or "").strip()
    source_display = (source_label or "").strip()
    if len(source_display) > 80:
        source_display = source_display[:77] + "..."

    marker_parts = []
    if prefix:
        marker_parts.append(prefix)
    if source_display:
        marker_parts.append(f"[{source_display}]")
    marker_prefix = " ".join(marker_parts).strip()
    summary = f"{marker_prefix} {base_summary}".strip() if marker_prefix else base_summary

    event = {
        "summary": summary,
        "description": source_event.get("description", ""),
        "location": source_event.get("location", ""),
        "start": source_event["start"],
        "end": source_event["end"],
        "transparency": source_event.get("transparency", "opaque"),
    }

    # Apply color to distinguish events from different client calendars
    if color_id:
        event["colorId"] = color_id

    if source_display:
        source_line = f"BusyBridge source: {source_display}"
        event["description"] = (
            f"{source_line}\n\n{event['description']}".strip()
            if event["description"]
            else source_line
        )

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
