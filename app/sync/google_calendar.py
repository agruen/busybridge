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

    def __init__(self, access_token=None, *, credentials=None, settings=None, timeout: int = 30):
        """
        Initialize with access token or pre-built credentials.

        Args:
            access_token: Google OAuth access token (positional, for backward compat)
            credentials: Pre-built credentials object (e.g. service account)
            settings: Settings object (optional, will use get_settings() if not provided)
            timeout: Request timeout in seconds (default: 30)
        """
        if credentials is not None:
            self.credentials = credentials
        elif access_token is not None:
            self.credentials = Credentials(token=access_token)
        else:
            raise ValueError("Either access_token or credentials must be provided")
        self.service = build("calendar", "v3", credentials=self.credentials)
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

    def list_cancelled_instances(self, calendar_id: str, recurring_event_id: str) -> list[dict]:
        """Return cancelled instances of a recurring event.

        Uses the ``events().instances()`` endpoint with ``showDeleted=True``
        and filters for ``status == 'cancelled'``.  Each returned dict
        contains at minimum ``originalStartTime`` which can be fed to
        ``derive_instance_event_id`` to construct the instance ID on a
        different recurring event (e.g. a busy-block copy).
        """
        try:
            all_items: list[dict] = []
            page_token = None
            while True:
                params: dict = {
                    "calendarId": calendar_id,
                    "eventId": recurring_event_id,
                    "showDeleted": True,
                    "maxResults": 250,
                }
                if page_token:
                    params["pageToken"] = page_token
                result = self.service.events().instances(**params).execute()
                all_items.extend(result.get("items", []))
                page_token = result.get("nextPageToken")
                if not page_token:
                    break
            return [i for i in all_items if i.get("status") == "cancelled"]
        except HttpError as e:
            if e.resp.status in (404, 410):
                return []
            raise
        except Exception:
            return []

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

    def find_by_origin(
        self,
        calendar_id: str,
        origin_id: str,
        bb_type: Optional[str] = None,
    ) -> list[dict]:
        """Find events we created for a specific origin event ID.

        Uses Google Calendar's privateExtendedProperty filter for an
        efficient server-side query.  Returns all non-cancelled matches.
        """
        try:
            params: dict = {
                "calendarId": calendar_id,
                "privateExtendedProperty": f"bb_origin_id={origin_id}",
                "showDeleted": False,
                "maxResults": 50,
                "singleEvents": False,
            }
            if bb_type:
                params["privateExtendedProperty"] = [
                    f"bb_origin_id={origin_id}",
                    f"bb_type={bb_type}",
                ]
            result = self.service.events().list(**params).execute()
            return [
                e for e in result.get("items", [])
                if e.get("status") != "cancelled"
            ]
        except HttpError as e:
            if e.resp.status in (404, 403):
                return []
            raise
        except Exception:
            return []

    def list_our_events(
        self,
        calendar_id: str,
        time_min: Optional[str] = None,
        time_max: Optional[str] = None,
    ) -> list[dict]:
        """List all events we created (have our sync tag) on a calendar."""
        try:
            params: dict = {
                "calendarId": calendar_id,
                "privateExtendedProperty": f"{self.settings.calendar_sync_tag}=true",
                "showDeleted": False,
                "maxResults": 2500,
                "singleEvents": False,
            }
            if time_min:
                params["timeMin"] = time_min
            if time_max:
                params["timeMax"] = time_max

            all_events = []
            page_token = None
            while True:
                if page_token:
                    params["pageToken"] = page_token
                result = self.service.events().list(**params).execute()
                all_events.extend(
                    e for e in result.get("items", [])
                    if e.get("status") != "cancelled"
                )
                page_token = result.get("nextPageToken")
                if not page_token:
                    break
            return all_events
        except HttpError as e:
            if e.resp.status in (404, 403):
                return []
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
        send_updates: str = "none",
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
            sendUpdates=send_updates,
            conferenceDataVersion=1,
        ).execute()

    def update_event(
        self,
        calendar_id: str,
        event_id: str,
        event_data: dict,
        send_notifications: bool = False,
        send_updates: str = "none",
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
            sendUpdates=send_updates,
            conferenceDataVersion=1,
        ).execute()

    def patch_event(
        self,
        calendar_id: str,
        event_id: str,
        event_patch: dict,
        send_notifications: bool = False,
        send_updates: str = "none",
    ) -> dict:
        """Patch (partial update) an event."""
        return self.service.events().patch(
            calendarId=calendar_id,
            eventId=event_id,
            body=event_patch,
            sendNotifications=send_notifications,
            sendUpdates=send_updates,
            conferenceDataVersion=1,
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

    def batch_delete_events(
        self,
        calendar_id: str,
        event_ids: list[str],
        batch_size: int = 50,
    ) -> tuple[int, list[str]]:
        """Delete multiple events in batch requests.

        Returns (deleted_count, failed_event_ids).
        404/410 responses are treated as success (already gone).
        """
        deleted = 0
        failed: list[str] = []

        for i in range(0, len(event_ids), batch_size):
            chunk = event_ids[i : i + batch_size]
            batch = self.service.new_batch_http_request()

            # Track results per event in this chunk
            chunk_results: dict[str, bool] = {}

            def _make_callback(eid: str):
                def _cb(request_id, response, exception):
                    if exception is None:
                        chunk_results[eid] = True
                    elif isinstance(exception, HttpError) and exception.resp.status in (404, 410):
                        chunk_results[eid] = True  # Already gone
                    else:
                        chunk_results[eid] = False
                return _cb

            for eid in chunk:
                batch.add(
                    self.service.events().delete(
                        calendarId=calendar_id, eventId=eid,
                    ),
                    callback=_make_callback(eid),
                )

            try:
                batch.execute()
            except Exception:
                # Entire batch failed — mark all as failed
                for eid in chunk:
                    if eid not in chunk_results:
                        chunk_results[eid] = False

            for eid, ok in chunk_results.items():
                if ok:
                    deleted += 1
                else:
                    failed.append(eid)

        return deleted, failed

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


def _set_bb_props(event: dict, props: Optional[dict]) -> None:
    """Embed BusyBridge origin metadata into extendedProperties.private."""
    if not props:
        return
    ep = event.setdefault("extendedProperties", {})
    priv = ep.setdefault("private", {})
    priv.update(props)


def create_busy_block(
    start: dict,
    end: dict,
    is_all_day: bool = False,
    use_service_account: bool = False,
    main_email: Optional[str] = None,
    origin_props: Optional[dict] = None,
) -> dict:
    """
    Create a busy block event structure.

    Args:
        start: Event start (dict with dateTime or date)
        end: Event end (dict with dateTime or date)
        is_all_day: Whether this is an all-day event
        use_service_account: If True, add guestsCanModify=false and user as attendee
        main_email: User's main calendar email (required when use_service_account=True)
    """
    settings = get_settings()

    summary = settings.busy_block_title
    prefix = (settings.managed_event_prefix or "").strip()
    if prefix:
        summary = f"{prefix} {summary}".strip()
    # Lock emoji — busy blocks are never user-editable
    summary = f"\U0001f510 {summary}"

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

    if use_service_account:
        event["guestsCanModify"] = False
        if main_email:
            event["attendees"] = [{"email": main_email, "responseStatus": "accepted"}]

    _set_bb_props(event, origin_props)
    return event


def create_personal_busy_block(
    start: dict,
    end: dict,
    is_all_day: bool = False,
    use_service_account: bool = False,
    main_email: Optional[str] = None,
    origin_props: Optional[dict] = None,
) -> dict:
    """
    Create a busy block event for a personal calendar event.

    Uses the personal_busy_block_title setting for the summary.
    """
    settings = get_settings()

    summary = settings.personal_busy_block_title
    prefix = (settings.managed_event_prefix or "").strip()
    if prefix:
        summary = f"{prefix} {summary}".strip()
    # Lock emoji — busy blocks are never user-editable
    summary = f"\U0001f510 {summary}"

    event = {
        "summary": summary,
        "description": "",
        "visibility": "private",
        "transparency": "opaque",
    }

    if is_all_day:
        event["start"] = {"date": start.get("date")}
        event["end"] = {"date": end.get("date")}
    else:
        event["start"] = _build_timed_dt(start)
        event["end"] = _build_timed_dt(end)

    if use_service_account:
        event["guestsCanModify"] = False
        if main_email:
            event["attendees"] = [{"email": main_email, "responseStatus": "accepted"}]

    _set_bb_props(event, origin_props)
    return event


def copy_event_for_main(
    source_event: dict,
    source_label: Optional[str] = None,
    color_id: Optional[str] = None,
    main_email: Optional[str] = None,
    current_rsvp_status: Optional[str] = None,
    user_can_edit: bool = True,
    use_service_account: bool = False,
    origin_props: Optional[dict] = None,
) -> dict:
    """
    Create a copy of an event suitable for the main calendar.

    Includes full details (title, description, location, etc.)
    """
    settings = get_settings()

    summary = source_event.get("summary", "Untitled Event")
    if not user_can_edit:
        summary = f"\U0001f510 {summary}"

    source_display = (source_label or "").strip()
    if len(source_display) > 80:
        source_display = source_display[:77] + "..."

    event = {
        "summary": summary,
        "description": source_event.get("description", ""),
        "location": source_event.get("location", ""),
        "start": source_event["start"],
        "end": source_event["end"],
        "transparency": source_event.get("transparency", "opaque"),
    }

    # Service account mode: SA is organizer, so control editability via guestsCanModify
    if use_service_account:
        event["guestsCanModify"] = user_can_edit
        # Ensure the user is an attendee so they see the event on their calendar
        if main_email:
            event["attendees"] = [{
                "email": main_email,
                "responseStatus": current_rsvp_status or "accepted",
            }]

    # Apply color to distinguish events from different client calendars
    if color_id:
        event["colorId"] = color_id

    # Copy recurrence rules if present
    if "recurrence" in source_event:
        event["recurrence"] = source_event["recurrence"]

    # Copy conference/video call data (Google Meet, Zoom links, etc.)
    if "conferenceData" in source_event:
        event["conferenceData"] = source_event["conferenceData"]
    if "hangoutLink" in source_event:
        event["hangoutLink"] = source_event["hangoutLink"]

    # Inject main_email as sole attendee so Google shows native RSVP buttons.
    # Use current_rsvp_status (from DB) on updates to avoid resetting an
    # existing response back to needsAction.
    # Skip if SA mode already set attendees above.
    if not use_service_account and source_event.get("attendees") and main_email:
        event["attendees"] = [{
            "email": main_email,
            "responseStatus": current_rsvp_status or "needsAction",
            "self": True,
        }]

    # Append metadata to the END of the description so the actual content
    # stays front and center.  The managed_event_prefix tag is included so
    # events can still be bulk-found/deleted in an emergency.
    footer_parts = []

    prefix = (settings.managed_event_prefix or "").strip()
    if prefix:
        footer_parts.append(f"Managed by {prefix}")

    if "attendees" in source_event:
        attendee_list = [a.get("email", "") for a in source_event["attendees"]]
        if attendee_list:
            footer_parts.append(f"Original attendees: {', '.join(attendee_list)}")

    if source_display:
        footer_parts.append(f"Source: {source_display}")

    # Include a link to the original event so the user can RSVP there
    if source_event.get("htmlLink"):
        footer_parts.append(f"Original event: {source_event['htmlLink']}")

    if footer_parts:
        footer = "\n".join(footer_parts)
        base_desc = event["description"]
        event["description"] = f"{base_desc}\n\n---\n{footer}".strip() if base_desc else footer

    _set_bb_props(event, origin_props)
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
