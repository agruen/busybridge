"""ICS/webcal feed fetcher and parser."""

import hashlib
import logging
import re
from datetime import datetime, timedelta, date, timezone
from typing import Optional

import httpx
import icalendar
import recurring_ical_events

from app.sync.google_calendar import _set_bb_props

logger = logging.getLogger(__name__)

# Matches bare UUID-style UIDs (v4-ish) that some feeds regenerate on every
# request.  Standard ICS UIDs typically include a domain suffix or other
# stable identifier, so this only catches the pathological case.
_UNSTABLE_UUID_RE = re.compile(
    r'^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$',
    re.IGNORECASE,
)


async def fetch_ics_feed(
    url: str,
    etag: Optional[str] = None,
    timeout: float = 30.0,
) -> tuple[Optional[str], Optional[str]]:
    """Fetch an ICS feed from a URL.

    Returns (ics_content, new_etag) or (None, None) if 304 Not Modified.
    """
    # Normalize webcal:// to https://
    if url.startswith("webcal://"):
        url = "https://" + url[len("webcal://"):]

    headers = {}
    if etag:
        headers["If-None-Match"] = etag

    async with httpx.AsyncClient(follow_redirects=True, timeout=timeout) as client:
        response = await client.get(url, headers=headers)

    if response.status_code == 304:
        return None, None

    response.raise_for_status()

    new_etag = response.headers.get("ETag")
    return response.text, new_etag


def parse_ics_events(
    ics_content: str,
    time_min: datetime,
    time_max: datetime,
) -> list[dict]:
    """Parse ICS content and expand recurring events into individual instances.

    Returns a list of dicts with keys: ics_uid, summary, start, end,
    location, description, transparency, is_all_day.
    """
    cal = icalendar.Calendar.from_ical(ics_content)

    events = recurring_ical_events.of(cal).between(time_min, time_max)

    results = []
    for component in events:
        if component.name != "VEVENT":
            continue

        raw_uid = str(component.get("UID", ""))
        if not raw_uid:
            continue

        summary = str(component.get("SUMMARY", "Untitled"))
        location = str(component.get("LOCATION", "")) if component.get("LOCATION") else ""
        description = str(component.get("DESCRIPTION", "")) if component.get("DESCRIPTION") else ""

        # Parse transparency
        transp = str(component.get("TRANSP", "OPAQUE")).upper()
        transparency = "transparent" if transp == "TRANSPARENT" else "opaque"

        # Parse start/end
        dtstart = component.get("DTSTART")
        dtend = component.get("DTEND")

        if dtstart is None:
            continue

        dt_start_val = dtstart.dt if hasattr(dtstart, 'dt') else dtstart
        dt_end_val = dtend.dt if dtend and hasattr(dtend, 'dt') else dtend

        # Handle missing DTEND
        if dt_end_val is None:
            duration = component.get("DURATION")
            if duration and hasattr(duration, 'dt'):
                dt_end_val = dt_start_val + duration.dt
            elif isinstance(dt_start_val, date) and not isinstance(dt_start_val, datetime):
                dt_end_val = dt_start_val + timedelta(days=1)
            else:
                dt_end_val = dt_start_val + timedelta(hours=1)

        is_all_day = isinstance(dt_start_val, date) and not isinstance(dt_start_val, datetime)

        # Build Google Calendar compatible start/end dicts
        if is_all_day:
            start_dict = {"date": dt_start_val.isoformat()}
            end_dict = {"date": dt_end_val.isoformat()}
        else:
            # Ensure timezone-aware
            if dt_start_val.tzinfo is None:
                dt_start_val = dt_start_val.replace(tzinfo=timezone.utc)
            if dt_end_val.tzinfo is None:
                dt_end_val = dt_end_val.replace(tzinfo=timezone.utc)
            start_dict = {"dateTime": dt_start_val.isoformat()}
            end_dict = {"dateTime": dt_end_val.isoformat()}

        # Some feeds (e.g. ISO) generate random UUIDs on every request,
        # making the UID useless for matching across polls.  Detect v4-style
        # UUIDs and replace them with a content-based hash so we get a
        # stable identity for the event.
        if _UNSTABLE_UUID_RE.match(raw_uid):
            start_str = dt_start_val.isoformat()
            end_str = dt_end_val.isoformat() if dt_end_val else ""
            content_key = f"{summary}\0{start_str}\0{end_str}"
            uid = hashlib.sha256(content_key.encode()).hexdigest()[:16]
        else:
            uid = raw_uid

        # Build stable UID for recurring instances
        if is_all_day:
            ics_uid = f"{uid}@{dt_start_val.isoformat()}"
        else:
            ics_uid = f"{uid}@{dt_start_val.strftime('%Y%m%dT%H%M%SZ') if dt_start_val.tzinfo else dt_start_val.strftime('%Y%m%dT%H%M%S')}"

        results.append({
            "ics_uid": ics_uid,
            "summary": summary,
            "start": start_dict,
            "end": end_dict,
            "location": location,
            "description": description,
            "transparency": transparency,
            "is_all_day": is_all_day,
        })

    return results


def build_webcal_google_event(
    parsed_event: dict,
    prefix: str,
    origin_props: Optional[dict] = None,
) -> dict:
    """Convert a parsed ICS event to a Google Calendar event body.

    Prepends subscription prefix + lock icon to summary, and appends a
    "Managed by" footer to the description.
    """
    from app.config import get_settings
    settings = get_settings()

    summary = parsed_event["summary"]
    if prefix:
        summary = f"{prefix} {summary}".strip()
    summary = f"\U0001f510 {summary}"

    description = parsed_event.get("description", "")
    managed_prefix = (settings.managed_event_prefix or "").strip()
    if managed_prefix:
        footer = f"Managed by {managed_prefix}"
        description = f"{description}\n\n---\n{footer}".strip() if description else footer

    event = {
        "summary": summary,
        "description": description,
        "location": parsed_event.get("location", ""),
        "start": parsed_event["start"],
        "end": parsed_event["end"],
        "transparency": parsed_event.get("transparency", "opaque"),
    }

    _set_bb_props(event, origin_props)
    return event
