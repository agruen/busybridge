"""ICS calendar export for backups.

Creates two ZIP files per backup cycle:
  - ics-full-<timestamp>.zip   : all events from every calendar as ICS
  - ics-clean-<timestamp>.zip  : same but with BusyBridge-managed events removed

Each ZIP contains one .ics file per calendar.
ICS is generated from events fetched via the Google Calendar API.
"""

import logging
import os
import re
import zipfile
from collections import defaultdict
from datetime import datetime
from typing import Optional

from app.config import get_settings
from app.database import get_database

logger = logging.getLogger(__name__)

ICS_FULL_PREFIX = "ics-full-"
ICS_CLEAN_PREFIX = "ics-clean-"


# ---------------------------------------------------------------------------
# ICS generation helpers
# ---------------------------------------------------------------------------

def _escape_ics(text: str) -> str:
    """Escape text for ICS format."""
    text = text.replace("\\", "\\\\")
    text = text.replace(";", "\\;")
    text = text.replace(",", "\\,")
    text = text.replace("\n", "\\n")
    return text


def _fold_line(line: str) -> str:
    """Fold long lines per RFC 5545 (max 75 octets)."""
    if len(line.encode("utf-8")) <= 75:
        return line
    parts = []
    current = ""
    for ch in line:
        candidate = current + ch
        if len(candidate.encode("utf-8")) > 75:
            parts.append(current)
            current = " " + ch
        else:
            current = candidate
    if current:
        parts.append(current)
    return "\r\n".join(parts)


def _format_dt(dt_dict: dict, prop_name: str) -> Optional[str]:
    """Convert a Google Calendar start/end dict to an ICS line."""
    if "date" in dt_dict:
        d = dt_dict["date"].replace("-", "")
        return f"{prop_name};VALUE=DATE:{d}"
    elif "dateTime" in dt_dict:
        raw = dt_dict["dateTime"]
        tz = dt_dict.get("timeZone")
        if raw.endswith("Z"):
            clean = re.sub(r"[:\-]", "", raw.replace("Z", "")) + "Z"
            return f"{prop_name}:{clean}"
        dt_part = re.sub(r"[+\-]\d{2}:\d{2}$", "", raw)
        clean = re.sub(r"[:\-]", "", dt_part)
        if tz:
            return f"{prop_name};TZID={tz}:{clean}"
        return f"{prop_name}:{clean}Z"
    return None


def _format_dt_value(dt_dict: dict) -> Optional[str]:
    """Return just the formatted value (no property name) for EXDATE use."""
    if "date" in dt_dict:
        return dt_dict["date"].replace("-", "")
    elif "dateTime" in dt_dict:
        raw = dt_dict["dateTime"]
        tz = dt_dict.get("timeZone")
        if raw.endswith("Z"):
            return re.sub(r"[:\-]", "", raw.replace("Z", "")) + "Z"
        dt_part = re.sub(r"[+\-]\d{2}:\d{2}$", "", raw)
        clean = re.sub(r"[:\-]", "", dt_part)
        if tz:
            return f"TZID={tz}:{clean}"
        return clean + "Z"
    return None


def _clean_timestamp(ts: str) -> str:
    """Convert Google timestamp to ICS format."""
    return re.sub(r"[:\-]", "", ts.replace(".000Z", "Z").replace(".000", ""))


def _build_conference_description(event: dict) -> str:
    """Build the Google Meet / conference block for the description."""
    conf = event.get("conferenceData")
    if not conf:
        return ""

    parts = []
    separator = "-::~:~::~:~:~:~:~:~:~:~:~:~:~:~:~:~:~:~:~:~:~:~:~:~:~:~:~:~:~:~:~:~:~:~:~:~:~:~::~:~::-"

    # Find the video entry point
    entry_points = conf.get("entryPoints", [])
    video_uri = None
    phone_entries = []

    for ep in entry_points:
        ep_type = ep.get("entryPointType")
        if ep_type == "video":
            video_uri = ep.get("uri", "")
        elif ep_type == "phone":
            label = ep.get("label", "")
            pin = ep.get("pin", "")
            region = ep.get("regionCode", "")
            phone_str = f"({region}) {label}" if region else label
            if pin:
                phone_str += f" PIN: {pin}#"
            phone_entries.append(phone_str)
        elif ep_type == "more":
            # "More phone numbers" link
            phone_entries.append(f"More phone numbers: {ep.get('uri', '')}")

    if video_uri or phone_entries:
        parts.append(separator)
        if video_uri:
            # Determine provider name
            provider = conf.get("conferenceSolution", {}).get("name", "Google Meet")
            parts.append(f"Join with {provider}: {video_uri}")
        for phone in phone_entries:
            if phone.startswith("More phone"):
                parts.append(phone)
            else:
                parts.append(f"Or dial: {phone}")
        notes = conf.get("notes", "")
        if notes:
            parts.append(notes)
        parts.append(f"Learn more about Meet at: https://support.google.com/a/users/answer/9282720")
        parts.append("")
        parts.append("Please do not edit this section.")
        parts.append(separator)

    return "\n".join(parts)


def _build_reminders(event: dict) -> list[str]:
    """Build VALARM blocks from Google Calendar reminder data."""
    reminders = event.get("reminders", {})
    alarms = []

    if reminders.get("useDefault"):
        # Default reminders are calendar-level, not event-level.
        # We can't know what they are from the event data alone,
        # so emit a common default (10 min popup).
        alarms.append("\r\n".join([
            "BEGIN:VALARM",
            "ACTION:DISPLAY",
            "DESCRIPTION:Reminder",
            "TRIGGER:-PT10M",
            "END:VALARM",
        ]))
    else:
        for override in reminders.get("overrides", []):
            method = override.get("method", "popup")
            minutes = override.get("minutes", 10)
            action = "EMAIL" if method == "email" else "DISPLAY"
            alarms.append("\r\n".join([
                "BEGIN:VALARM",
                f"ACTION:{action}",
                "DESCRIPTION:Reminder",
                f"TRIGGER:-PT{minutes}M",
                "END:VALARM",
            ]))

    return alarms


# Google Calendar colorId to hex mapping
_COLOR_MAP = {
    "1": "#7986CB",   # Lavender
    "2": "#33B679",   # Sage
    "3": "#8E24AA",   # Grape
    "4": "#E67C73",   # Flamingo
    "5": "#F6BF26",   # Banana
    "6": "#F4511E",   # Tangerine
    "7": "#039BE5",   # Peacock
    "8": "#616161",   # Graphite
    "9": "#3F51B5",   # Blueberry
    "10": "#0B8043",  # Basil
    "11": "#D50000",  # Tomato
}


def _event_to_vevent(event: dict, exdates: list[str] = None) -> Optional[str]:
    """Convert a Google Calendar event dict to a VEVENT block.

    Args:
        event: Google Calendar event dict
        exdates: List of pre-formatted EXDATE values for cancelled recurring instances
    """
    if event.get("status") == "cancelled":
        return None

    lines = ["BEGIN:VEVENT"]

    # UID — for edited recurring instances, use parent ID so they link together
    recurring_event_id = event.get("recurringEventId")
    if recurring_event_id:
        lines.append(_fold_line(f"UID:{recurring_event_id}"))
    else:
        lines.append(_fold_line(f"UID:{event.get('id', '')}"))

    # Summary
    summary = event.get("summary", "(No title)")
    lines.append(_fold_line(f"SUMMARY:{_escape_ics(summary)}"))

    # Start / End
    start = event.get("start", {})
    end = event.get("end", {})
    start_line = _format_dt(start, "DTSTART")
    end_line = _format_dt(end, "DTEND")
    if start_line:
        lines.append(_fold_line(start_line))
    if end_line:
        lines.append(_fold_line(end_line))

    # RECURRENCE-ID for edited instances of recurring events
    original_start = event.get("originalStartTime")
    if original_start and recurring_event_id:
        recurrence_id_line = _format_dt(original_start, "RECURRENCE-ID")
        if recurrence_id_line:
            lines.append(_fold_line(recurrence_id_line))

    # Location
    location = event.get("location")
    if location:
        lines.append(_fold_line(f"LOCATION:{_escape_ics(location)}"))

    # Description — Google already embeds Meet info in the description
    # with -::~:~:: separators. Only build from conferenceData if missing.
    description = event.get("description", "") or ""
    separator = "-::~:~::~:~:~:~:~:~:~:~:~:~:~:~:~:~:~:~:~:~:~:~:~:~:~:~:~:~:~:~:~:~:~:~:~:~:~:~::~:~::-"
    if separator not in description:
        conference_block = _build_conference_description(event)
        if conference_block:
            if description:
                description = description + "\n\n" + conference_block
            else:
                description = conference_block
    if description:
        lines.append(_fold_line(f"DESCRIPTION:{_escape_ics(description)}"))

    # Status
    status_val = (event.get("status") or "confirmed").upper()
    lines.append(f"STATUS:{status_val}")

    # Transparency
    transparency = (event.get("transparency") or "opaque").upper()
    lines.append(f"TRANSP:{transparency}")

    # Visibility
    visibility = event.get("visibility")
    if visibility and visibility != "default":
        vis_map = {"public": "PUBLIC", "private": "PRIVATE", "confidential": "CONFIDENTIAL"}
        ics_vis = vis_map.get(visibility)
        if ics_vis:
            lines.append(f"CLASS:{ics_vis}")

    # Recurrence rules
    for rule in event.get("recurrence", []):
        lines.append(_fold_line(rule))

    # EXDATE — cancelled instances of this recurring event
    if exdates:
        for exdate in exdates:
            lines.append(_fold_line(f"EXDATE:{exdate}"))

    # Color
    color_id = event.get("colorId")
    if color_id:
        hex_color = _COLOR_MAP.get(color_id, "")
        if hex_color:
            lines.append(f"X-APPLE-CALENDAR-COLOR:{hex_color}")
            lines.append(f"COLOR:{hex_color}")

    # Attendees
    for attendee in event.get("attendees", []):
        email = attendee.get("email", "")
        name = attendee.get("displayName", "")
        rsvp = attendee.get("responseStatus", "needsAction")
        partstat_map = {
            "accepted": "ACCEPTED",
            "declined": "DECLINED",
            "tentative": "TENTATIVE",
            "needsAction": "NEEDS-ACTION",
        }
        partstat = partstat_map.get(rsvp, "NEEDS-ACTION")
        parts = [f"PARTSTAT={partstat}"]
        if name:
            parts.append(f"CN={_escape_ics(name)}")
        if attendee.get("organizer"):
            parts.append("ROLE=CHAIR")
        elif attendee.get("optional"):
            parts.append("ROLE=OPT-PARTICIPANT")
        else:
            parts.append("ROLE=REQ-PARTICIPANT")
        if attendee.get("self"):
            parts.append("X-NUM-GUESTS=0")
        lines.append(_fold_line(f"ATTENDEE;{';'.join(parts)}:mailto:{email}"))

    # Organizer
    organizer = event.get("organizer", {})
    if organizer.get("email"):
        org_name = organizer.get("displayName", "")
        if org_name:
            lines.append(_fold_line(
                f"ORGANIZER;CN={_escape_ics(org_name)}:mailto:{organizer['email']}"
            ))
        else:
            lines.append(_fold_line(f"ORGANIZER:mailto:{organizer['email']}"))

    # Guest permissions (Google-specific X-properties)
    if event.get("guestsCanModify") is not None:
        lines.append(f"X-GOOGLE-GUESTS-CAN-MODIFY:{str(event['guestsCanModify']).upper()}")
    if event.get("guestsCanInviteOthers") is not None:
        lines.append(f"X-GOOGLE-GUESTS-CAN-INVITE-OTHERS:{str(event['guestsCanInviteOthers']).upper()}")
    if event.get("guestsCanSeeOtherGuests") is not None:
        lines.append(f"X-GOOGLE-GUESTS-CAN-SEE-OTHER-GUESTS:{str(event['guestsCanSeeOtherGuests']).upper()}")

    # Event type (out-of-office, focus time, working location)
    event_type = event.get("eventType")
    if event_type and event_type != "default":
        lines.append(f"X-GOOGLE-EVENT-TYPE:{event_type}")

    # Source URL
    source = event.get("source", {})
    if source.get("url"):
        lines.append(_fold_line(f"URL:{source['url']}"))

    # HTML link
    html_link = event.get("htmlLink")
    if html_link:
        lines.append(_fold_line(f"X-GOOGLE-CALENDAR-LINK:{html_link}"))

    # Attachments
    for attachment in event.get("attachments", []):
        file_url = attachment.get("fileUrl", "")
        title = attachment.get("title", "")
        mime_type = attachment.get("mimeType", "")
        if file_url:
            attach_parts = []
            if mime_type:
                attach_parts.append(f"FMTTYPE={mime_type}")
            if title:
                attach_parts.append(f"FILENAME={_escape_ics(title)}")
            if attach_parts:
                lines.append(_fold_line(f"ATTACH;{';'.join(attach_parts)}:{file_url}"))
            else:
                lines.append(_fold_line(f"ATTACH:{file_url}"))

    # Timestamps
    created = event.get("created", "")
    if created:
        lines.append(f"CREATED:{_clean_timestamp(created)}")

    updated = event.get("updated", "")
    if updated:
        ts = _clean_timestamp(updated)
        lines.append(f"LAST-MODIFIED:{ts}")
        lines.append(f"DTSTAMP:{ts}")

    # Sequence number
    sequence = event.get("sequence")
    if sequence is not None:
        lines.append(f"SEQUENCE:{sequence}")

    # Reminders / alarms
    for alarm in _build_reminders(event):
        lines.append(alarm)

    lines.append("END:VEVENT")
    return "\r\n".join(lines)


def _events_to_ics(events: list[dict], calendar_name: str) -> str:
    """Convert a list of Google Calendar events to an ICS string.

    Handles cancelled recurring instances by converting them to EXDATE
    entries on the parent event.
    """
    header = "\r\n".join([
        "BEGIN:VCALENDAR",
        "VERSION:2.0",
        "PRODID:-//BusyBridge//Calendar Export//EN",
        f"X-WR-CALNAME:{_escape_ics(calendar_name)}",
        "CALSCALE:GREGORIAN",
        "METHOD:PUBLISH",
    ])

    # Collect cancelled instances to build EXDATE entries per parent event
    exdates_by_parent: dict[str, list[str]] = defaultdict(list)
    active_events = []

    for event in events:
        if event.get("status") == "cancelled":
            parent_id = event.get("recurringEventId")
            original_start = event.get("originalStartTime")
            if parent_id and original_start:
                exdate_val = _format_dt_value(original_start)
                if exdate_val:
                    exdates_by_parent[parent_id].append(exdate_val)
        else:
            active_events.append(event)

    vevents = []
    for event in active_events:
        event_id = event.get("id", "")
        exdates = exdates_by_parent.get(event_id, [])
        vevent = _event_to_vevent(event, exdates=exdates)
        if vevent:
            vevents.append(vevent)

    footer = "END:VCALENDAR"
    parts = [header]
    if vevents:
        parts.extend(vevents)
    parts.append(footer)
    return "\r\n".join(parts) + "\r\n"


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _safe_filename(name: str) -> str:
    """Make a string safe for use as a filename."""
    name = re.sub(r'[<>:"/\\|?*]', '_', name)
    name = name.strip('. ')
    return name or "calendar"


def _is_busybridge_event(event: dict) -> bool:
    """Check if an event is BusyBridge-managed."""
    settings = get_settings()

    ext_props = event.get("extendedProperties", {})
    private_props = ext_props.get("private", {})
    if private_props.get(settings.calendar_sync_tag) == "true":
        return True

    prefix = (settings.managed_event_prefix or "").strip().lower()
    if prefix:
        summary = (event.get("summary") or "").strip().lower()
        if summary.startswith(prefix):
            return True

    return False


# ---------------------------------------------------------------------------
# Fetch events from all calendars
# ---------------------------------------------------------------------------

async def _fetch_all_user_calendars(user_id: int) -> list[dict]:
    """Fetch all events from all calendars for a user via the Calendar API.

    Returns list of dicts with: calendar_name, events
    Events include cancelled instances (needed for EXDATE generation).
    """
    from app.auth.google import get_valid_access_token
    from app.sync.google_calendar import GoogleCalendarClient

    db = await get_database()
    settings = get_settings()

    cursor = await db.execute("SELECT * FROM users WHERE id = ?", (user_id,))
    user = await cursor.fetchone()
    if not user or not user["main_calendar_id"]:
        return []

    results = []

    # Full export — wide time range to capture everything
    time_min = datetime(2000, 1, 1)
    time_max = datetime(2100, 1, 1)

    # Main calendar
    try:
        token = await get_valid_access_token(user_id, user["email"])
        client = GoogleCalendarClient(token, settings=settings)
        logger.info(f"ICS export: fetching main calendar {user['main_calendar_id']}")
        resp = client.list_events(
            user["main_calendar_id"],
            single_events=False,
            time_min=time_min,
            time_max=time_max,
        )
        # Keep ALL events including cancelled (needed for EXDATE)
        events = resp.get("events", [])
        logger.info(f"ICS export: main calendar returned {len(events)} events")
        results.append({
            "calendar_name": f"Main - {user['email']}",
            "events": events,
        })
    except Exception as e:
        logger.error(f"ICS export: main calendar failed for user {user_id}: {e}")

    # Client calendars
    cursor = await db.execute(
        """SELECT cc.id, cc.google_calendar_id, cc.display_name,
                  ot.google_account_email
           FROM client_calendars cc
           JOIN oauth_tokens ot ON cc.oauth_token_id = ot.id
           WHERE cc.user_id = ? AND cc.is_active = TRUE""",
        (user_id,),
    )
    client_cals = await cursor.fetchall()

    for cal in client_cals:
        try:
            token = await get_valid_access_token(user_id, cal["google_account_email"])
            client = GoogleCalendarClient(token, settings=settings)
            logger.info(f"ICS export: fetching client calendar {cal['google_calendar_id']}")
            resp = client.list_events(
                cal["google_calendar_id"],
                single_events=False,
                time_min=datetime(2000, 1, 1),
                time_max=datetime(2100, 1, 1),
            )
            # Keep ALL events including cancelled (needed for EXDATE)
            events = resp.get("events", [])
            display = cal["display_name"] or cal["google_account_email"]
            logger.info(f"ICS export: {display} returned {len(events)} events")
            results.append({
                "calendar_name": f"Client - {display}",
                "events": events,
            })
        except Exception as e:
            logger.error(f"ICS export: client calendar {cal['id']} failed: {e}")

    return results


# ---------------------------------------------------------------------------
# ICS backup creation
# ---------------------------------------------------------------------------

def get_ics_backup_dir() -> str:
    """Return the ICS backup directory, creating it if necessary."""
    base = os.environ.get("BACKUP_PATH", "/data/backups")
    path = os.path.join(base, "ics")
    os.makedirs(path, exist_ok=True)
    return path


async def create_ics_backup() -> dict:
    """Create ICS export ZIPs (full + clean) for all users.

    Returns metadata dict with backup IDs and event counts.
    """
    db = await get_database()
    now = datetime.now()
    timestamp = now.strftime("%Y%m%d-%H%M%S")

    full_id = f"{ICS_FULL_PREFIX}{timestamp}"
    clean_id = f"{ICS_CLEAN_PREFIX}{timestamp}"

    ics_dir = get_ics_backup_dir()
    full_path = os.path.join(ics_dir, f"{full_id}.zip")
    clean_path = os.path.join(ics_dir, f"{clean_id}.zip")

    cursor = await db.execute(
        "SELECT * FROM users WHERE main_calendar_id IS NOT NULL"
    )
    users = await cursor.fetchall()

    total_calendars = 0
    total_full = 0
    total_clean = 0
    errors = []

    with zipfile.ZipFile(full_path, "w", zipfile.ZIP_DEFLATED) as zf_full, \
         zipfile.ZipFile(clean_path, "w", zipfile.ZIP_DEFLATED) as zf_clean:

        for user in users:
            try:
                calendars = await _fetch_all_user_calendars(user["id"])
            except Exception as e:
                errors.append(f"user {user['id']}: {e}")
                continue

            for cal in calendars:
                total_calendars += 1
                events = cal["events"]
                name = _safe_filename(cal["calendar_name"])
                filename = f"{name}.ics"

                # Full ICS — all events
                full_ics = _events_to_ics(events, cal["calendar_name"])
                zf_full.writestr(filename, full_ics)
                active_count = sum(1 for e in events if e.get("status") != "cancelled")
                total_full += active_count

                # Clean ICS — BusyBridge events removed
                clean_events = [e for e in events if not _is_busybridge_event(e)]
                clean_ics = _events_to_ics(clean_events, cal["calendar_name"])
                zf_clean.writestr(filename, clean_ics)
                clean_active = sum(1 for e in clean_events if e.get("status") != "cancelled")
                total_clean += clean_active

    metadata = {
        "created_at": now.isoformat(),
        "timestamp": timestamp,
        "full_backup_id": full_id,
        "clean_backup_id": clean_id,
        "total_calendars": total_calendars,
        "total_events_full": total_full,
        "total_events_clean": total_clean,
        "events_filtered": total_full - total_clean,
        "full_size_bytes": os.path.getsize(full_path),
        "clean_size_bytes": os.path.getsize(clean_path),
        "errors": errors,
    }

    logger.info(
        f"ICS backup created: {total_calendars} calendars, "
        f"{total_full} events full / {total_clean} clean "
        f"({total_full - total_clean} filtered)"
    )

    return metadata


# ---------------------------------------------------------------------------
# Listing and deletion
# ---------------------------------------------------------------------------

def list_ics_backups() -> list[dict]:
    """List all ICS backup pairs, newest first."""
    ics_dir = get_ics_backup_dir()
    if not os.path.exists(ics_dir):
        return []

    timestamps: dict[str, dict] = {}

    for fname in os.listdir(ics_dir):
        if not fname.endswith(".zip"):
            continue
        fpath = os.path.join(ics_dir, fname)
        backup_id = fname[:-len(".zip")]

        if backup_id.startswith(ICS_FULL_PREFIX):
            ts = backup_id[len(ICS_FULL_PREFIX):]
            timestamps.setdefault(ts, {})["full_id"] = backup_id
            timestamps[ts]["full_size"] = os.path.getsize(fpath)
        elif backup_id.startswith(ICS_CLEAN_PREFIX):
            ts = backup_id[len(ICS_CLEAN_PREFIX):]
            timestamps.setdefault(ts, {})["clean_id"] = backup_id
            timestamps[ts]["clean_size"] = os.path.getsize(fpath)

    results = []
    for ts in sorted(timestamps.keys(), reverse=True):
        entry = timestamps[ts]
        try:
            dt = datetime.strptime(ts, "%Y%m%d-%H%M%S")
            created_at = dt.isoformat()
        except ValueError:
            created_at = ts

        results.append({
            "timestamp": ts,
            "created_at": created_at,
            "full_backup_id": entry.get("full_id"),
            "clean_backup_id": entry.get("clean_id"),
            "full_size_bytes": entry.get("full_size", 0),
            "clean_size_bytes": entry.get("clean_size", 0),
        })

    return results


def delete_ics_backup(timestamp: str) -> bool:
    """Delete an ICS backup pair by timestamp."""
    ics_dir = get_ics_backup_dir()
    deleted = False
    for prefix in [ICS_FULL_PREFIX, ICS_CLEAN_PREFIX]:
        path = os.path.join(ics_dir, f"{prefix}{timestamp}.zip")
        if os.path.exists(path):
            os.remove(path)
            deleted = True
    if deleted:
        logger.info(f"Deleted ICS backup pair: {timestamp}")
    return deleted


def apply_ics_retention_policy() -> dict:
    """Enforce same 7 daily / 2 weekly / 6 monthly retention for ICS backups."""
    from app.sync.backup import _classify_backup

    limits = {"daily": 7, "weekly": 2, "monthly": 6}
    backups = list_ics_backups()

    by_type: dict[str, list] = {"daily": [], "weekly": [], "monthly": []}
    for b in backups:
        try:
            dt = datetime.strptime(b["timestamp"], "%Y%m%d-%H%M%S")
            btype = _classify_backup(dt)
        except ValueError:
            btype = "daily"
        by_type.setdefault(btype, []).append(b)

    to_delete = []
    kept = []

    for btype, limit in limits.items():
        for i, entry in enumerate(by_type.get(btype, [])):
            if i < limit:
                kept.append(entry["timestamp"])
            else:
                to_delete.append(entry["timestamp"])

    for ts in to_delete:
        delete_ics_backup(ts)

    return {"kept": kept, "deleted": to_delete}
