"""ICS/webcal feed fetcher and parser."""

import hashlib
import ipaddress
import logging
import re
import socket
from datetime import datetime, timedelta, date, timezone
from typing import Optional
from urllib.parse import urlparse

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


# ---------------------------------------------------------------------------
# SSRF protection
# ---------------------------------------------------------------------------

# Explicit blocklist for defence-in-depth (covers cloud metadata, RFC 1918,
# carrier-grade NAT, benchmarking, documentation, and broadcast ranges that
# some older Python ipaddress builds may not flag via is_private/is_reserved).
_BLOCKED_NETWORKS = [
    ipaddress.ip_network("0.0.0.0/8"),
    ipaddress.ip_network("10.0.0.0/8"),
    ipaddress.ip_network("100.64.0.0/10"),     # Carrier-grade NAT
    ipaddress.ip_network("127.0.0.0/8"),
    ipaddress.ip_network("169.254.0.0/16"),    # Link-local / cloud metadata
    ipaddress.ip_network("172.16.0.0/12"),
    ipaddress.ip_network("192.0.0.0/24"),
    ipaddress.ip_network("192.0.2.0/24"),      # TEST-NET-1
    ipaddress.ip_network("192.168.0.0/16"),
    ipaddress.ip_network("198.18.0.0/15"),     # Benchmarking
    ipaddress.ip_network("198.51.100.0/24"),   # TEST-NET-2
    ipaddress.ip_network("203.0.113.0/24"),    # TEST-NET-3
    ipaddress.ip_network("224.0.0.0/4"),       # Multicast
    ipaddress.ip_network("240.0.0.0/4"),
    ipaddress.ip_network("255.255.255.255/32"),
    # IPv6
    ipaddress.ip_network("::1/128"),
    ipaddress.ip_network("fc00::/7"),          # Unique local
    ipaddress.ip_network("fe80::/10"),         # Link-local
]


def _is_ip_blocked(ip_str: str) -> bool:
    """Return True if *ip_str* resolves to a private/reserved address."""
    try:
        addr = ipaddress.ip_address(ip_str)
    except ValueError:
        return True  # Unparseable → block

    # Built-in checks (Python 3.12 covers RFC 6890 correctly).
    if (
        addr.is_private
        or addr.is_reserved
        or addr.is_loopback
        or addr.is_link_local
        or addr.is_multicast
    ):
        return True

    # Explicit network list for defence-in-depth.
    for network in _BLOCKED_NETWORKS:
        if addr in network:
            return True

    return False


def validate_url_for_ssrf(url: str) -> None:
    """Raise ``ValueError`` if *url* targets a private/reserved network.

    Resolves the hostname via DNS and checks every returned address.
    """
    parsed = urlparse(url)
    hostname = parsed.hostname

    if not hostname:
        raise ValueError("URL has no hostname")

    # Reject raw-IP URLs that point at internal ranges.
    try:
        if _is_ip_blocked(hostname):
            raise ValueError("URL points to a blocked address")
    except ValueError:
        pass  # Not a literal IP — continue to DNS resolution.

    try:
        addrinfos = socket.getaddrinfo(
            hostname, None, socket.AF_UNSPEC, socket.SOCK_STREAM,
        )
    except socket.gaierror:
        raise ValueError(f"Cannot resolve hostname: {hostname}")

    if not addrinfos:
        raise ValueError(f"Cannot resolve hostname: {hostname}")

    for _family, _type, _proto, _canonname, sockaddr in addrinfos:
        ip = sockaddr[0]
        if _is_ip_blocked(ip):
            raise ValueError("URL resolves to a blocked address")


async def fetch_ics_feed(
    url: str,
    etag: Optional[str] = None,
    timeout: float = 30.0,
) -> tuple[Optional[str], Optional[str]]:
    """Fetch an ICS feed from a URL.

    Returns (ics_content, new_etag) or (None, None) if 304 Not Modified.
    Raises ``ValueError`` if the URL targets a private/internal network.
    """
    # Normalize webcal:// to https://
    if url.startswith("webcal://"):
        url = "https://" + url[len("webcal://"):]

    # SSRF protection: block requests to internal/private networks.
    validate_url_for_ssrf(url)

    headers = {}
    if etag:
        headers["If-None-Match"] = etag

    async with httpx.AsyncClient(follow_redirects=True, timeout=timeout) as client:
        response = await client.get(url, headers=headers)

    # Re-validate after redirects — the final URL may differ from the
    # original (e.g. an external host 302-ing to an internal IP).
    final_url = str(response.url)
    if final_url != url:
        validate_url_for_ssrf(final_url)

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
