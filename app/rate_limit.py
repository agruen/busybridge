"""Rate limiter shared across routers."""

from fastapi import Request
from slowapi import Limiter
from slowapi.util import get_remote_address

from app.config import get_settings


def _get_real_ip(request: Request) -> str:
    """Extract real client IP behind a reverse proxy, falling back to direct IP.

    Prefers X-Real-IP (nginx overwrites this to $remote_addr, so it can't
    be spoofed through the proxy).  Falls back to the rightmost
    X-Forwarded-For entry (the one the proxy appended), then to the
    direct connection IP.
    """
    # X-Real-IP: set by nginx to the connecting client's IP, not appendable
    real_ip = request.headers.get("X-Real-IP")
    if real_ip:
        return real_ip.strip()

    # X-Forwarded-For: client, proxy1, proxy2 — rightmost entry is the one
    # our trusted proxy appended (leftmost entries can be spoofed by the client)
    forwarded = request.headers.get("X-Forwarded-For")
    if forwarded:
        return forwarded.split(",")[-1].strip()

    return get_remote_address(request)


_settings = get_settings()
limiter = Limiter(
    key_func=_get_real_ip,
    default_limits=[f"{_settings.rate_limit_per_minute}/minute"],
)
