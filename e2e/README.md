# E2E Tests for BusyBridge

Practical end-to-end tests using **Playwright** (browser automation) and **google-api-python-client** (direct Calendar API calls) against a live BusyBridge instance. These are not mocked — they exercise real sync through the real Google Calendar APIs.

## Prerequisites

- Python 3.10+
- Network access to the BusyBridge instance (default: `https://busybridge.workingpaper.co`)
- A Google OAuth `client_secret.json` placed in `e2e/auth/` (same project credentials BusyBridge uses, or a separate "Desktop" type client with Calendar API enabled)

## Setup (one-time)

### 1. Install dependencies

```bash
pip install -r e2e/requirements.txt
playwright install chromium
```

### 2. Authorize Google Calendar API access

This runs an OAuth flow for each of the three test accounts. A browser tab opens for each — sign in with the correct account and grant calendar access.

```bash
python -m e2e.auth.get_calendar_tokens
```

Tokens are saved to `e2e/auth/.calendar_tokens.json` (gitignored). They use refresh tokens so they don't expire unless revoked.

### 3. Save browser session for Playwright

This opens a Chromium window pointed at BusyBridge. Log in with the home account (`bbmaincalendar@gmail.com`), wait until you reach the dashboard, then close the browser.

```bash
python -m e2e.auth.save_auth_state
```

Session state is saved to `e2e/auth/.auth_state.json` (gitignored). **Expires after ~7 days** (BusyBridge session TTL) — re-run this script to refresh.

## Running tests

```bash
# From the e2e/ directory:
cd e2e

# All tests
pytest

# Sync tests only (Google Calendar API, no browser needed)
pytest -m api_only

# Browser/UI tests only (requires saved auth state)
pytest -m browser

# Skip slow tests (ones that wait for periodic sync up to 120s)
pytest -m "not slow"

# Verbose output
pytest -v

# Single test file
pytest tests/test_03_client_to_main.py
```

## Test accounts

| Role | Email | Purpose |
|------|-------|---------|
| Main (home) | `bbmaincalendar@gmail.com` | Logs into BusyBridge, owns the main calendar |
| Client 1 | `andrew.gruen@gmail.com` | Connected client calendar |
| Client 2 | `agyttv@gmail.com` | Connected client calendar |

Override via environment variables: `E2E_MAIN_EMAIL`, `E2E_CLIENT1_EMAIL`, `E2E_CLIENT2_EMAIL`, `E2E_BASE_URL`.

## Test inventory

| File | Scenario | Type |
|------|----------|------|
| `test_01_login.py` | Auth state works, unauthenticated redirects to login | browser |
| `test_02_dashboard.py` | Dashboard loads, shows connected calendars | browser |
| `test_03_client_to_main.py` | Client1/Client2 event → copy on main + busy block on other client (not self) | api_only |
| `test_04_main_to_client.py` | Main native event → busy blocks on both clients | api_only |
| `test_05_delete_cascade.py` | Delete from client → main copy + busy blocks removed; delete from main → busy blocks removed | api_only |
| `test_06_edit_propagation.py` | Edit summary/time on client → propagates to main copy and busy blocks | api_only |
| `test_07_settings.py` | Settings page loads, full re-sync trigger works | browser |
| `test_08_connect_disconnect.py` | Connect button visible, client-calendars API returns connected list | browser |
| `test_09_webhook_sync.py` | Speed test: sync completes within 30s (webhook timeout) | api_only |
| `test_10_logs.py` | Logs page loads, sync log API returns entries | browser |
| `test_11_all_day_sync.py` | All-day event → all-day copy on main + all-day busy block | api_only |
| `test_12_declined_skipped.py` | Declined event → no copy on main, no busy block | api_only |
| `test_13_free_allday_skipped.py` | Free/transparent all-day → copy on main but no busy block | api_only |

## How sync tests work

1. **Create** a test event on one calendar via the Google Calendar API. Every test event has a `[E2E-TEST]` prefix + UUID in its summary for isolation.
2. **Poll** the destination calendar(s) until the expected copy or busy block appears (or disappears for delete tests). Two timeout tiers:
   - **30s** for webhook-triggered sync
   - **120s** for periodic sync fallback
3. **Assert** on the synced event's properties (summary, time, all-day status, etc.).
4. **Cleanup** automatically — the `event_tracker` fixture deletes all events created during each test. A session-level fixture also sweeps any stale `[E2E-TEST]` events before and after the run.

## File structure

```
e2e/
├── __init__.py
├── config.py                       # URLs, accounts, timeouts
├── conftest.py                     # Shared fixtures
├── pytest.ini                      # Markers, test discovery
├── requirements.txt                # Dependencies
├── README.md                       # This file
├── auth/
│   ├── __init__.py
│   ├── save_auth_state.py          # One-time: save browser session
│   ├── get_calendar_tokens.py      # One-time: save API refresh tokens
│   ├── .auth_state.json            # (gitignored) browser cookies
│   ├── .calendar_tokens.json       # (gitignored) API tokens
│   └── client_secret.json          # (gitignored) Google OAuth credentials
├── helpers/
│   ├── __init__.py
│   ├── google_calendar.py          # CalendarTestClient wrapper
│   ├── sync_waiter.py              # Poll-based wait_for_event / wait_for_event_gone
│   └── event_factory.py            # Unique event names, time slots, auto-cleanup
└── tests/
    ├── __init__.py
    ├── test_01_login.py
    ├── test_02_dashboard.py
    ├── test_03_client_to_main.py
    ├── test_04_main_to_client.py
    ├── test_05_delete_cascade.py
    ├── test_06_edit_propagation.py
    ├── test_07_settings.py
    ├── test_08_connect_disconnect.py
    ├── test_09_webhook_sync.py
    ├── test_10_logs.py
    ├── test_11_all_day_sync.py
    ├── test_12_declined_skipped.py
    └── test_13_free_allday_skipped.py
```

## Troubleshooting

**"Calendar tokens not found"** — Run `python -m e2e.auth.get_calendar_tokens` first.

**"No refresh token received"** — Revoke the app's access at https://myaccount.google.com/permissions for that account, then re-run the token script.

**Browser tests skip with "No saved auth state"** — Run `python -m e2e.auth.save_auth_state` and complete the login.

**Sync tests time out** — Check that BusyBridge is running and webhooks are enabled. The periodic sync interval is 5 minutes; tests allow up to 120s by default.

**Stale test events on calendars** — Run the test suite once; the session fixture auto-cleans any `[E2E-TEST]` events. Or manually search for `[E2E-TEST]` in Google Calendar and delete them.
