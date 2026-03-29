# BusyBridge

A self-hosted calendar synchronization service for consulting organizations. Connect multiple client calendars to a single main calendar, keeping availability in sync without exposing details across clients.

## Features

- **Bidirectional Sync**: Client calendar events appear on your main calendar with full details; your main calendar events appear as "Busy" blocks on client calendars
- **Personal Calendar Sync**: Connect personal Gmail/Workspace calendars as read-only sources that create privacy-preserving "Busy (Personal)" blocks across all calendars
- **Webcal/ICS Subscriptions**: Subscribe to external ICS feeds (e.g. conference schedules, travel itineraries) that sync to your main calendar with busy blocks on clients
- **Recurring Events**: Full fidelity for recurring events including single-instance modifications and cancellations
- **Smart Busy Blocks**: Only creates blocks for events that actually block time (respects Free/Busy status)
- **RSVP Propagation**: Accept/decline on main calendar propagates back to the client calendar
- **Calendar Color Coding**: Assign Google Calendar colors to each client calendar; events are color-coded on your main calendar
- **Webhook Integration**: Real-time sync via Google Calendar push notifications (5-second debounce)
- **Rate Limiting**: Token-bucket rate limiter (5 req/s) with exponential backoff prevents Google API quota exhaustion
- **Service Account Mode**: Optional SA creates main calendar events, making non-editable events physically immovable
- **ICS Calendar Export**: Full calendar export to ICS format with a "clean" variant that strips BusyBridge-managed events (for migration or external backup)
- **Email Alerts**: Notifications for sync failures, token revocations, integrity issues
- **Automated Backups**: Daily database + ICS backups with 7-daily/2-weekly/6-monthly retention
- **Self-Healing**: Hourly consistency checks, 6-hourly orphan scans, automatic retry of missing busy blocks, circuit breaker auto-pauses sync when all calendars fail consecutively
- **Sync Control**: Full re-sync, per-calendar cleanup & re-sync, global cleanup & pause, live progress tracking
- **Admin Dashboard**: User management, system health, sync activity feed, log viewer, factory reset

## Quick Start

### Prerequisites

1. Docker and Docker Compose
2. A Google Cloud project with Calendar API enabled and OAuth 2.0 credentials
3. A domain with HTTPS (for webhooks and OAuth callbacks)

### Installation

```bash
git clone <repository-url>
cd busybridge
mkdir -p data secrets
```

Create a `.env` file:

```bash
FQDN=your-domain.com
MANAGED_EVENT_PREFIX=[BusyBridge]
ENABLE_WEBHOOKS=true
# SERVICE_ACCOUNT_KEY_FILE=/secrets/sa-key.json  # optional
```

Start the service:

```bash
docker compose up -d
```

Access the setup wizard at `https://your-domain.com`. The wizard walks through 7 steps:

1. **Welcome** -- overview and prerequisites
2. **Google Cloud Credentials** -- guided walkthrough to create OAuth credentials
3. **Admin Authentication** -- sign in with Google, establishes home org domain
4. **Email Alerts** -- optional SMTP configuration for sync failure notifications
5. **Service Account** -- optional SA key upload for immovable events
6. **Encryption Key** -- generates master key (save it!), initializes database
7. **Complete** -- next steps and link to dashboard

### Google Cloud Setup

1. Go to [Google Cloud Console](https://console.cloud.google.com/)
2. Create a project and enable the **Google Calendar API**
3. Configure the OAuth consent screen:
   - Scopes: `calendar`, `calendar.readonly`, `email`, `profile`, `openid`
4. Create OAuth 2.0 credentials (Web application) with redirect URIs:
   - `https://your-domain/auth/callback`
   - `https://your-domain/auth/connect-client/callback`
   - `https://your-domain/auth/connect-personal/callback`
   - `https://your-domain/setup/step/3/callback`

## How It Works

Your **main calendar is your single source of truth**. Client calendars are managed automatically.

### Creating Appointments

**Create on your main calendar** for personal blocks, internal meetings, or any appointment you own. BusyBridge creates "Busy" blocks on every connected client calendar:

```
You create event on Main Calendar
         |
BusyBridge creates "Busy" blocks on Client A, B, C
```

**Create on a client calendar** when the invite should come from that client's domain. BusyBridge copies it to your main calendar and creates "Busy" blocks on other clients:

```
You create event on Client A
         |
Full-detail copy on Main Calendar
"Busy" blocks on Client B, C (not A -- it has the real event)
```

### Personal Calendar Events

Personal calendars are **read-only**. Events create privacy-preserving blocks:

```
Personal calendar event ("Doctor Appointment")
         |
"Busy (Personal)" block on Main Calendar
"Busy (Personal)" blocks on Client A, B, C
```

No event details are shared.

### Webcal/ICS Subscriptions

Subscribe to external calendar feeds (conference schedules, travel itineraries). Events sync to your main calendar and create busy blocks on client calendars. Feeds with unstable UIDs (e.g. ISO) are handled via content-based hashing.

### Recurring Events

Recurring events sync as recurring events -- BusyBridge copies the RRULE directly. Single-instance modifications and cancellations sync individually without affecting the series.

### RSVP Propagation

When you accept or decline an event on your main calendar that originated from a client calendar, BusyBridge propagates the RSVP status back to the client calendar.

### Event Markers

BusyBridge-managed events are visually marked:

- **Lock icon** (🔒) in the title indicates a non-editable managed event
- **Color coding** distinguishes events from different client calendars
- **"Managed by [BusyBridge]"** appears in the description footer
- **Edit protection**: if you move a non-editable event on your main calendar, BusyBridge reverts it to the original time within one sync cycle

### When Clients Schedule You

Client-created events sync automatically to your main calendar with full details. Other clients only see "Busy" blocks -- no cross-client information is ever shared.

| Action | Where |
|--------|-------|
| Create a personal/internal appointment | Your **main calendar** |
| Organize a meeting within a client's domain | That **client's calendar** |
| Block personal time across all calendars | Connect a **personal calendar** |
| Subscribe to a conference schedule | Add a **webcal subscription** |
| View your full schedule | Your **main calendar** |
| Accept/decline a client meeting | Your **main calendar** (RSVP propagates back) |

## Dashboard

The main dashboard (`/app`) shows:

- **Status grid**: connected calendars, total events synced, healthy count, issues
- **Integrity checker**: live consistency status with auto-fix tracking
- **Main calendar**: current selection with quick-change link
- **Client calendars**: each with status icon, color dot picker, last sync time, event/busy block counts, per-calendar sync button with live progress bar, and actions (cleanup & re-sync, disconnect)
- **Personal calendars**: status, last sync, busy block counts, sync/disconnect
- **Webcal subscriptions**: add new feeds (URL + display prefix), status, sync/delete
- **Sync activity feed**: real-time log of sync events

### Sync Control

The sync control page (`/app/settings/sync`) provides:

- **Full Re-sync**: Clear all sync tokens and re-process every event from scratch
- **Cleanup & Re-sync**: Delete ALL BusyBridge-managed events, then recreate from scratch
- **Cleanup & Pause**: Remove all managed events and stop syncing (for troubleshooting)
- **Connection Health Check**: Test all OAuth tokens and show which accounts need reconnection
- **Live progress tracking** with step labels and event counts during cleanup operations

Per-calendar cleanup is also available from the dashboard (cleanup & re-sync a single calendar without affecting others).

### Calendar Exports

The exports page (`/app/settings/exports`) manages ICS calendar backups:

- **Full export**: ZIP containing one `.ics` file per calendar with complete event data (attendees, Meet links, recurrence, etc.)
- **Clean export**: Same but with BusyBridge-managed events filtered out -- useful for migrating away or importing into another calendar tool
- Automatic daily exports alongside database backups
- Same retention policy: 7 daily, 2 weekly, 6 monthly

## Architecture

Single Docker container running:

- **FastAPI** web server (HTTP, OAuth, webhooks, UI)
- **APScheduler** background jobs (sync, cleanup, maintenance)
- **SQLite** database (all configuration and sync state)
- **Rate-limited Google Calendar API client** (token-bucket, 5 req/s)

### Sync Flow

```
Client Calendar Event --> Main Calendar (full details)
                      --> Busy blocks on other Client Calendars

Main Calendar Event   --> Busy blocks on ALL Client Calendars

Personal Calendar     --> "Busy (Personal)" on Main + all Clients

Webcal/ICS Feed       --> Events on Main Calendar
                      --> Busy blocks on all Client Calendars
```

### Loop Prevention

All BusyBridge-created events are tagged with `extendedProperties.private.calendarSyncEngine = "true"`. The sync engine skips any event with this tag, preventing feedback loops.

### Key Directories

```
app/
  auth/         OAuth, sessions, service account
  api/          REST API endpoints
  sync/         Core sync engine, rules, Google API wrapper
  jobs/         Background job definitions
  alerts/       Email alerting
  ui/           Web interface and Jinja2 templates
```

## Configuration

### Environment Variables

| Variable | Description | Default |
|----------|-------------|---------|
| `FQDN` | Domain name (used to construct `PUBLIC_URL`) | `localhost:3000` |
| `DATABASE_PATH` | Path to SQLite database | `/data/calendar-sync.db` |
| `ENCRYPTION_KEY_FILE` | Path to encryption key file | `/secrets/encryption.key` |
| `LOG_LEVEL` | Logging level | `info` |
| `TZ` | Timezone for scheduled jobs | `UTC` |
| `ENABLE_WEBHOOKS` | Enable Google Calendar push notifications | `true` |
| `MANAGED_EVENT_PREFIX` | Prefix on BusyBridge-created events | `[BusyBridge]` |
| `SERVICE_ACCOUNT_KEY_FILE` | Path to Google SA JSON key (optional) | _(none)_ |
| `TEST_MODE` | Enable Gmail-safe testing mode | `false` |
| `TEST_MODE_ALLOWED_HOME_EMAILS` | Email allowlist for home login in test mode | _(none)_ |
| `TEST_MODE_ALLOWED_CLIENT_EMAILS` | Email allowlist for client connections in test mode | _(none)_ |

Google OAuth credentials and SMTP settings are stored in the database after the setup wizard, not in environment variables.

### Service Account Mode

When a service account is configured, BusyBridge creates main calendar events using the SA's credentials. This makes non-editable events (events you don't organize) physically immovable in Google Calendar, because the SA is the organizer.

| `sa_tier` | Mode | Behavior |
|-----------|------|----------|
| 0 | Fallback | User token creates events; lock emoji + time revert for non-editable |
| 2 | SA as organizer | SA creates events; non-editable events are natively immovable |

**Setup:**

1. Create a service account in Google Cloud (no domain-wide delegation needed)
2. Download the JSON key to `secrets/sa-key.json`
3. Set `SERVICE_ACCOUNT_KEY_FILE=/secrets/sa-key.json` in `.env`
4. Share each user's main calendar with the SA email ("Make changes to events")
5. Activate via Admin > Service Account in the dashboard

### Scheduled Jobs

| Job | Frequency | Description |
|-----|-----------|-------------|
| Periodic Sync | Every 5 min | Poll all calendars + webcal subscriptions |
| Webhook Renewal | Every 6 hours | Renew expiring push notification channels |
| Consistency Check | Every hour | Verify database matches Google Calendar reality |
| Orphan Scan | Every 6 hours | Find and remove orphaned events on Google |
| Token Refresh | Every 30 min | Proactively refresh expiring OAuth tokens |
| Alert Processing | Every 1 min | Send queued email alerts |
| Retention Cleanup | Daily 3 AM | Delete old records per retention policy |
| Stale Alert Cleanup | Daily 4 AM | Remove old sent/failed alert records |
| Daily Backup | Daily 11 PM | Create backup, enforce retention policy |

## Backup & Recovery

BusyBridge creates daily automated backups at 11 PM with a retention policy of 7 daily, 2 weekly, and 6 monthly backups. Backups are stored in `/data/backups/`.

### What to Back Up

- `/data/calendar-sync.db` -- all application data
- `/secrets/encryption.key` -- required to decrypt OAuth tokens

### Manual Backup

Create and download backups from the web UI at `/app/settings`, or via the API:

```bash
curl -X POST https://your-domain/api/backups
```

### Recovery

1. Stop the container
2. Place backup as `/data/restore-pending.zip`
3. Start the container -- it auto-detects and restores
4. Verify via admin dashboard

## Development

### Local Setup

```bash
python -m venv venv
source venv/bin/activate
pip install -r requirements.txt -r requirements-dev.txt

export DATABASE_PATH=./data/calendar-sync.db
export ENCRYPTION_KEY_FILE=./secrets/encryption.key
export PUBLIC_URL=http://localhost:3000

python -m uvicorn app.main:app --host 0.0.0.0 --port 3000 --reload
```

### Running Tests

```bash
pytest
pytest --cov=app --cov-report=html
```

### End-to-End Tests

See `e2e/README.md` for Playwright-based tests against a running instance.

### Test Sidecar

A test sidecar container runs automated sync scenarios against the live instance:

```bash
docker compose --profile test up -d
```

Dashboard at port 8100.

### Gmail Test Mode

For testing with Gmail accounts (no Workspace), see `GMAIL_TESTING.md`.

## Upgrading

Pull new code and rebuild:

```bash
git pull
docker compose up -d --build calendar-sync
```

Database migrations run automatically on startup (inline `ALTER TABLE` statements that are safe to re-run). No manual migration steps needed.

## Adding Users

Any user with an email address in the home org domain can log in at `/app/login`. They:

1. Sign in with Google (home org account)
2. Select their main calendar
3. Connect client calendars from the dashboard

The first user (setup wizard admin) can grant admin privileges to other users from `/admin/users`.

## Troubleshooting

### Checking Logs

```bash
# Today's errors (excluding noise)
docker exec calendar-sync sh -c 'cat /data/logs/busybridge.log | grep " - ERROR - \| - WARNING - " | grep -v "file_cache\|discovery_cache\|Unknown webhook channel"'

# Previous days
docker exec calendar-sync ls /data/logs/
```

### Common Issues

| Symptom | Cause | Fix |
|---------|-------|-----|
| "credentials do not contain" errors | OAuth token lost its refresh_token | Re-authorize the account (disconnect + reconnect from dashboard) |
| Rate limit errors (403 rateLimitExceeded) | Too many API calls | Rate limiter handles this automatically; check if a calendar has excessive events |
| "Service accounts cannot invite attendees" | SA lacks Domain-Wide Delegation | Expected if DWD not configured; the retry-without-attendees path handles it |
| Backup permission denied | `/data/backups` owned by root | `docker exec -u root calendar-sync chown appuser:appuser /data/backups` |
| Sync not running | Global pause is on | Check admin dashboard or `POST /api/sync/resume` |
| Duplicate events | Webcal feed with unstable UIDs | Should be handled automatically; run orphan scan from sync control |
| "Unknown webhook channel" warnings | Stale channels after restart | Harmless, self-resolving when channels expire |

### Maintenance Scripts

One-time scripts for fixing historical data issues:

```bash
# Backfill origin metadata onto events created before metadata embedding
docker compose exec calendar-sync python /app/scripts/backfill_metadata.py --dry-run

# Clean up duplicate busy blocks from a prior bug
docker compose exec calendar-sync python /app/scripts/cleanup_duplicate_blocks.py --dry-run
```

Remove `--dry-run` to apply changes.

## API

When running, interactive API documentation is available at:

- Swagger UI: `https://your-domain/docs`
- ReDoc: `https://your-domain/redoc`

## Security

- OAuth tokens encrypted at rest with AES-256-GCM (key stored separately from database)
- Home org domain restriction enforced at OAuth callback
- Session tokens are JWTs with 7-day expiration
- Rate limiting on all endpoints (60/min general, 120/min webhooks)
- Runs as non-root user (`appuser`) in container
- Designed to run behind a reverse proxy with TLS termination

## Logs

Logs are written to stdout and to rotating files at `/data/logs/busybridge.log` (14-day retention, daily rotation). Inside the container:

```bash
# Today's log
docker exec calendar-sync cat /data/logs/busybridge.log

# Previous days
docker exec calendar-sync ls /data/logs/
```

## Technology Stack

| Component | Technology |
|-----------|------------|
| Language | Python 3.12 |
| Web Framework | FastAPI |
| Database | SQLite (aiosqlite, WAL mode) |
| Google API | google-api-python-client |
| Scheduling | APScheduler |
| Encryption | AES-256-GCM (cryptography) |
| Email | aiosmtplib |
| Frontend | Jinja2 + htmx + Alpine.js + Tailwind CSS |
| ICS Parsing | icalendar + recurring-ical-events |

## License

MIT License
