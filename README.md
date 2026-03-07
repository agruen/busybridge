# Calendar Sync Engine (BusyBridge)

A self-hosted, multi-user calendar synchronization service for consulting organizations. This service allows users to connect multiple "client" calendars (from client organizations) to their "main" calendar, keeping availability in sync across all calendars without duplicating event details where they don't belong.

## Features

- **Bidirectional Sync**: Events from client calendars appear on your main calendar with full details, while your main calendar events appear as "Busy" blocks on client calendars
- **Personal Calendar Sync**: Connect personal Gmail/Workspace calendars as read-only sources — events create privacy-preserving "Busy (Personal)" blocks on your main and all client calendars
- **Multi-User Support**: Each user in your organization can manage their own calendar connections
- **Recurring Event Support**: Full fidelity for recurring events, including single-instance modifications
- **Smart Busy Blocks**: Only creates blocks for events that actually block time (respects "Free" vs "Busy" status)
- **Webhook Integration**: Real-time sync via Google Calendar push notifications
- **Email Alerts**: Notifications for sync failures, token revocations, and other issues
- **Manual Managed Cleanup**: One-click removal of BusyBridge-created events using DB mappings plus prefix sweep
- **Service Account Mode**: Optional Google service account creates main calendar events, making non-editable events physically immovable in Google Calendar
- **Admin Dashboard**: Manage users, view system health, and configure settings

## Quick Start

### Prerequisites

1. Docker and Docker Compose
2. A Google Cloud project with:
   - Google Calendar API enabled
   - OAuth 2.0 credentials (Web application type)
3. A domain with HTTPS (for webhooks)

### Installation

1. Clone the repository:
   ```bash
   git clone <repository-url>
   cd busybridge
   ```

2. Create data directories:
   ```bash
   mkdir -p data secrets
   ```

3. Configure environment (optional - can be set in docker-compose.yml):
   ```bash
   export FQDN=your-domain.com
   ```

4. Start the service:
   ```bash
   docker-compose up -d
   ```

5. Access the setup wizard at `https://your-domain.com` and follow the prompts.

### Google Cloud Setup

1. Go to [Google Cloud Console](https://console.cloud.google.com/)
2. Create a new project or select an existing one
3. Enable the Google Calendar API
4. Configure OAuth consent screen:
   - User Type: Internal (for Workspace) or External
   - Scopes: `calendar`, `calendar.readonly`, `email`, `profile`, `openid`
5. Create OAuth 2.0 credentials (Web application):
   - Add redirect URIs:
     - `https://your-domain/auth/callback`
     - `https://your-domain/auth/connect-client/callback`
     - `https://your-domain/auth/connect-personal/callback`
     - `https://your-domain/setup/step/3/callback`

## Appointment Workflow

BusyBridge is built around one simple principle: **your main calendar is your single source of truth**. Client calendars are managed automatically — you rarely need to interact with them directly.

### Creating Appointments

Where you create an appointment depends on who needs to receive the invite:

**Create on your main calendar** when the meeting is yours to own — personal blocks, internal meetings with your own org, or any appointment where you don't need the invite to come from a client domain. BusyBridge automatically creates "Busy" blocks on every connected client calendar.

```
You create event on Main Calendar
         ↓
BusyBridge detects it (within 5 min, or instantly via webhook)
         ↓
"Busy" block created on Client A calendar
"Busy" block created on Client B calendar
"Busy" block created on Client C calendar
```

**Create on the client calendar** when you're organizing a meeting that needs to live within that client's domain — for example, inviting client colleagues to an internal meeting where the invite should come from their org. BusyBridge treats it as a client-origin event: it syncs a full-detail copy to your main calendar and creates "Busy" blocks on your other connected client calendars (but not back on Client A, to avoid duplication).

```
You create event on Client A Calendar
         ↓
BusyBridge copies it to your Main Calendar (with full details)
         ↓
"Busy" block created on Client B calendar
"Busy" block created on Client C calendar
(no busy block on Client A — it already has the real event)
```

### Personal Calendar Events

Connect a personal calendar (any Gmail or Workspace account) to block off personal time across all your work calendars. Personal calendars are **read-only** — BusyBridge never writes back to them.

```
Personal calendar event (e.g. "Doctor Appointment")
         ↓
BusyBridge creates "Busy (Personal)" block on Main Calendar
         ↓
"Busy (Personal)" block created on Client A calendar
"Busy (Personal)" block created on Client B calendar
```

No event details are shared — only "Busy (Personal)" is visible. When the personal event is deleted or cancelled, all blocks are cleaned up automatically.

### Recurring Meetings

Recurring events sync as recurring events — BusyBridge copies the recurrence rule (RRULE) directly onto each busy block, so the block expands identically to the original. Your clients see a recurring "Busy" block that matches the series; BusyBridge does not create individual entries per occurrence.

If you later modify or cancel a single instance of a recurring series, that change is synced individually without affecting the rest of the series.

### When Clients Schedule You

When a client adds you to a meeting on their calendar, BusyBridge handles the sync automatically:

```
Client creates event on Client A Calendar
         ↓
BusyBridge copies it to your Main Calendar (with full details)
         ↓
"Busy" block created on Client B calendar
"Busy" block created on Client C calendar
```

You'll see the appointment on your main calendar with full details (title, description, attendees). Other clients only see "Busy" — no cross-client information is ever shared.

### Viewing Your Schedule

**Always view your main calendar** for your complete schedule. It contains:

- Full details of all client-scheduled meetings (synced from client calendars)
- All your own appointments (created directly on main)

Client calendars are not useful for your day-to-day viewing — they only show "Busy" blocks from your other commitments, which is the view your clients see.

### Summary

| Action | Where |
|--------|-------|
| Create a personal or internal appointment | Your **main calendar** |
| Organize a meeting within a client's domain | That **client's calendar** |
| Block personal time across all calendars | Connect a **personal calendar** (read-only) |
| View your full schedule | Your **main calendar** |
| See what a client sees | That **client's calendar** (shows "Busy" blocks) |
| Accept a client meeting | Handled automatically — appears on main calendar |

## Architecture

The application runs as a single Docker container containing:

- **FastAPI web server**: Handles HTTP requests, OAuth, webhooks
- **Background scheduler (APScheduler)**: Runs periodic sync, cleanup, and maintenance tasks
- **SQLite database**: Stores all configuration and sync state

### Sync Flow

```
Client Calendar Event → Main Calendar (with full details)
                      ↓
                      → Busy blocks on other Client Calendars
```

### Key Components

- `/app/auth/`: OAuth, session management, and service account credentials
- `/app/api/`: REST API endpoints
- `/app/sync/`: Core sync engine and rules
- `/app/jobs/`: Background job definitions
- `/app/alerts/`: Email alerting system
- `/app/ui/`: Web interface and templates

## Configuration

### Environment Variables

| Variable | Description | Default |
|----------|-------------|---------|
| `DATABASE_PATH` | Path to SQLite database | `/data/calendar-sync.db` |
| `ENCRYPTION_KEY_FILE` | Path to encryption key file | `/secrets/encryption.key` |
| `PUBLIC_URL` | Public URL for OAuth callbacks | Required |
| `LOG_LEVEL` | Logging level (debug, info, warning, error) | `info` |
| `TZ` | Timezone for scheduled jobs | `UTC` |
| `ENABLE_WEBHOOKS` | Enable webhook renewal job | `true` |
| `TEST_MODE` | Enable Gmail-safe testing mode with allowlists | `false` |
| `TEST_MODE_ALLOWED_HOME_EMAILS` | Comma-separated allowlist for home-login accounts in test mode | `` |
| `TEST_MODE_ALLOWED_CLIENT_EMAILS` | Comma-separated allowlist for client-account connections in test mode | `` |
| `MANAGED_EVENT_PREFIX` | Visible summary prefix added to BusyBridge-created events | `[BusyBridge]` |
| `SERVICE_ACCOUNT_KEY_FILE` | Path to Google service account JSON key file (optional) | _(none)_ |

### Service Account Mode (Immovable Events)

When a third party invites a user to a meeting, that event syncs to the main calendar. Without a service account, the user could accidentally move the event on their main calendar even though they can't on the client calendar. BusyBridge has a revert mechanism (lock emoji + time restore), but the best fix is having a **service account (SA) create events on the main calendar** so the SA is the organizer and the user physically cannot drag-and-drop them in Google Calendar.

#### How It Works

- **SA mode (tier 2)**: The SA creates all main calendar events. Editable events get `guestsCanModify: true` so the user can still move those. Non-editable events are natively immovable because the SA owns them.
- **Fallback (tier 0)**: Current behavior — user token creates events, with lock emoji + revert for non-editable events. Used when no SA is configured, or SA access fails.

Existing events are not migrated. New and updated events transition naturally. If SA access is lost (key removed, sharing revoked), BusyBridge catches the error, resets the user to fallback mode, and logs a warning.

#### Setup

1. **Create a service account** in your Google Cloud project:
   - Go to IAM & Admin → Service Accounts → Create Service Account
   - Name it something like `busybridge-sync`
   - No roles needed (it uses calendar sharing, not domain-wide delegation)
   - Create a JSON key and download it

2. **Place the key file** in the `secrets/` directory:
   ```bash
   cp ~/Downloads/busybridge-sa-key.json secrets/sa-key.json
   ```

3. **Configure the environment variable** in `docker-compose.yml` or `.env`:
   ```
   SERVICE_ACCOUNT_KEY_FILE=/secrets/sa-key.json
   ```

4. **Share each user's main calendar** with the service account email (shown in Admin → Service Account):
   - Open Google Calendar → Settings → the main calendar → Share with specific people
   - Add the SA email with "Make changes to events" permission

5. **Activate SA mode** via the admin API:
   ```bash
   curl -X POST https://your-domain/api/admin/service-account/test/{user_id}
   ```
   This validates SA access and sets the user to tier 2. If the calendar isn't shared, it returns an error with the SA email to share with.

### Gmail Test Mode

For production-like testing with Gmail test accounts (without paid Workspace), see `GMAIL_TESTING.md`.

### Scheduled Jobs

| Job | Frequency | Description |
|-----|-----------|-------------|
| Periodic Sync | Every 5 minutes | Poll all calendars for changes |
| Webhook Renewal | Every 6 hours | Renew expiring webhook channels (if `ENABLE_WEBHOOKS=true`) |
| Consistency Check | Every hour | Verify database matches reality |
| Token Refresh | Every 30 minutes | Proactively refresh expiring tokens |
| Alert Processing | Every minute | Send queued email alerts |
| Retention Cleanup | Daily at 3 AM | Delete old records per retention policy |

## Development

### Local Setup

1. Create a virtual environment:
   ```bash
   python -m venv venv
   source venv/bin/activate
   ```

2. Install dependencies:
   ```bash
   pip install -r requirements.txt
   pip install -r requirements-dev.txt
   ```

3. Set environment variables:
   ```bash
   export DATABASE_PATH=./data/calendar-sync.db
   export ENCRYPTION_KEY_FILE=./secrets/encryption.key
   export PUBLIC_URL=http://localhost:3000
   ```

4. Run the application:
   ```bash
   python -m uvicorn app.main:app --host 0.0.0.0 --port 3000 --reload
   ```

### Running Tests

```bash
pytest
pytest --cov=app --cov-report=html
```

## API Documentation

When running, API documentation is available at:
- Swagger UI: `https://your-domain/docs`
- ReDoc: `https://your-domain/redoc`

## Backup & Recovery

### What to Back Up

- `/data/calendar-sync.db` - All application data
- `/secrets/encryption.key` - Required to decrypt tokens

### Recovery

1. Stop the container
2. Restore database and encryption key files
3. Start the container
4. Verify via admin dashboard

## Security Considerations

- All OAuth tokens are encrypted using AES-256-GCM
- Encryption key is stored separately from database
- Home login restriction is enforced (domain-based by default, email allowlist in `TEST_MODE`)
- Session tokens are JWTs with configurable expiration
- Rate limiting on all endpoints

## License

MIT License - See LICENSE file for details.
