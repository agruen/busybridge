# Calendar Sync Engine Specification

## Project Overview

Build a self-hosted, multi-user calendar synchronization service for a consulting organization. The service allows users to connect multiple “client” calendars (from client organizations) to their “main” calendar, keeping availability in sync across all calendars without duplicating event details where they don’t belong.

### Core Problem Being Solved

Consultants work with multiple client organizations, each with their own calendar system. They need:

1. A single “main” calendar that shows ALL their commitments with full details
1. Client calendars that show their real availability (as “Busy” blocks) without exposing details from other clients or personal events
1. Bidirectional sync so changes propagate correctly

-----

## User Roles & Access

### Organization Model

- The service is restricted to a single Google Workspace organization (the “home org”)
- Only users with email addresses in that Workspace domain can log in
- The home org is configured during the OOBE (Out-of-Box Experience) wizard

### User Types

|Role |Description                                            |Capabilities                                        |
|-----|-------------------------------------------------------|----------------------------------------------------|
|Admin|First user to complete OOBE, plus anyone they designate|Full system access, user management, global settings|
|User |Any authenticated user from the home org domain        |Connect own calendars, manage own sync settings     |

### Per-User Setup

- Each user authenticates with their home org Google account
- Each user designates one calendar as their “main” calendar (defaults to primary calendar, but configurable)
- Each user can connect multiple “client” calendars by authenticating with those external Google accounts

-----

## Out-of-Box Experience (OOBE) Wizard

When the application is first accessed and no organization exists in the database, users are directed to the setup wizard.

### Wizard Flow

**Step 1: Welcome**

- Welcome message explaining what the app does
- “Get Started” button
- Prerequisites checklist:
  - [ ] You have a Google Cloud project (link to instructions)
  - [ ] You have OAuth credentials ready
  - [ ] You are an admin of your Google Workspace

**Step 2: Google Cloud Credentials**

- Input fields:
  - Google OAuth Client ID
  - Google OAuth Client Secret
- Expandable instructions panel:
1. Go to Google Cloud Console
1. Create a new project (or select existing)
1. Enable Google Calendar API
1. Configure OAuth consent screen (scopes needed: calendar, email, profile)
1. Create OAuth 2.0 credentials (Web application)
1. Add redirect URIs: `https://{your-domain}/auth/callback` and `https://{your-domain}/auth/connect-client/callback`
1. Copy Client ID and Client Secret
- “Test Connection” button to validate credentials
- “Next” button (disabled until credentials validated)

**Step 3: Admin Authentication**

- “Sign in with Google” button
- Uses the credentials from Step 2
- After OAuth completes:
  - Extract user’s email domain
  - Display: “You signed in as `admin@workingpaper.co`”
  - Display: “This will restrict the service to `@workingpaper.co` users”
- “Confirm & Continue” button

**Step 4: Email Alerts (Optional)**

- Toggle: “Enable email alerts for sync failures and issues”
- If enabled, show fields:
  - SMTP Host (e.g., `smtp.gmail.com`)
  - SMTP Port (e.g., `587`)
  - SMTP Username
  - SMTP Password (masked input)
  - “From” Email Address
  - Admin Notification Email(s) (comma-separated)
- “Send Test Email” button
- “Skip” or “Next” button

**Step 5: Encryption Key**

- Auto-generate a 32-byte encryption key
- Display it (base64 encoded) in a copyable box
- Warning message: “⚠️ Save this key securely. You will need it to restore from backup. It will not be shown again.”
- Checkbox: “I have saved this encryption key”
- “Complete Setup” button (disabled until checkbox checked)

**Step 6: Setup Complete**

- Success message
- “Go to Dashboard” button
- Quick start tips:
  - “Select your main calendar in Settings”
  - “Connect your first client calendar”

### OOBE Data Storage

After wizard completion, store:

- Google OAuth credentials (encrypted) in database
- Home org domain in `organization` table
- Admin user in `users` table with `is_admin = TRUE`
- Encryption key written to `secrets/encryption.key` file (or configured path)
- SMTP settings (password encrypted) in `settings` table

### Re-running OOBE

- OOBE only runs when `organization` table is empty
- To reset: admin can “Factory Reset” from settings (deletes all data, returns to OOBE)
- Partial reset: admin can update individual settings without full reset

-----

## Calendar Types & Terminology

### Main Calendar

- The user’s authoritative calendar where they see everything
- Belongs to their home org Google Workspace account
- Contains: native events + synced copies of client calendar events (with full details)

### Client Calendars

- Calendars from external organizations (client Google Workspaces)
- User authenticates separately to each client org
- Contains: native events + “Busy” blocks representing main calendar events

### Personal Calendars

- User's personal calendar from a Workspace or Gmail account
- Connected via separate OAuth flow (`/auth/connect-personal`)
- Read-only source — BusyBridge never writes events back to personal calendars
- Events sync as privacy-preserving busy blocks (no title, description, or attendees)
- Stored in `client_calendars` table with `calendar_type = 'personal'`

### Event Origin

- **Main-origin event**: Created directly on the main calendar (or synced there from a client and now living on main)
- **Client-origin event**: Created on or invited to via a client calendar
- **Personal-origin event**: An event on a connected personal calendar (synced as busy block only)

-----

## Sync Rules

### Client Calendar → Main Calendar

|Scenario                                  |Behavior                                                                                                                     |
|------------------------------------------|-----------------------------------------------------------------------------------------------------------------------------|
|New event on client calendar              |Create copy on main calendar with full details (title, description, location, attendees list for reference, time, recurrence)|
|Event modified on client calendar         |Update the copy on main calendar                                                                                             |
|Event deleted on client calendar          |Delete the copy from main calendar AND delete all “Busy” blocks on other client calendars that were created for this event   |
|User edits synced event on main calendar  |Sync changes back to client calendar IF user has edit rights on the original                                                 |
|User deletes synced event on main calendar|Delete/decline the event on the client calendar                                                                              |

### Main Calendar → Client Calendars

|Scenario                       |Behavior                                                                        |
|-------------------------------|--------------------------------------------------------------------------------|
|New event on main calendar     |Create “Busy” block on ALL connected client calendars                           |
|Event modified on main calendar|Update the “Busy” blocks (time changes) on all client calendars                 |
|Event deleted on main calendar |Delete the “Busy” blocks from all client calendars                              |
|All-day event on main calendar |Only create “Busy” blocks if the event’s “Show as” status is “Busy” (not “Free”)|

### Personal Calendar → Main Calendar + Client Calendars

|Scenario                                    |Behavior                                                                                    |
|--------------------------------------------|--------------------------------------------------------------------------------------------|
|New event on personal calendar              |Create “Busy (Personal)” block on main calendar AND on ALL connected client calendars        |
|Event modified on personal calendar         |Update the “Busy (Personal)” blocks (time changes) on main and all client calendars          |
|Event deleted on personal calendar          |Delete the “Busy (Personal)” blocks from main calendar and all client calendars              |
|All-day event on personal calendar          |Create all-day “Busy (Personal)” blocks on main and all client calendars                     |
|Recurring event on personal calendar        |Copy recurrence rule to “Busy (Personal)” blocks                                            |

**Important**: Personal calendars are strictly read-only — BusyBridge never creates busy blocks on the personal calendar itself.

### Busy Block Properties

**Client-origin busy blocks:**

- Title: “Busy” (prefixed with `MANAGED_EVENT_PREFIX`, e.g. “[BusyBridge] Busy”)
- Description: Empty
- Show as: Busy
- Visibility: Private (if supported)
- No attendees

**Personal-origin busy blocks:**

- Title: “Busy (Personal)” (prefixed with `MANAGED_EVENT_PREFIX`, e.g. “[BusyBridge] Busy (Personal)”)
- Description: Empty
- Show as: Busy
- Visibility: Private (if supported)
- No attendees
- Distinct labeling allows manual identification and cleanup

### Cross-Client Sync

When an event exists on Client Calendar A, it syncs to Main, and then must create “Busy” blocks on Client Calendars B, C, D, etc.

```
Client A event → syncs to Main (with details) → creates Busy blocks on Client B, C, D
```

If the Client A event is deleted:

1. Delete the detailed copy from Main
1. Delete the Busy blocks from Client B, C, D

-----

## Recurring Events

### Full Fidelity Required

- Recurring events must sync as recurring events (not expanded into individual instances)
- Recurrence rules (RRULE) must be preserved
- Single-instance modifications (exceptions) must sync correctly
- Single-instance deletions must sync as exceptions

### Implementation Notes

- Google Calendar API represents recurring events with a parent event and optionally modified instances
- When syncing, preserve the recurrence structure
- Track both the recurring series and any modified instances in the database
- When a single instance is modified on client, sync that specific instance modification to main
- When a single instance is deleted, sync that deletion

-----

## Edit Rights & Permissions

### Determining Edit Rights

Before syncing an edit from main back to a client calendar:

1. Check if the user is the organizer of the event
1. Check if the user has “writer” access via guestsCanModify or explicit ACL

### If User Lacks Edit Rights

- Do NOT sync title/description/time changes from main back to client
- Treat the main calendar copy as read-only (informational)
- If user tries to delete from main: remove from main view but do NOT delete from client (just decline if it’s an invite)

### Visual Indicator

- In the UI, show which synced events are editable vs. read-only (where applicable)

-----

## All-Day Event Handling

### Rules

- All-day events where “Show as” = “Free” → Do NOT sync to client calendars
- All-day events where “Show as” = “Busy” → Sync as all-day “Busy” blocks to client calendars
- When syncing from client to main: preserve all-day status and show-as setting

### Rationale

Events like “Vacation” marked as “Free” are informational and shouldn’t block time on client calendars. Events explicitly marked “Busy” should block.

-----

## Conflict Handling

### Overlapping Events

- If two different client calendars have events at the same time, BOTH sync to main as separate events
- The UI on main calendar will show them overlapping (this is correct—it surfaces a real conflict)
- The system does not attempt to resolve conflicts automatically

### Busy Block Collisions

- If someone at a client org books over an existing “Busy” block, the system does nothing
- The new event will sync to main, creating an overlap there
- User can manually resolve

-----

## Disconnection Behavior

When a user disconnects a client calendar:

1. **On the client calendar**: Delete all “Busy” blocks that the system created
1. **On the main calendar**: Delete all events that were synced from that client calendar
1. **On other client calendars**: Delete “Busy” blocks that were created for events originating from the disconnected client
1. **In the database**: Mark the calendar as disconnected, keep records for audit (cleaned up by retention policy)

-----

## Database Schema

### Technology

- SQLite (single-file, easy to backup and manage)
- Located at a configurable path, default: `/data/calendar-sync.db`

### Retention Policy

|Record Type                           |Retention                               |
|--------------------------------------|----------------------------------------|
|Single (non-recurring) event mappings |30 days after event end date            |
|Recurring event series mappings       |Kept indefinitely while series exists   |
|Recurring event instance modifications|Kept as long as parent series exists    |
|Soft-deleted recurring series         |30 days after deletion (allows recovery)|
|Audit/sync log entries                |90 days                                 |
|Disconnected calendar records         |30 days after disconnection             |

### Core Tables

```sql
-- The home organization (single row)
CREATE TABLE organization (
    id INTEGER PRIMARY KEY,
    google_workspace_domain TEXT NOT NULL UNIQUE,
    google_client_id_encrypted BLOB NOT NULL,
    google_client_secret_encrypted BLOB NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP
);

-- System settings
CREATE TABLE settings (
    key TEXT PRIMARY KEY,
    value_encrypted BLOB,  -- Encrypted for sensitive values
    value_plain TEXT,      -- Plain text for non-sensitive values
    is_sensitive BOOLEAN DEFAULT FALSE,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Settings to store:
-- smtp_host, smtp_port, smtp_username, smtp_password (sensitive), 
-- smtp_from_address, alert_emails, alerts_enabled

-- Users in the home org
CREATE TABLE users (
    id INTEGER PRIMARY KEY,
    email TEXT NOT NULL UNIQUE,
    google_user_id TEXT NOT NULL UNIQUE,
    display_name TEXT,
    main_calendar_id TEXT, -- Google Calendar ID, NULL until configured
    is_admin BOOLEAN DEFAULT FALSE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    last_login_at TIMESTAMP
);

-- OAuth tokens (encrypted at rest)
CREATE TABLE oauth_tokens (
    id INTEGER PRIMARY KEY,
    user_id INTEGER NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    account_type TEXT NOT NULL, -- 'home', 'client', or 'personal'
    google_account_email TEXT NOT NULL,
    access_token_encrypted BLOB NOT NULL,
    refresh_token_encrypted BLOB NOT NULL,
    token_expiry TIMESTAMP,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP,
    UNIQUE(user_id, google_account_email)
);

-- Connected client and personal calendars
CREATE TABLE client_calendars (
    id INTEGER PRIMARY KEY,
    user_id INTEGER NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    oauth_token_id INTEGER NOT NULL REFERENCES oauth_tokens(id),
    google_calendar_id TEXT NOT NULL,
    display_name TEXT,
    calendar_type TEXT NOT NULL DEFAULT 'client', -- 'client' or 'personal'
    is_active BOOLEAN DEFAULT TRUE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    disconnected_at TIMESTAMP
);

-- Tracks sync state for each calendar
CREATE TABLE calendar_sync_state (
    id INTEGER PRIMARY KEY,
    client_calendar_id INTEGER NOT NULL REFERENCES client_calendars(id) ON DELETE CASCADE,
    sync_token TEXT, -- Google's sync token for incremental sync
    last_full_sync TIMESTAMP,
    last_incremental_sync TIMESTAMP,
    consecutive_failures INTEGER DEFAULT 0,
    last_error TEXT,
    UNIQUE(client_calendar_id)
);

-- The core event mapping table
CREATE TABLE event_mappings (
    id INTEGER PRIMARY KEY,
    user_id INTEGER NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    
    -- Origin info
    origin_type TEXT NOT NULL, -- 'main', 'client', or 'personal'
    origin_calendar_id INTEGER REFERENCES client_calendars(id), -- NULL if origin is main
    origin_event_id TEXT NOT NULL, -- Google Calendar event ID at origin
    origin_recurring_event_id TEXT, -- For instances of recurring events
    
    -- Main calendar copy
    main_event_id TEXT, -- Google Calendar event ID on main calendar
    
    -- Metadata
    event_start TIMESTAMP,
    event_end TIMESTAMP,
    is_all_day BOOLEAN DEFAULT FALSE,
    is_recurring BOOLEAN DEFAULT FALSE,
    user_can_edit BOOLEAN DEFAULT TRUE,
    
    -- Soft delete for recurring events
    deleted_at TIMESTAMP,
    
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP,
    
    UNIQUE(user_id, origin_calendar_id, origin_event_id)
);

-- Index for retention cleanup
CREATE INDEX idx_event_mappings_cleanup ON event_mappings(is_recurring, event_end, deleted_at);

-- Busy blocks created on client calendars
CREATE TABLE busy_blocks (
    id INTEGER PRIMARY KEY,
    event_mapping_id INTEGER NOT NULL REFERENCES event_mappings(id) ON DELETE CASCADE,
    client_calendar_id INTEGER NOT NULL REFERENCES client_calendars(id),
    busy_block_event_id TEXT NOT NULL, -- Google Calendar event ID of the busy block
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    UNIQUE(event_mapping_id, client_calendar_id)
);

-- Audit log
CREATE TABLE sync_log (
    id INTEGER PRIMARY KEY,
    user_id INTEGER REFERENCES users(id),
    calendar_id INTEGER REFERENCES client_calendars(id),
    action TEXT NOT NULL,
    status TEXT NOT NULL, -- 'success', 'failure', 'warning'
    details TEXT, -- JSON
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_sync_log_created ON sync_log(created_at);
CREATE INDEX idx_sync_log_user ON sync_log(user_id, created_at);

-- Webhook registrations
CREATE TABLE webhook_channels (
    id INTEGER PRIMARY KEY,
    user_id INTEGER NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    calendar_type TEXT NOT NULL, -- 'main' or 'client'
    client_calendar_id INTEGER REFERENCES client_calendars(id),
    channel_id TEXT NOT NULL UNIQUE,
    resource_id TEXT NOT NULL,
    expiration TIMESTAMP NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_webhook_expiration ON webhook_channels(expiration);

-- Email alert queue (for retry logic)
CREATE TABLE alert_queue (
    id INTEGER PRIMARY KEY,
    alert_type TEXT NOT NULL,
    recipient_email TEXT NOT NULL,
    subject TEXT NOT NULL,
    body TEXT NOT NULL,
    attempts INTEGER DEFAULT 0,
    last_attempt TIMESTAMP,
    sent_at TIMESTAMP,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Job locking (prevent concurrent job runs)
CREATE TABLE job_locks (
    job_name TEXT PRIMARY KEY,
    locked_at TIMESTAMP,
    locked_by TEXT  -- instance identifier
);
```

### SQLite Size Considerations

- SQLite handles databases up to 140 TB; this will never be a constraint
- With retention policy, database stays bounded
- Recommend: daily backup to a second location

-----

## Sync Engine Architecture

### Single Container Design

The application runs as a single Docker container containing:

- FastAPI web server (handles HTTP requests, OAuth, webhooks)
- Background scheduler (APScheduler) for periodic tasks
- All components share the same process and database connection pool

This simplifies deployment, logging, and debugging on resource-constrained hardware like Raspberry Pi.

### Sync Triggers

1. **Webhooks (Push)**: Google Calendar sends notifications when events change
1. **Periodic Poll (Backup)**: Every 5 minutes, poll all calendars using sync tokens
1. **Manual Trigger**: User can request a full re-sync from UI
1. **On Connect**: When a new client calendar is connected, do initial sync

### Webhook Setup

- Register a webhook channel for each calendar (main + all clients) per user
- Webhooks expire; re-register before expiration (Google max is ~7 days)
- Webhook endpoint: `https://{FQDN}/api/webhooks/google-calendar`

### Sync Process

```
1. Receive trigger (webhook or poll timer)
2. Identify which calendar changed
3. Fetch changes using sync token (incremental) or list (full)
4. For each changed event:
   a. Determine if it's an event we created (busy block) or a real event
   b. If it's a busy block we created, skip (avoid loops)
   c. If it's a real event, process according to sync rules
5. Update database mappings
6. Propagate changes to other calendars as needed
7. Update sync token
```

### Loop Prevention

- Tag all events created by the system with an extended property: `calendarSyncEngine: true`
- When processing events, skip any with this tag
- This prevents: Main→Client busy block triggering a sync back to Main

### Consistency & Recovery

**Consistency Check (runs hourly):**

1. For each event_mapping, verify both origin and synced copies still exist
1. If origin deleted but copies remain: delete copies
1. If copies deleted but origin remains: recreate copies
1. Log discrepancies for review

**Full Re-sync (manual or on error):**

1. Clear sync tokens for affected calendar
1. Fetch all events from scratch
1. Reconcile with database
1. Recreate missing busy blocks
1. Remove orphaned entries

-----

## Email Alerting

### Alert Types

|Alert                      |Trigger                                    |Recipients            |
|---------------------------|-------------------------------------------|----------------------|
|Token Revoked              |OAuth refresh fails with “invalid_grant”   |Affected user + admins|
|Calendar Inaccessible      |404 or 403 when accessing calendar         |Affected user + admins|
|Sync Failures              |5+ consecutive sync failures for a calendar|Affected user + admins|
|Webhook Registration Failed|Unable to register/renew webhooks          |Admins only           |
|System Error               |Unhandled exceptions, database errors      |Admins only           |

### Alert Behavior

- Alerts are queued in `alert_queue` table
- Background job processes queue every minute
- Retry failed sends up to 3 times with exponential backoff
- De-duplicate: don’t send same alert type for same calendar within 1 hour
- Include “Manage alerts” link in emails

### Email Templates

- Plain text + HTML versions
- Include: timestamp, affected calendar, error details, suggested action
- Footer: link to dashboard, unsubscribe/manage preferences

-----

## Admin Capabilities

### User Management

|Action                      |Description                                                       |
|----------------------------|------------------------------------------------------------------|
|View all users              |List all users with email, last login, calendar count, sync status|
|View user details           |See a user’s connected calendars and recent sync activity         |
|Trigger sync for user       |Run immediate sync for any user’s calendars                       |
|Disconnect calendar for user|Remove a calendar connection on behalf of a user                  |
|Force re-authentication     |Invalidate a user’s tokens, requiring fresh OAuth                 |
|Promote to admin            |Grant admin privileges to another user                            |
|Remove user                 |Delete user and all their data (with confirmation)                |

### System Management

|Action                    |Description                                                                |
|--------------------------|---------------------------------------------------------------------------|
|View system health        |Overall sync status, error rates, webhook health                           |
|Pause/resume sync globally|Emergency stop for all sync operations                                     |
|View system logs          |Aggregated sync logs across all users                                      |
|Manual cleanup            |Trigger retention cleanup immediately                                      |
|Update settings           |Modify SMTP config, OAuth credentials, etc.                                |
|Export data               |Download database backup                                                   |
|Factory reset             |Delete all data and return to OOBE (requires confirmation + typing “RESET”)|

### Admin UI Pages

**Admin Dashboard** (`/admin`)

- System health summary (users, calendars, events synced, error rate)
- Recent alerts
- Quick actions (pause sync, run cleanup)

**User Management** (`/admin/users`)

- Table of all users
- Search/filter
- Click to view user details

**User Detail** (`/admin/users/:id`)

- User info
- Connected calendars with status
- Sync log for this user
- Action buttons (sync, disconnect, force re-auth)

**System Logs** (`/admin/logs`)

- Filterable/searchable log viewer
- Filter by: user, calendar, action type, status, date range

**Settings** (`/admin/settings`)

- Update OAuth credentials (re-validates on save)
- Update SMTP settings (test button)
- Alert configuration
- Danger zone: factory reset

-----

## API Design

### Public Endpoints (no auth)

```
GET  /                              - Redirect to /app or /setup
GET  /health                        - Health check (for monitoring)
GET  /setup                         - OOBE wizard (if not configured)
POST /setup/step/:step              - OOBE wizard step submission
```

### Authentication Endpoints

```
GET  /auth/login                      - Initiate Google OAuth for home org
GET  /auth/callback                   - OAuth callback
POST /auth/logout                     - Log out
GET  /auth/connect-client             - Initiate OAuth for a client account
GET  /auth/connect-client/callback    - Client OAuth callback
GET  /auth/connect-personal           - Initiate OAuth for a personal account
GET  /auth/connect-personal/callback  - Personal OAuth callback
```

### User Endpoints (authenticated)

```
GET  /api/me                        - Get current user profile
PUT  /api/me/main-calendar          - Set main calendar
GET  /api/me/calendars              - List user's Google calendars (for selection)
GET  /api/me/alert-preferences      - Get user's alert preferences
PUT  /api/me/alert-preferences      - Update alert preferences
```

### Client Calendar Management (authenticated)

```
GET  /api/client-calendars                  - List connected client calendars
POST /api/client-calendars                  - Connect a new client calendar (after OAuth)
DELETE /api/client-calendars/:id            - Disconnect a client calendar
POST /api/client-calendars/:id/sync         - Trigger manual sync
GET  /api/client-calendars/:id/status       - Get detailed sync status
```

### Personal Calendar Management (authenticated)

```
GET  /api/personal-calendars                - List connected personal calendars
POST /api/personal-calendars                - Connect a new personal calendar (after OAuth)
DELETE /api/personal-calendars/:id          - Disconnect a personal calendar
POST /api/personal-calendars/:id/sync       - Trigger manual sync
GET  /api/personal-calendars/:id/status     - Get detailed sync status
```

### Sync Status & Logs (authenticated)

```
GET  /api/sync/status               - Overall sync status for current user
GET  /api/sync/log                  - Recent sync activity log for current user
POST /api/sync/full                 - Trigger full re-sync for current user
```

### Admin Endpoints (admin only)

```
GET  /api/admin/health              - Detailed system health
GET  /api/admin/users               - List all users
GET  /api/admin/users/:id           - Get user details
POST /api/admin/users/:id/sync      - Trigger sync for a user
POST /api/admin/users/:id/force-reauth - Force re-authentication
DELETE /api/admin/users/:id         - Delete user
PUT  /api/admin/users/:id/admin     - Set admin status
POST /api/admin/users/:id/calendars/:calId/disconnect - Disconnect calendar for user
GET  /api/admin/logs                - System-wide sync logs
POST /api/admin/sync/pause          - Pause all sync operations
POST /api/admin/sync/resume         - Resume sync operations
POST /api/admin/cleanup             - Trigger manual cleanup
GET  /api/admin/settings            - Get system settings
PUT  /api/admin/settings            - Update system settings
POST /api/admin/settings/test-email - Send test email
POST /api/admin/factory-reset       - Factory reset (requires confirmation token)
GET  /api/admin/export              - Download database backup
```

### Webhook Endpoint

```
POST /api/webhooks/google-calendar  - Receive Google Calendar push notifications
```

-----

## Web UI

### Technology

- **Backend**: FastAPI with Jinja2 templates
- **Frontend**: Server-rendered HTML + htmx for interactivity + Alpine.js for client-side state
- **Styling**: Tailwind CSS (via CDN)
- **Icons**: Heroicons or Lucide (via CDN)

### Why This Approach

- No build step required
- Works well on low-bandwidth connections
- Easy to maintain and modify
- Progressive enhancement (works without JS, better with it)

### Pages

**Login Page** (`/app/login`)

- “Sign in with Google” button
- Shows home org domain restriction message
- Error display for failed logins

**Dashboard** (`/app`)

- Current user info
- Main calendar display (with “Change” link)
- Sync status summary (last sync, any errors)
- List of connected client calendars:
  - Display name
  - Account email
  - Last sync time
  - Status indicator (✓ green, ⚠ yellow, ✗ red)
  - “Sync Now” button
  - “Disconnect” button (with confirmation)
- “Connect Client Calendar” button
- Personal Calendars section (purple-themed):
  - List of connected personal calendars with sync status
  - “Connect Personal Calendar” button
  - “Sync Now” / “Disconnect” actions per calendar

**Connect Client Calendar Flow**

1. Click “Connect Client Calendar”
1. Redirected to Google OAuth (different account)
1. After auth, shown list of calendars from that account
1. User selects which calendar to sync
1. System does initial sync
1. Returns to dashboard with new calendar listed

**Settings** (`/app/settings`)

- Main calendar selector (dropdown)
- Email notification preferences
- View sync history
- “Full Re-sync” button
- “Disconnect All & Start Fresh” button (with confirmation)

**Sync Log** (`/app/logs`)

- Paginated table of sync events
- Filter by calendar, status
- Shows: timestamp, calendar, action, status, details

**OOBE Wizard** (`/setup`)

- Multi-step form as described in OOBE section
- Progress indicator
- Back/Next navigation
- Validation at each step

**Admin Pages** (as described in Admin Capabilities section)

-----

## Infrastructure

### Docker Setup

**Dockerfile**

```dockerfile
FROM python:3.12-slim

WORKDIR /app

# Install system dependencies
RUN apt-get update && apt-get install -y \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Install Python dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy application code
COPY . .

# Create data directory
RUN mkdir -p /data /secrets

# Run as non-root user
RUN useradd -m -u 1000 appuser && chown -R appuser:appuser /app /data
USER appuser

EXPOSE 3000

# Health check
HEALTHCHECK --interval=30s --timeout=10s --start-period=5s --retries=3 \
    CMD curl -f http://localhost:3000/health || exit 1

CMD ["python", "-m", "uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "3000"]
```

**docker-compose.yml**

```yaml
version: '3.8'

services:
  calendar-sync:
    build: .
    container_name: calendar-sync
    restart: unless-stopped
    ports:
      - "3000:3000"
    environment:
      - DATABASE_PATH=/data/calendar-sync.db
      - ENCRYPTION_KEY_FILE=/secrets/encryption.key
      - PUBLIC_URL=https://${FQDN}
      - LOG_LEVEL=info
      - TZ=America/Chicago  # Set to your timezone
    volumes:
      - ./data:/data
      - ./secrets:/secrets
    healthcheck:
      test: ["CMD", "curl", "-f", "http://localhost:3000/health"]
      interval: 30s
      timeout: 10s
      retries: 3
```

**requirements.txt**

```
fastapi>=0.109.0
uvicorn[standard]>=0.27.0
python-multipart>=0.0.6
jinja2>=3.1.3
httpx>=0.26.0
google-api-python-client>=2.114.0
google-auth>=2.27.0
google-auth-oauthlib>=1.2.0
apscheduler>=3.10.4
cryptography>=42.0.0
pydantic>=2.5.3
pydantic-settings>=2.1.0
python-jose[cryptography]>=3.3.0
aiosmtplib>=3.0.1
aiosqlite>=0.19.0
```

### Environment Variables

|Variable             |Description                                                |Required          |
|---------------------|-----------------------------------------------------------|------------------|
|`DATABASE_PATH`      |Path to SQLite database file                               |Yes               |
|`ENCRYPTION_KEY_FILE`|Path to file containing encryption key                     |Yes               |
|`PUBLIC_URL`         |Full public URL (e.g., `https://calendar-sync.example.com`)|Yes               |
|`LOG_LEVEL`          |Logging level: debug, info, warning, error                 |No (default: info)|
|`TZ`                 |Timezone for scheduled jobs                                |No (default: UTC) |

Note: Google OAuth credentials and SMTP settings are stored in the database after OOBE, not in environment variables.

### First Run

1. Create directories: `mkdir -p data secrets`
1. Start the container: `docker-compose up -d`
1. Access `https://{FQDN}` in browser
1. Complete OOBE wizard
1. Wizard generates encryption key and saves to `/secrets/encryption.key`
1. Backup the `secrets/` directory immediately

### Backup & Recovery

**What to back up:**

- `/data/calendar-sync.db` - All application data
- `/secrets/encryption.key` - Required to decrypt tokens

**Recovery:**

1. Stop container
1. Restore `calendar-sync.db` and `encryption.key` to their paths
1. Start container
1. Verify via admin dashboard

**Automated backup (example cron job on host):**

```bash
0 3 * * * docker exec calendar-sync sqlite3 /data/calendar-sync.db ".backup '/data/backup-$(date +\%Y\%m\%d).db'"
0 4 * * * find /path/to/data/backup-*.db -mtime +7 -delete
```

-----

## Scheduled Jobs

All jobs run within the main application process using APScheduler.

|Job                   |Frequency       |Description                                       |
|----------------------|----------------|--------------------------------------------------|
|Periodic Sync         |Every 5 minutes |Poll all calendars for changes using sync tokens  |
|Webhook Renewal       |Every 6 hours   |Renew webhook channels expiring within 24 hours   |
|Consistency Check     |Every hour      |Verify database matches reality, fix discrepancies|
|Retention Cleanup     |Daily at 3 AM   |Delete old event mappings per retention policy    |
|Token Refresh         |Every 30 minutes|Proactively refresh tokens expiring within 1 hour |
|Alert Queue Processing|Every minute    |Send queued email alerts                          |
|Stale Alert Cleanup   |Daily at 4 AM   |Remove sent/failed alerts older than 7 days       |

-----

## Error Handling

### Transient Errors (retry with backoff)

- Network timeouts
- 5xx responses from Google
- Rate limit responses (429)
- SMTP connection failures

### Permanent Errors (alert user)

- Token revoked (invalid_grant) → requires re-authentication
- Calendar deleted (404) → requires disconnection
- Insufficient permissions (403) → requires re-authentication with correct scopes

### Error Response Strategy

```python
# Pseudo-code for sync error handling
def sync_calendar(calendar):
    try:
        result = google_api.sync(calendar)
        calendar.consecutive_failures = 0
        return result
    except TransientError as e:
        calendar.consecutive_failures += 1
        if calendar.consecutive_failures >= 5:
            send_alert("sync_failures", calendar)
        raise  # Let scheduler retry
    except TokenRevokedError:
        send_alert("token_revoked", calendar)
        calendar.is_active = False
    except CalendarNotFoundError:
        send_alert("calendar_inaccessible", calendar)
        calendar.is_active = False
```

-----

## Security Considerations

### Token Encryption

- All OAuth tokens encrypted using AES-256-GCM
- Encryption key stored in separate file (not in database or env vars)
- Key generated during OOBE with secure random bytes

### HTTPS

- Application assumes it runs behind a reverse proxy (nginx)
- Reverse proxy handles TLS termination
- Application checks `X-Forwarded-Proto` header
- Set `PUBLIC_URL` to `https://` URL

### Domain Restriction

- On login, verify user’s email domain matches configured home org domain
- Reject authentication attempts from other domains
- Enforce at OAuth callback, not just UI

### Session Security

- Use secure, HTTP-only cookies for session
- Session tokens are JWTs signed with a secret derived from encryption key
- Sessions expire after 7 days of inactivity

### Rate Limiting

- Implement rate limiting on all endpoints
- Stricter limits on webhook endpoint (potential for abuse)
- Use in-memory rate limiter (e.g., `slowapi`)

### Input Validation

- Validate all inputs with Pydantic models
- Sanitize any user-provided strings before display
- Use parameterized queries (SQLAlchemy/aiosqlite handles this)

-----

## Testing Strategy

### Unit Tests

- Sync rule logic (given event X, what operations should occur)
- Database operations (CRUD, retention cleanup)
- Token encryption/decryption
- Webhook payload parsing
- Alert deduplication logic

### Integration Tests

- OAuth flow (mock Google responses)
- Calendar CRUD operations (mock Google API)
- End-to-end sync scenarios
- Webhook handling
- OOBE wizard flow

### Test Scenarios to Cover

1. New event on client → appears on main
1. New event on main → busy blocks appear on all clients
1. Edit event on client → updates on main
1. Edit event on main (with permission) → updates on client
1. Edit event on main (without permission) → no change on client
1. Delete event on client → removes from main and all busy blocks
1. Delete event on main → removes from client (if origin) or removes busy block
1. Recurring event sync
1. Single instance modification of recurring event
1. Single instance deletion of recurring event
1. All-day free event → no busy blocks
1. All-day busy event → busy blocks created
1. Disconnect calendar → cleanup occurs
1. Webhook triggers sync
1. Recovery after missed webhooks (via polling)
1. Token refresh before expiry
1. Token revocation handling
1. Multiple users with overlapping client orgs
1. OOBE wizard completion
1. Admin operations (user management, force sync, etc.)

### Running Tests

```bash
# Run all tests
pytest

# Run with coverage
pytest --cov=app --cov-report=html

# Run specific test file
pytest tests/test_sync_rules.py
```

-----

## Technology Stack Summary

|Component             |Technology                   |
|----------------------|-----------------------------|
|Language              |Python 3.12                  |
|Web Framework         |FastAPI                      |
|ASGI Server           |Uvicorn                      |
|Database              |SQLite (via aiosqlite)       |
|Templating            |Jinja2                       |
|Frontend Interactivity|htmx + Alpine.js             |
|Styling               |Tailwind CSS (CDN)           |
|Google API            |google-api-python-client     |
|Scheduling            |APScheduler                  |
|Encryption            |cryptography (Fernet/AES-GCM)|
|Email                 |aiosmtplib                   |
|HTTP Client           |httpx                        |
|Validation            |Pydantic                     |

-----

## Project Structure

```
calendar-sync/
├── app/
│   ├── __init__.py
│   ├── main.py                 # FastAPI app, startup/shutdown
│   ├── config.py               # Settings, environment loading
│   ├── database.py             # Database connection, models
│   ├── encryption.py           # Token encryption/decryption
│   ├── auth/
│   │   ├── __init__.py
│   │   ├── routes.py           # OAuth endpoints
│   │   ├── google.py           # Google OAuth helpers
│   │   └── session.py          # Session management
│   ├── api/
│   │   ├── __init__.py
│   │   ├── users.py            # User endpoints
│   │   ├── calendars.py        # Calendar management endpoints
│   │   ├── sync.py             # Sync status/trigger endpoints
│   │   ├── admin.py            # Admin endpoints
│   │   └── webhooks.py         # Webhook receiver
│   ├── sync/
│   │   ├── __init__.py
│   │   ├── engine.py           # Core sync logic
│   │   ├── google_calendar.py  # Google Calendar API wrapper
│   │   ├── rules.py            # Sync rule implementations
│   │   └── consistency.py      # Consistency check logic
│   ├── jobs/
│   │   ├── __init__.py
│   │   ├── scheduler.py        # APScheduler setup
│   │   ├── sync_job.py         # Periodic sync job
│   │   ├── webhook_renewal.py  # Webhook renewal job
│   │   ├── cleanup.py          # Retention cleanup job
│   │   └── alerts.py           # Alert processing job
│   ├── alerts/
│   │   ├── __init__.py
│   │   ├── email.py            # Email sending
│   │   └── templates/          # Email templates
│   ├── ui/
│   │   ├── __init__.py
│   │   ├── routes.py           # Page routes
│   │   └── templates/          # Jinja2 templates
│   │       ├── base.html
│   │       ├── login.html
│   │       ├── dashboard.html
│   │       ├── settings.html
│   │       ├── setup/          # OOBE wizard templates
│   │       └── admin/          # Admin page templates
│   └── static/                 # Static assets (minimal)
├── tests/
│   ├── __init__.py
│   ├── conftest.py             # Pytest fixtures
│   ├── test_sync_rules.py
│   ├── test_database.py
│   ├── test_encryption.py
│   └── test_api.py
├── Dockerfile
├── docker-compose.yml
├── requirements.txt
├── requirements-dev.txt
├── pytest.ini
├── README.md
└── .gitignore
```

-----

## Implementation Order

### Phase 1: Foundation

1. Project setup (Docker, structure, dependencies)
1. Database schema and migrations
1. Encryption module
1. Configuration loading

### Phase 2: OOBE & Auth

1. OOBE wizard (all steps)
1. Google OAuth for home org
1. Session management
1. Domain restriction enforcement

### Phase 3: Core User Features

1. Main calendar selection
1. Client calendar OAuth flow
1. Client calendar connection/disconnection
1. Basic dashboard UI

### Phase 4: Sync Engine (MVP)

1. Client → Main sync (single events)
1. Main → Client busy blocks (single events)
1. Loop prevention (extended properties)
1. Manual sync trigger

### Phase 5: Full Sync

1. Edit propagation (with permission checking)
1. Delete cascading
1. Recurring event support
1. All-day event handling

### Phase 6: Automation

1. Webhook registration and handling
1. Periodic polling backup
1. Token refresh job
1. Consistency check job

### Phase 7: Reliability

1. Retention cleanup job
1. Error handling and recovery
1. Email alerting
1. Alert queue processing

### Phase 8: Admin

1. Admin dashboard
1. User management
1. System settings
1. Logs viewer

### Phase 9: Polish

1. UI improvements
1. Comprehensive error messages
1. Test coverage
1. Documentation

-----

## Success Criteria

The system is working correctly when:

1. ✓ OOBE wizard successfully configures the system from scratch
1. ✓ A consultant can log in with their home org Google account
1. ✓ Domain restriction prevents login from other domains
1. ✓ User can select their main calendar
1. ✓ User can connect 3+ client Google Workspace calendars
1. ✓ Events from all client calendars appear on main calendar with full details
1. ✓ “Busy” blocks appear on all client calendars for main calendar events
1. ✓ Changes propagate within 5 minutes (faster with working webhooks)
1. ✓ No duplicate events appear
1. ✓ Deleting an event properly cascades to all copies/blocks
1. ✓ Recurring events with modifications sync correctly
1. ✓ All-day “free” events don’t create busy blocks
1. ✓ Disconnecting a calendar cleans up all related data
1. ✓ Email alerts are sent for sync failures
1. ✓ Admin can view and manage all users
1. ✓ System runs reliably on a Raspberry Pi for weeks without intervention
1. ✓ Multiple consultants can use the system independently
1. ✓ Database stays bounded due to retention policy
