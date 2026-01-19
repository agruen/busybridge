# Calendar Sync Engine (BusyBridge)

A self-hosted, multi-user calendar synchronization service for consulting organizations. This service allows users to connect multiple "client" calendars (from client organizations) to their "main" calendar, keeping availability in sync across all calendars without duplicating event details where they don't belong.

## Features

- **Bidirectional Sync**: Events from client calendars appear on your main calendar with full details, while your main calendar events appear as "Busy" blocks on client calendars
- **Multi-User Support**: Each user in your organization can manage their own calendar connections
- **Recurring Event Support**: Full fidelity for recurring events, including single-instance modifications
- **Smart Busy Blocks**: Only creates blocks for events that actually block time (respects "Free" vs "Busy" status)
- **Webhook Integration**: Real-time sync via Google Calendar push notifications
- **Email Alerts**: Notifications for sync failures, token revocations, and other issues
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
   - Scopes: `calendar`, `email`, `profile`, `openid`
5. Create OAuth 2.0 credentials (Web application):
   - Add redirect URIs:
     - `https://your-domain/auth/callback`
     - `https://your-domain/auth/connect-client/callback`
     - `https://your-domain/setup/step/3/callback`

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

- `/app/auth/`: OAuth and session management
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

### Scheduled Jobs

| Job | Frequency | Description |
|-----|-----------|-------------|
| Periodic Sync | Every 5 minutes | Poll all calendars for changes |
| Webhook Renewal | Every 6 hours | Renew expiring webhook channels |
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
- Domain restriction prevents login from unauthorized domains
- Session tokens are JWTs with configurable expiration
- Rate limiting on all endpoints

## License

MIT License - See LICENSE file for details.
