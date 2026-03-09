# BusyBridge Test Sidecar

Continuous soak-test sidecar for BusyBridge. Runs 79 tests across 12 suites against
real Google Calendar accounts, with a web dashboard on port 8100.

## Quick Start

```bash
# Build
docker compose --profile test build test-sidecar

# Start (requires calendar-sync to be healthy)
docker compose --profile test up test-sidecar -d

# View logs
docker compose logs -f test-sidecar

# Dashboard
open http://<host>:8100
```

## Architecture

- Reads shared SQLite DB (read-only) for accounts, tokens, calendars
- Decrypts OAuth tokens using shared encryption key
- Forges JWT session cookies for API auth
- Creates `[TEST-BB]` prefixed events for isolation
- Cleans up on startup, shutdown, and after each test
- Random test selection with timing-based delays (10s-5min)

## Test Suites (79 tests)

| Suite | Tests | Description |
|-------|-------|-------------|
| client_to_main | 12 | Basic event sync, updates, deletes, all-day, metadata |
| busy_blocks | 13 | Block creation, visibility, timing, cross-calendar |
| recurring | 7 | Weekly/daily, instance edits/deletes, no duplicates |
| personal | 5 | Personal calendar blocks, privacy, locking |
| rsvp | 3 | Accept/decline/tentative propagation |
| edit_protection | 6 | Lock emoji, move handling, editability |
| deletion | 3 | Cascading deletes, DB cleanup |
| self_healing | 3 | Missing block recreation, idempotent sync |
| edge_cases | 11 | Rapid updates, timezones, special chars, midnight span |
| multi_calendar | 2 | Cross-calendar blocks, overlapping events |
| sync_control | 6 | Pause/resume, cleanup, full resync |
| full_state | 8 | Complete state verification across all calendars + DB |
