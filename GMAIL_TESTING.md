# Gmail Test Mode Guide

This guide is for running BusyBridge as a production-like instance with test Gmail accounts and test calendars only.

## Goal

Use a real deployment workflow (Docker + OAuth + background sync) without paying for Google Workspace seats.

## What `TEST_MODE` Changes

When `TEST_MODE=true`:

1. Home login is enforced by exact email allowlist (`TEST_MODE_ALLOWED_HOME_EMAILS`), not by domain.
2. Client account connections are enforced by exact email allowlist (`TEST_MODE_ALLOWED_CLIENT_EMAILS`).
3. If either allowlist is empty, BusyBridge fails safe and blocks that auth path.
4. BusyBridge-created events include a visible summary prefix (`MANAGED_EVENT_PREFIX`, default `[BusyBridge]`).

This prevents accidental use of non-test Gmail accounts.

## Recommended Account Setup (No Cost)

1. Create 2-5 dedicated Gmail accounts for testing.
2. Never use your real personal/work Gmail accounts in allowlists.
3. Optional: Use fewer accounts and multiple calendars per client account.

Example:

1. `bb-home-admin@gmail.com` (home/admin account)
2. `bb-home-user1@gmail.com` (optional extra home user)
3. `bb-client-1@gmail.com`
4. `bb-client-2@gmail.com`
5. `bb-client-3@gmail.com`

## Google Cloud OAuth Setup

1. Create a dedicated Google Cloud project for testing.
2. Enable Google Calendar API.
3. Create OAuth client credentials (Web application).
4. Add redirect URIs:
   - `https://<your-fqdn>/auth/callback`
   - `https://<your-fqdn>/auth/connect-client/callback`
   - `https://<your-fqdn>/setup/step/3/callback`

To avoid periodic calendar re-consent:

1. Move the OAuth app to **In production** (not Testing).
2. Complete any required verification for requested scopes.

If you keep the OAuth app in Google "Testing" status, refresh tokens can expire in ~7 days and force reconnects.

## Configure Docker Compose

Set these environment variables (for example in `.env`):

```env
FQDN=busybridge-test.example.com

TEST_MODE=true
TEST_MODE_ALLOWED_HOME_EMAILS=bb-home-admin@gmail.com,bb-home-user1@gmail.com
TEST_MODE_ALLOWED_CLIENT_EMAILS=bb-client-1@gmail.com,bb-client-2@gmail.com,bb-client-3@gmail.com
MANAGED_EVENT_PREFIX=[BusyBridge]

# Optional: disable webhooks and rely on polling sync only
ENABLE_WEBHOOKS=false
```

Then run:

```bash
docker-compose up -d
```

## OOBE / Initial Setup

1. Open `https://<your-fqdn>/setup`.
2. Enter OAuth client ID/secret.
3. At admin auth (Step 3), sign in with an address from `TEST_MODE_ALLOWED_HOME_EMAILS`.
4. Complete remaining setup steps.

## Real-World Test Flow

1. Log in as home account (`/app/login`).
2. Use `Connect Calendar` to add each client Gmail account.
3. For each connected client account, select one writable calendar.
4. Create/modify/delete events in client calendars and verify sync to home main calendar.
5. Create/modify/delete events on main and verify busy blocks on all other client calendars.

## Test Cases to Run

1. Access control
   - Home login with non-allowlisted Gmail must fail.
   - Client connect with non-allowlisted Gmail must fail.
2. Basic sync
   - New event client -> appears on main with details.
   - New event main -> busy blocks appear on other clients.
3. Update and delete
   - Edit event time/title on client and verify propagation.
   - Delete source event and verify mirror/busy-block cleanup.
4. Edge behavior
   - Recurring series update.
   - All-day event with "Free" should not create busy blocks.
5. Recovery
   - Restart container and verify sync resumes.
   - Re-run manual sync from dashboard.

## Useful Errors and Meanings

1. `test_mode_no_home_allowlist`: `TEST_MODE=true` but `TEST_MODE_ALLOWED_HOME_EMAILS` is empty.
2. `email_not_allowed`: Home login account not in home allowlist.
3. `test_mode_no_client_allowlist`: `TEST_MODE=true` but `TEST_MODE_ALLOWED_CLIENT_EMAILS` is empty.
4. `client_email_not_allowed`: Client connect account not in client allowlist.
5. `no_refresh_token`: Google did not return refresh token for client account; reconnect with consent and account chooser.

## Cleanup / Reset

1. Remove BusyBridge access from each Gmail test account in Google Account security settings.
2. In BusyBridge dashboard, click **Run Managed Cleanup**.
3. This cleanup runs both:
   - DB-based deletion of tracked BusyBridge event IDs, and
   - Prefix sweep (`MANAGED_EVENT_PREFIX`) across your main + connected client calendars.
4. If needed, manually verify in Google Calendar search using the same prefix (for example `[BusyBridge]`).
5. Delete test calendars/events if needed.
6. Rotate OAuth client credentials if the environment was shared.
7. Remove `data/` and `secrets/` to reset local state.
