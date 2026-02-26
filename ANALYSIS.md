# BusyBridge: Comprehensive Code & Safety Analysis

## Executive Summary

**Tests**: All 171 tests pass. 98% line coverage. The test suite is structurally sound
but has critical blind spots that allow real bugs to hide.

**Verdict**: The app has **6 bugs that can corrupt real calendars** in production.
Several are in core sync paths that execute every 5 minutes. The app should NOT be
given access to real calendars without fixes.

---

## 1. Test Results

```
171 passed, 0 failed, 1 warning
Coverage: 98% (2909 statements, 46 missed)
Runtime: ~4 seconds
```

The one warning is a benign `RuntimeWarning` about module import order in the
`test_main_module_main_block_runs_with_stubbed_uvicorn` test.

Uncovered lines are mostly in `app/sync/engine.py` (the `cleanup_managed_events_for_user`
error paths) and `app/config.py` (environment variable edge cases). No uncovered lines
in critical sync logic -- but coverage doesn't mean correctness.

---

## 2. Bugs That Will Corrupt Real Calendars

### BUG 1 (CRITICAL): `update_event` strips sync tags, breaking self-detection

**File**: `app/sync/google_calendar.py:193-206`

`create_event` stamps every event with an `extendedProperties.private` sync tag so
`is_our_event()` can identify BusyBridge-managed events. But `update_event` uses
Google's `events().update()` (a full replacement) and does NOT re-stamp the tag.
Since neither `copy_event_for_main` nor `create_busy_block` include the tag in
their output, every event update **silently removes the sync tag**.

**Consequence**: After any event is updated, `is_our_event()` returns `false`. The
engine will attempt to re-sync its own managed events, potentially creating duplicates
or sync loops. This executes on every incremental sync cycle (every 5 minutes by
default) for any event that changes.

### DESIGN NOTE: Deleting a synced copy deletes the ORIGINAL client event

**File**: `app/sync/rules.py:415-434`

When `handle_deleted_main_event` detects a deleted main calendar event that originated
from a client calendar, it calls `delete_event` on the original client calendar.
This is **intentional behavior**: if you delete a synced meeting, you want to decline
it on the client calendar too. Without this, the next sync cycle would just re-create
the copy. The Google Calendar API treats this as a decline (for meetings you were
invited to) rather than a hard delete.

**Edge case to be aware of**: If the user is the meeting *organizer* on the client
calendar, `delete_event` cancels the meeting for all attendees, which has a larger
blast radius than declining.

### BUG 3 (HIGH): Update failure creates permanently orphaned duplicates

**File**: `app/sync/rules.py:80-88`

When updating an existing main event fails (for ANY reason, including transient network
errors), the code unconditionally creates a brand-new event:

```python
except Exception as e:
    result = main_client.create_event(main_calendar_id, main_event_data)
    main_event_id = result["id"]
```

The old event is NOT deleted. The DB mapping is repointed to the new event. The old
event is permanently orphaned on the main calendar with no tracking. A single transient
Google API timeout creates a permanent duplicate.

### BUG 4 (HIGH): No concurrency protection -- concurrent syncs create duplicates

**File**: `app/sync/engine.py:39-171`

There is no per-calendar lock. The periodic sync job holds a global `periodic_sync`
lock, but webhook-triggered syncs have NO locking at all. If a webhook fires during
a periodic sync (which is common -- webhooks fire when events change, and the sync
itself changes events), both processes:

1. Fetch the same events from Google
2. Call `create_event` for the same source events
3. Create duplicate events and busy blocks on real calendars

The `UNIQUE` DB constraints prevent duplicate rows in the database, but they do NOT
prevent duplicate events on Google Calendar (the Google API `insert` is not idempotent).

### BUG 5 (HIGH): `cleanup_managed_events_for_user` destroys tracking unconditionally

**File**: `app/sync/engine.py:735`

After attempting remote deletions (which may partially fail), the function runs:

```python
await db.execute("DELETE FROM event_mappings WHERE user_id = ?", (user_id,))
```

This deletes ALL event mapping records regardless of whether the corresponding remote
events were actually deleted. If Google's API was partially down during cleanup, the
DB records needed for retry are permanently destroyed. Those undeleted events on real
calendars become invisible to the system.

### BUG 6 (HIGH): `reconcile_calendar` creates permanent orphans

**File**: `app/sync/consistency.py:232-239`

When `reconcile_calendar` finds stale mappings (DB records for events no longer on the
client calendar), it deletes the mapping from the DB but does NOT delete the
corresponding main calendar event or busy blocks from Google. The `ON DELETE CASCADE`
removes the `busy_blocks` DB rows too, so both the main copies and busy blocks on real
calendars become permanently orphaned with no way to track or clean them up.

### BUG 7 (MODERATE): Deleted-event handlers orphan busy blocks

**Files**: `app/sync/rules.py:379-380` and `app/sync/rules.py:457`

Both `handle_deleted_client_event` and `handle_deleted_main_event` unconditionally
delete busy block DB records after attempting remote deletion:

```python
await db.execute("DELETE FROM busy_blocks WHERE event_mapping_id = ?", (mapping["id"],))
```

If any remote busy block deletion failed (logged but ignored on lines 376-377), the DB
tracking record is still destroyed. Those busy block events persist on real client
calendars as phantom "Busy" events with no way to clean them up through normal sync.

---

## 3. Security Vulnerabilities

### No webhook authentication

**File**: `app/api/webhooks.py:14-42`

The webhook endpoint is publicly accessible with no cryptographic verification that
requests come from Google. Google Calendar supports an `X-Goog-Channel-Token` header
for shared-secret verification, but the codebase never sets or checks it.

The only validation is matching the `channel_id` against the database and comparing
`resource_id`. An attacker who guesses a valid channel UUID can trigger arbitrary sync
operations, forcing the app to read/write real calendars.

### Webhook-triggered sync has no rate limiting or deduplication

**File**: `app/api/webhooks.py:86-102`

Each incoming webhook spawns a background sync task with no deduplication. An attacker
(or a burst of legitimate Google notifications) could trigger dozens of concurrent syncs
for the same calendar, amplifying Bug 4 (duplicate events).

### Job lock race condition (TOCTOU)

**File**: `app/jobs/sync_job.py:132-146`

The lock acquire uses separate DELETE + INSERT with commits between them, creating a
window where two coroutines can both acquire the "same" lock.

### OOBE is unauthenticated

**File**: `app/ui/setup.py`

The entire setup wizard is accessible to anyone who can reach the server before setup
is complete. An attacker could complete the OOBE and become the admin.

### GET-based logout enables CSRF

**File**: `app/auth/routes.py:396-399`

The logout endpoint accepts GET requests, allowing `<img src="/auth/logout">` to
force-logout any user via CSRF.

---

## 4. Test Quality Assessment

### Strengths

- **Real database**: Tests use an in-memory SQLite database (not mocked), so SQL
  queries, constraints, foreign keys, and transactions are all exercised.
- **Fail-safe tests are excellent**: The `test_fail_safe_behavior.py` suite explicitly
  tests that sync tokens are NOT advanced on partial failure, that DB records are
  preserved when remote deletions fail, and that mismatched webhook resource IDs don't
  trigger sync. This is well above average.
- **Deletion scenarios are thorough**: Both client-event and main-event deletion paths
  are tested with non-recurring (hard delete) and recurring (soft delete) variants.
- **Error-path coverage is genuine**: Tests for token refresh failures, consecutive
  failure alerting, and partial-cleanup reporting all verify real behavior.

### Critical Gaps

1. **No integration test for the full sync pipeline**. Engine tests monkeypatch
   `sync_client_event_to_main` and `sync_main_event_to_clients` with stubs. Rule tests
   use fake Google clients. There is no test that exercises:
   `webhook -> trigger_sync -> list_events -> sync_client_event_to_main -> sync_main_event_to_clients`
   as a complete flow. A bug in how the engine passes data to the rules would be invisible.

2. **Tests never verify WHICH calendar is written to**. Fake clients accept any
   `calendar_id` silently. A bug that passes `main_calendar_id` where it should pass
   `client_calendar_id` would not be caught.

3. **Tests never verify the event BODY sent to Google**. Most tests only check the
   returned event ID and DB state, not the actual event data passed to `create_event`
   or `update_event`. A bug in `copy_event_for_main` that drops the location or mangles
   the description would only be caught by one unit test.

4. **Zero concurrency tests**. No test runs two sync operations simultaneously. The
   duplicate-event bugs from concurrent syncs are completely invisible to the suite.

5. **No idempotency tests**. No test calls `sync_client_event_to_main` twice with the
   same event to verify it doesn't create duplicates.

6. **Bug 1 (sync tag stripping) is untestable with current mocks**. The fake clients
   don't simulate Google's full-replacement semantics for `update`, so the tag-stripping
   bug cannot be caught even if a test were written with the current mock infrastructure.

7. **No timezone testing**. All test events use UTC. The `create_busy_block` function
   defaults to `"UTC"` for timezone, but no test verifies timezone preservation.

8. **No multi-user isolation test**. No test verifies that user A's sync never writes
   to user B's calendars.

### Why 98% Coverage Is Misleading

The coverage is high because the tests exercise most code paths, but they don't verify
the *correctness* of what those paths do. Specifically:

- Every `create_event` call is "covered" but the event data is never inspected
- Every `delete_event` call is "covered" but the calendar ID is never verified
- The `update_event` path is "covered" but the sync-tag stripping (Bug 1) is invisible
- Concurrent execution paths are impossible to cover in synchronous test runs

This is a textbook case of high coverage with low defect detection capability.

---

## 5. Design Concerns for Real Calendar Safety

### Bidirectional deletion

The current design propagates deletion bidirectionally: delete from main -> declines
on client. This is intentional -- without it, deleted copies would reappear on the
next sync cycle. Users should be aware that deleting synced copies has real-calendar
consequences, and organizers should note that deleting cancels the meeting for everyone.

### No dry-run or preview mode

There is no way to see what the sync engine WOULD do before it does it. For an app
that writes to real calendars, a dry-run mode would dramatically reduce risk during
initial setup and debugging.

### Full calendar OAuth scope

Both `HOME_SCOPES` and `CLIENT_SCOPES` request `https://www.googleapis.com/auth/calendar`
(full read/write access). Consider whether `calendar.events` (more limited) would
suffice. The broader scope means any bug has maximum blast radius.

### No undo/rollback capability

Once events are created, modified, or deleted on real calendars, there is no way to
roll back. The audit log records what happened but provides no restoration capability.
Combined with the bugs above, this means errors are permanent.

### Single-process, single-connection architecture

The global `aiosqlite` connection has no connection pooling. All async operations share
one connection. While adequate for small deployments, this means a slow query in the
cleanup job can block sync operations, and there's no horizontal scaling path.

---

## 6. Fix Status

All critical bugs have been fixed in this commit:

| Bug | Fix | Status |
|-----|-----|--------|
| Bug 1: sync tag stripping | `update_event` now re-stamps sync tag | FIXED |
| Bug 3: no concurrency lock | Per-calendar asyncio locks in engine | FIXED |
| Bug 4: update fallback duplicates | Only create replacement on 404/410, re-raise otherwise | FIXED |
| Bug 5: unconditional mapping delete | Track successful deletes, only remove confirmed-clean records | FIXED |
| Bug 6: reconcile orphans remote events | Delete remote events/blocks before DB records | FIXED |
| Bug 7: busy block DB orphaning | Only delete DB records for blocks confirmed deleted remotely | FIXED |

Remaining items (not yet addressed):

| Priority | Item | Impact |
|----------|------|--------|
| P2 | Webhook authentication | Spoofed webhooks trigger sync |
| P2 | Add integration tests | Catch cross-layer bugs |
| P2 | Add concurrency tests | Catch race conditions |
| P3 | Job lock TOCTOU | Overlapping scheduled jobs |
| P3 | OOBE authentication | Setup hijacking |

---

## 7. Bottom Line

The app is well-architected and the codebase is clean and readable. The fail-safe
patterns (not advancing sync tokens on failure, preserving DB records when remote
ops fail) show genuine care for data safety. The test suite is above average in its
error-handling coverage.

The critical bugs identified in this analysis (sync tag stripping, concurrency gaps,
orphaned duplicates, unconditional DB cleanup) have all been fixed. The remaining
items are security hardening (webhook auth, OOBE protection) and test infrastructure
improvements (integration tests, concurrency tests).
