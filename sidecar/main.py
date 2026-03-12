"""BusyBridge Test Sidecar - Entry point."""

from __future__ import annotations

import asyncio
import logging
import os
import signal
import sys
import time
import uuid

import httpx
import uvicorn

from sidecar.config import Config
from sidecar.dashboard.server import add_result, app, load_today_results, set_state
from sidecar.framework.base import TestContext, TestResult
from sidecar.framework.cleanup import CleanupManager
from sidecar.framework.event_factory import EventFactory
from sidecar.framework.runner import SoakRunner
from sidecar.framework.sentinel import SentinelManager
from sidecar.framework.sync_waiter import SyncWaiter
from sidecar.infra.api_client import APIClient
from sidecar.infra.calendar_client import CalendarTestClient
from sidecar.infra.db_reader import DBReader
from sidecar.infra.encryption import EncryptionManager
from sidecar.infra.session_forger import derive_session_secret, forge_session_token
from sidecar.infra.token_manager import TokenManager

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    stream=sys.stdout,
)
logger = logging.getLogger("sidecar")


async def wait_for_app(base_url: str, max_wait: int = 120) -> None:
    """Wait for the main app to be healthy."""
    logger.info("Waiting for app at %s ...", base_url)
    start = time.time()
    async with httpx.AsyncClient() as client:
        while time.time() - start < max_wait:
            try:
                resp = await client.get(f"{base_url}/health", timeout=5)
                if resp.status_code == 200:
                    logger.info("App is healthy")
                    return
            except Exception:
                pass
            await asyncio.sleep(2)
    raise RuntimeError(f"App not healthy after {max_wait}s")


async def build_accounts(
    db: DBReader, token_mgr: TokenManager
) -> list[dict]:
    """Build per-user account info with calendar clients."""
    users = await db.get_users()
    accounts = []

    for user in users:
        user_id = user["id"]
        tokens = await db.get_oauth_tokens(user_id)
        calendars = await db.get_client_calendars(user_id)

        clients: dict[str, CalendarTestClient] = {}
        for token in tokens:
            try:
                creds = await token_mgr.get_credentials(token["id"])
                email = token["google_account_email"]
                clients[email] = CalendarTestClient(email, creds)
            except Exception as e:
                logger.warning(
                    "Failed to build client for token %d: %s", token["id"], e
                )

        # Find main calendar
        main_cal_id = user.get("main_calendar_id")
        main_email = user.get("email")

        # Build calendar list with their clients
        cal_info = []
        for cal in calendars:
            token = next(
                (t for t in tokens if t["id"] == cal["oauth_token_id"]), None
            )
            if token and token["google_account_email"] in clients:
                cal_info.append({
                    "calendar": cal,
                    "client": clients[token["google_account_email"]],
                    "google_calendar_id": cal["google_calendar_id"],
                    "calendar_type": cal.get("calendar_type", "client"),
                })

        accounts.append({
            "user": user,
            "user_id": user_id,
            "email": main_email,
            "main_calendar_id": main_cal_id,
            "main_client": clients.get(main_email),
            "tokens": tokens,
            "calendars": cal_info,
            "clients": clients,
        })

    return accounts


def collect_tests() -> list:
    """Import and collect all test cases."""
    from sidecar.tests.test_busy_blocks import TESTS as busy_blocks
    from sidecar.tests.test_client_to_main import TESTS as client_to_main
    from sidecar.tests.test_deletion import TESTS as deletion
    from sidecar.tests.test_edge_cases import TESTS as edge_cases
    from sidecar.tests.test_edit_protection import TESTS as edit_protection
    from sidecar.tests.test_full_state import TESTS as full_state
    from sidecar.tests.test_multi_calendar import TESTS as multi_calendar
    from sidecar.tests.test_personal import TESTS as personal
    from sidecar.tests.test_recurring import TESTS as recurring
    from sidecar.tests.test_rsvp import TESTS as rsvp
    from sidecar.tests.test_self_healing import TESTS as self_healing
    from sidecar.tests.test_sync_control import TESTS as sync_control

    all_tests = []
    for suite in [
        client_to_main, busy_blocks, recurring, personal, rsvp,
        edit_protection, deletion, self_healing, edge_cases,
        multi_calendar, sync_control, full_state,
    ]:
        all_tests.extend(suite)

    logger.info("Collected %d tests", len(all_tests))
    return all_tests


async def main() -> None:
    config = Config()
    run_id = uuid.uuid4().hex[:8]
    logger.info("Sidecar starting (run_id=%s)", run_id)

    # 1. Load encryption key
    key = open(config.ENCRYPTION_KEY_FILE, "rb").read()
    encryption = EncryptionManager(key)
    session_secret = derive_session_secret(key)

    # 2. Connect to DB
    db = DBReader(config.DATABASE_PATH)
    await db.connect()

    # 3. Initialize token manager
    token_mgr = TokenManager(db, encryption)
    await token_mgr.init()

    # 4. Get admin user and forge session
    admin = await db.get_admin_user()
    if not admin:
        raise RuntimeError("No admin user found in database")

    session_token = forge_session_token(
        admin["id"], admin["email"], session_secret
    )

    # 5. Wait for app
    await wait_for_app(config.APP_BASE_URL)

    # 6. Build API client
    api = APIClient(config.APP_BASE_URL, session_token)

    # 7. Build accounts
    accounts = await build_accounts(db, token_mgr)
    if not accounts:
        raise RuntimeError("No accounts found")
    logger.info("Built %d accounts", len(accounts))

    # 8. Run startup cleanup
    all_cal_clients: list[tuple[CalendarTestClient, str]] = []
    for acct in accounts:
        if acct["main_client"] and acct["main_calendar_id"]:
            all_cal_clients.append(
                (acct["main_client"], acct["main_calendar_id"])
            )
        for ci in acct["calendars"]:
            # Skip personal calendars — sidecar only has read access
            if ci.get("calendar_type") == "personal":
                continue
            all_cal_clients.append(
                (ci["client"], ci["google_calendar_id"])
            )

    logger.info("Running startup cleanup sweep...")
    deleted = await CleanupManager.sweep_all(
        all_cal_clients, include_sentinels=True, time_window_days=30,
    )
    logger.info("Startup cleanup: removed %d test events (incl. sentinels)", deleted)

    # 9. Build test context
    waiter = SyncWaiter(
        timeout=config.SYNC_WAIT_TIMEOUT,
        poll_interval=config.POLL_INTERVAL,
    )
    factory = EventFactory(run_id)
    cleanup = CleanupManager()

    ctx = TestContext(
        api=api,
        db=db,
        waiter=waiter,
        factory=factory,
        cleanup=cleanup,
        run_id=run_id,
        accounts=accounts,
    )

    # 10. Collect tests
    tests = collect_tests()

    # 11. Load previous results
    prev_results = load_today_results()
    for r in prev_results:
        set_state("results", [])  # clear default
    state_results = prev_results
    set_state("results", state_results)

    # 12. Build runner
    async def on_result(result: TestResult) -> None:
        add_result(result)

    runner = SoakRunner(
        tests, ctx,
        min_delay=config.SOAK_MIN_DELAY,
        max_delay=config.SOAK_MAX_DELAY,
        on_result=on_result,
    )
    set_state("runner", runner)

    # 13. Handle shutdown
    shutdown_event = asyncio.Event()

    def handle_signal(*_):
        logger.info("Shutdown signal received")
        runner.request_shutdown()
        shutdown_event.set()

    loop = asyncio.get_running_loop()
    for sig in (signal.SIGTERM, signal.SIGINT):
        loop.add_signal_handler(sig, handle_signal)

    # 14. Start dashboard
    uvicorn_config = uvicorn.Config(
        app, host="0.0.0.0", port=config.DASHBOARD_PORT,
        log_level="warning",
    )
    server = uvicorn.Server(uvicorn_config)
    dashboard_task = asyncio.create_task(server.serve())

    # 15. Start soak runner
    runner_task = asyncio.create_task(runner.run())

    # 16. Start sentinel manager
    sentinel_mgr = SentinelManager(ctx, on_result=on_result)
    sentinel_task = asyncio.create_task(sentinel_mgr.run())

    logger.info(
        "Sidecar running: dashboard on :%d, %d tests loaded, sentinels active",
        config.DASHBOARD_PORT, len(tests),
    )

    # Wait for either to finish
    done, pending = await asyncio.wait(
        [dashboard_task, runner_task, sentinel_task,
         asyncio.create_task(shutdown_event.wait())],
        return_when=asyncio.FIRST_COMPLETED,
    )

    # Shutdown
    logger.info("Shutting down...")
    runner.request_shutdown()
    sentinel_mgr.request_shutdown()

    # Wait for runner to finish current test (up to 5 min)
    try:
        await asyncio.wait_for(runner_task, timeout=300)
    except asyncio.TimeoutError:
        logger.warning("Runner didn't stop in time")

    # Final cleanup
    logger.info("Running shutdown cleanup sweep...")
    await CleanupManager.sweep_all(
        all_cal_clients, include_sentinels=True, time_window_days=30,
    )

    # Stop dashboard
    server.should_exit = True
    try:
        await asyncio.wait_for(dashboard_task, timeout=5)
    except (asyncio.TimeoutError, Exception):
        pass

    await api.close()
    await db.close()
    logger.info("Sidecar stopped")


if __name__ == "__main__":
    asyncio.run(main())
