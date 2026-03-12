"""Suite 7: Deletion Scenarios (tests 47-49)."""

from __future__ import annotations

import asyncio

from sidecar.framework.base import TestCase, TestContext, TestTiming

SUITE = "deletion"


def _get_two_client_cals(ctx: TestContext):
    for acct in ctx.accounts:
        clients = [c for c in acct["calendars"] if c["calendar_type"] == "client"]
        if len(clients) >= 2:
            return acct, clients[0], clients[1]
    raise RuntimeError("Need 2 client calendars for deletion tests")


class ClientDeleteCleansAll(TestCase):
    name = "ClientDeleteCleansAll"
    suite = SUITE
    timing = TestTiming.NORMAL

    async def run(self, ctx: TestContext) -> None:
        acct, cal_a, cal_b = _get_two_client_cals(ctx)
        main_client = acct["main_client"]
        main_cal_id = acct["main_calendar_id"]

        summary = ctx.factory.make_summary("del-all")
        start, end = ctx.factory.future_time_slot(hours_from_now=3)

        event = cal_a["client"].create_event(
            cal_a["google_calendar_id"], summary, start, end,
        )
        ctx.cleanup.track(cal_a["client"], cal_a["google_calendar_id"], event["id"])

        main_event = await ctx.waiter.wait_for_event(
            main_client, main_cal_id,
            lambda e: summary in e.get("summary", ""),
            search_query=summary,
        )
        ctx.cleanup.track(main_client, main_cal_id, main_event["id"])

        busy = await ctx.waiter.wait_for_event(
            cal_b["client"], cal_b["google_calendar_id"],
            lambda e: "[BusyBridge]" in e.get("summary", "") and "Busy" in e.get("summary", ""),
            time_min=start, time_max=end,
        )
        busy_id = busy["id"]

        # Delete on client A
        cal_a["client"].delete_event(cal_a["google_calendar_id"], event["id"])

        # Main copy should be gone
        await ctx.waiter.wait_for_gone(
            main_client, main_cal_id,
            lambda e: e["id"] == main_event["id"],
            description="main copy after client delete",
        )

        # Busy block on B should be gone
        await ctx.waiter.wait_for_gone(
            cal_b["client"], cal_b["google_calendar_id"],
            lambda e: e["id"] == busy_id,
            description="busy block after client delete",
        )


class MainDeleteCleansBlocks(TestCase):
    name = "MainDeleteCleansBlocks"
    suite = SUITE
    timing = TestTiming.NORMAL

    async def run(self, ctx: TestContext) -> None:
        acct = ctx.accounts[0]
        main_client = acct["main_client"]
        main_cal_id = acct["main_calendar_id"]
        client_cals = [c for c in acct["calendars"] if c["calendar_type"] == "client"]
        if not client_cals:
            return

        summary = ctx.factory.make_summary("del-main")
        start, end = ctx.factory.future_time_slot(hours_from_now=3)

        # Create directly on main
        event = main_client.create_event(main_cal_id, summary, start, end)
        ctx.cleanup.track(main_client, main_cal_id, event["id"])

        # Wait for busy blocks
        busy_ids = []
        for cal in client_cals:
            try:
                busy = await ctx.waiter.wait_for_event(
                    cal["client"], cal["google_calendar_id"],
                    lambda e: "[BusyBridge]" in e.get("summary", "") and "Busy" in e.get("summary", ""),
                    time_min=start, time_max=end,
                    timeout=60,
                )
                busy_ids.append((cal, busy["id"]))
            except TimeoutError:
                pass

        # Delete on main
        main_client.delete_event(main_cal_id, event["id"])

        # Verify blocks removed
        for cal, bid in busy_ids:
            await ctx.waiter.wait_for_gone(
                cal["client"], cal["google_calendar_id"],
                lambda e, _bid=bid: e["id"] == _bid,
                description="busy block after main delete",
            )


class DBCleanedOnlyAfterRemoteDelete(TestCase):
    name = "DBCleanedOnlyAfterRemoteDelete"
    suite = SUITE
    timing = TestTiming.NORMAL

    async def run(self, ctx: TestContext) -> None:
        acct = ctx.accounts[0]
        client_cal = next(
            c for c in acct["calendars"] if c["calendar_type"] == "client"
        )
        cal_client = client_cal["client"]
        cal_id = client_cal["google_calendar_id"]
        main_client = acct["main_client"]
        main_cal_id = acct["main_calendar_id"]

        summary = ctx.factory.make_summary("del-db")
        start, end = ctx.factory.future_time_slot(hours_from_now=3)

        event = cal_client.create_event(cal_id, summary, start, end)
        ctx.cleanup.track(cal_client, cal_id, event["id"])

        main_event = await ctx.waiter.wait_for_event(
            main_client, main_cal_id,
            lambda e: summary in e.get("summary", ""),
            search_query=summary,
        )
        ctx.cleanup.track(main_client, main_cal_id, main_event["id"])

        # Check DB has mapping
        mappings = await ctx.db.get_event_mappings(acct["user_id"])
        found = [m for m in mappings if m.get("origin_event_id") == event["id"]]
        assert len(found) > 0, "No mapping found in DB after sync"

        # Delete
        cal_client.delete_event(cal_id, event["id"])

        await ctx.waiter.wait_for_gone(
            main_client, main_cal_id,
            lambda e: e["id"] == main_event["id"],
            description="main copy deleted",
        )

        # After deletion sync, DB mapping should be gone
        await asyncio.sleep(5)
        mappings = await ctx.db.get_event_mappings(acct["user_id"])
        found = [m for m in mappings if m.get("origin_event_id") == event["id"]]
        assert len(found) == 0, f"Mapping still in DB after delete: {found}"


TESTS = [
    ClientDeleteCleansAll(),
    MainDeleteCleansBlocks(),
    DBCleanedOnlyAfterRemoteDelete(),
]
