"""Suite 12: Full-State Verification (tests 72-79)."""

from __future__ import annotations

import asyncio
from datetime import datetime

from sidecar.framework.base import TestCase, TestContext, TestTiming


def _times_match(iso1: str, iso2: str) -> bool:
    """Compare two ISO datetime strings as timezone-aware instants."""
    try:
        dt1 = datetime.fromisoformat(iso1)
        dt2 = datetime.fromisoformat(iso2)
        return abs((dt1 - dt2).total_seconds()) < 60
    except (ValueError, TypeError):
        return False

SUITE = "full_state"


class FullStateAfterClientEvent(TestCase):
    name = "FullStateAfterClientEvent"
    suite = SUITE
    timing = TestTiming.SLOW

    async def run(self, ctx: TestContext) -> None:
        acct = ctx.accounts[0]
        client_cals = [c for c in acct["calendars"] if c["calendar_type"] == "client"]
        if len(client_cals) < 2:
            return
        cal_a, cal_b = client_cals[0], client_cals[1]
        main_client = acct["main_client"]
        main_cal_id = acct["main_calendar_id"]

        summary = ctx.factory.make_summary("fs-client")
        start, end = ctx.factory.future_time_slot(hours_from_now=3)

        event = cal_a["client"].create_event(
            cal_a["google_calendar_id"], summary, start, end,
        )
        ctx.cleanup.track(cal_a["client"], cal_a["google_calendar_id"], event["id"])

        # Verify main has copy
        main_event = await ctx.waiter.wait_for_event(
            main_client, main_cal_id,
            lambda e: summary in e.get("summary", ""),
            search_query=summary,
        )
        ctx.cleanup.track(main_client, main_cal_id, main_event["id"])

        # Verify busy block on B
        busy_b = await ctx.waiter.wait_for_event(
            cal_b["client"], cal_b["google_calendar_id"],
            lambda e: "[BusyBridge]" in e.get("summary", "") and "Busy" in e.get("summary", ""),
            time_min=start, time_max=end,
            description="busy block on B",
        )
        ctx.cleanup.track(cal_b["client"], cal_b["google_calendar_id"], busy_b["id"])

        # Verify NO busy block on A (origin)
        events_a = cal_a["client"].list_events(
            cal_a["google_calendar_id"],
            q="BusyBridge Busy", time_min=start, time_max=end,
        )
        busys_a = [e for e in events_a if "[BusyBridge]" in e.get("summary", "")]
        assert len(busys_a) == 0, f"Busy block on origin calendar: {len(busys_a)}"

        # Verify DB
        mappings = await ctx.db.get_event_mappings(acct["user_id"])
        found = [m for m in mappings if m.get("origin_event_id") == event["id"]]
        assert len(found) > 0, "No event mapping in DB"

        if found:
            blocks = await ctx.db.get_busy_blocks(found[0]["id"])
            block_cal_ids = [b["client_calendar_id"] for b in blocks]
            assert cal_b["calendar"]["id"] in block_cal_ids, "No busy block for B in DB"


class FullStateAfterMainEvent(TestCase):
    name = "FullStateAfterMainEvent"
    suite = SUITE
    timing = TestTiming.SLOW

    async def run(self, ctx: TestContext) -> None:
        acct = ctx.accounts[0]
        client_cals = [c for c in acct["calendars"] if c["calendar_type"] == "client"]
        main_client = acct["main_client"]
        main_cal_id = acct["main_calendar_id"]

        summary = ctx.factory.make_summary("fs-main")
        start, end = ctx.factory.future_time_slot(hours_from_now=3)

        event = main_client.create_event(main_cal_id, summary, start, end)
        ctx.cleanup.track(main_client, main_cal_id, event["id"])

        for cal in client_cals:
            busy = await ctx.waiter.wait_for_event(
                cal["client"], cal["google_calendar_id"],
                lambda e: "[BusyBridge]" in e.get("summary", "") and "Busy" in e.get("summary", ""),
                time_min=start, time_max=end,
                description=f"busy block on {cal['google_calendar_id'][:20]}",
            )
            ctx.cleanup.track(cal["client"], cal["google_calendar_id"], busy["id"])


class FullStateAfterDelete(TestCase):
    name = "FullStateAfterDelete"
    suite = SUITE
    timing = TestTiming.SLOW

    async def run(self, ctx: TestContext) -> None:
        acct = ctx.accounts[0]
        client_cals = [c for c in acct["calendars"] if c["calendar_type"] == "client"]
        if len(client_cals) < 2:
            return
        cal_a, cal_b = client_cals[0], client_cals[1]
        main_client = acct["main_client"]
        main_cal_id = acct["main_calendar_id"]

        summary = ctx.factory.make_summary("fs-del")
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
        busy_b = await ctx.waiter.wait_for_event(
            cal_b["client"], cal_b["google_calendar_id"],
            lambda e: "[BusyBridge]" in e.get("summary", "") and "Busy" in e.get("summary", ""),
            time_min=start, time_max=end,
        )
        busy_b_id = busy_b["id"]
        main_id = main_event["id"]

        # Delete on client A
        cal_a["client"].delete_event(cal_a["google_calendar_id"], event["id"])

        await ctx.waiter.wait_for_gone(
            main_client, main_cal_id,
            lambda e: e["id"] == main_id,
            description="main event after delete",
        )

        await ctx.waiter.wait_for_gone(
            cal_b["client"], cal_b["google_calendar_id"],
            lambda e: e["id"] == busy_b_id,
            description="busy block after delete",
        )

        await asyncio.sleep(5)
        mappings = await ctx.db.get_event_mappings(acct["user_id"])
        found = [m for m in mappings if m.get("origin_event_id") == event["id"]]
        assert len(found) == 0, "Mapping still in DB after full delete"


class FullStateMultipleEvents(TestCase):
    name = "FullStateMultipleEvents"
    suite = SUITE
    timing = TestTiming.SLOW

    async def run(self, ctx: TestContext) -> None:
        acct = ctx.accounts[0]
        client_cals = [c for c in acct["calendars"] if c["calendar_type"] == "client"]
        if len(client_cals) < 2:
            return
        cal_a, cal_b = client_cals[0], client_cals[1]
        main_client = acct["main_client"]
        main_cal_id = acct["main_calendar_id"]

        summaries_a = [ctx.factory.make_summary(f"fs-multi-a{i}") for i in range(2)]
        summaries_b = [ctx.factory.make_summary("fs-multi-b0")]

        for s in summaries_a:
            start, end = ctx.factory.future_time_slot(hours_from_now=3 + summaries_a.index(s))
            event = cal_a["client"].create_event(
                cal_a["google_calendar_id"], s, start, end,
            )
            ctx.cleanup.track(cal_a["client"], cal_a["google_calendar_id"], event["id"])

        for s in summaries_b:
            start, end = ctx.factory.future_time_slot(hours_from_now=6)
            event = cal_b["client"].create_event(
                cal_b["google_calendar_id"], s, start, end,
            )
            ctx.cleanup.track(cal_b["client"], cal_b["google_calendar_id"], event["id"])

        for s in summaries_a + summaries_b:
            main_event = await ctx.waiter.wait_for_event(
                main_client, main_cal_id,
                lambda e, _s=s: _s in e.get("summary", ""),
                search_query=s,
                description=f"main copy of {s[:30]}",
            )
            ctx.cleanup.track(main_client, main_cal_id, main_event["id"])

        await asyncio.sleep(10)
        busy_on_b = cal_b["client"].list_events(
            cal_b["google_calendar_id"], q="BusyBridge Busy",
        )
        test_busys_b = [e for e in busy_on_b if "[BusyBridge]" in e.get("summary", "")]
        for e in test_busys_b:
            ctx.cleanup.track(cal_b["client"], cal_b["google_calendar_id"], e["id"])

        busy_on_a = cal_a["client"].list_events(
            cal_a["google_calendar_id"], q="BusyBridge Busy",
        )
        test_busys_a = [e for e in busy_on_a if "[BusyBridge]" in e.get("summary", "")]
        for e in test_busys_a:
            ctx.cleanup.track(cal_a["client"], cal_a["google_calendar_id"], e["id"])


class FullStateAfterEditableMove(TestCase):
    name = "FullStateAfterEditableMove"
    suite = SUITE
    timing = TestTiming.SLOW

    async def run(self, ctx: TestContext) -> None:
        acct = ctx.accounts[0]
        client_cals = [c for c in acct["calendars"] if c["calendar_type"] == "client"]
        if len(client_cals) < 2:
            return
        cal_a, cal_b = client_cals[0], client_cals[1]
        main_client = acct["main_client"]
        main_cal_id = acct["main_calendar_id"]

        summary = ctx.factory.make_summary("fs-move")
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

        busy_b = await ctx.waiter.wait_for_event(
            cal_b["client"], cal_b["google_calendar_id"],
            lambda e: "[BusyBridge]" in e.get("summary", "") and "Busy" in e.get("summary", ""),
            time_min=start, time_max=end,
        )
        ctx.cleanup.track(cal_b["client"], cal_b["google_calendar_id"], busy_b["id"])

        # Move on main
        new_start, new_end = ctx.factory.future_time_slot(hours_from_now=6)
        main_client.update_event(main_cal_id, main_event["id"], {
            "start": {"dateTime": new_start, "timeZone": "America/New_York"},
            "end": {"dateTime": new_end, "timeZone": "America/New_York"},
        })

        await ctx.waiter.wait_for_event_updated(
            cal_a["client"], cal_a["google_calendar_id"],
            lambda e: e["id"] == event["id"],
            lambda e: _times_match(new_start, e.get("start", {}).get("dateTime", "")),
            description="moved client event",
            timeout=180,
        )

        updated_busy = await ctx.waiter.wait_for_event(
            cal_b["client"], cal_b["google_calendar_id"],
            lambda e: "[BusyBridge]" in e.get("summary", "") and "Busy" in e.get("summary", ""),
            time_min=new_start, time_max=new_end,
            description="moved busy block",
        )
        ctx.cleanup.track(cal_b["client"], cal_b["google_calendar_id"], updated_busy["id"])


class FullStateAfterNonEditableMove(TestCase):
    name = "FullStateAfterNonEditableMove"
    suite = SUITE
    timing = TestTiming.SLOW

    async def run(self, ctx: TestContext) -> None:
        acct = ctx.accounts[0]
        client_cal = next(
            c for c in acct["calendars"] if c["calendar_type"] == "client"
        )
        cal_client = client_cal["client"]
        cal_id = client_cal["google_calendar_id"]
        main_client = acct["main_client"]
        main_cal_id = acct["main_calendar_id"]

        summary = ctx.factory.make_summary("fs-ne-move")
        start, end = ctx.factory.future_time_slot(hours_from_now=3)

        event = cal_client.create_event(cal_id, summary, start, end)
        ctx.cleanup.track(cal_client, cal_id, event["id"])

        main_event = await ctx.waiter.wait_for_event(
            main_client, main_cal_id,
            lambda e: summary in e.get("summary", ""),
            search_query=summary,
        )
        ctx.cleanup.track(main_client, main_cal_id, main_event["id"])
        assert summary in main_event["summary"]


class FullStateConsistencyCheck(TestCase):
    name = "FullStateConsistencyCheck"
    suite = SUITE
    timing = TestTiming.SLOW

    async def run(self, ctx: TestContext) -> None:
        acct = ctx.accounts[0]
        client_cal = next(
            c for c in acct["calendars"] if c["calendar_type"] == "client"
        )
        cal_client = client_cal["client"]
        cal_id = client_cal["google_calendar_id"]
        main_client = acct["main_client"]
        main_cal_id = acct["main_calendar_id"]

        summary = ctx.factory.make_summary("fs-consist")
        start, end = ctx.factory.future_time_slot(hours_from_now=3)

        event = cal_client.create_event(cal_id, summary, start, end)
        ctx.cleanup.track(cal_client, cal_id, event["id"])

        main_event = await ctx.waiter.wait_for_event(
            main_client, main_cal_id,
            lambda e: summary in e.get("summary", ""),
            search_query=summary,
        )
        ctx.cleanup.track(main_client, main_cal_id, main_event["id"])

        mappings = await ctx.db.get_event_mappings(acct["user_id"])
        found = [m for m in mappings if m.get("origin_event_id") == event["id"]]
        assert len(found) == 1, f"Expected 1 mapping, found {len(found)}"

        mapping = found[0]
        assert mapping["main_event_id"] == main_event["id"], \
            f"DB main_event_id mismatch: {mapping['main_event_id']} != {main_event['id']}"


TESTS = [
    FullStateAfterClientEvent(),
    FullStateAfterMainEvent(),
    FullStateAfterDelete(),
    FullStateMultipleEvents(),
    FullStateAfterEditableMove(),
    FullStateAfterNonEditableMove(),
    FullStateConsistencyCheck(),
]
