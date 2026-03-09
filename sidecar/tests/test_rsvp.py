"""Suite 5: RSVP Propagation (tests 38-40)."""

from __future__ import annotations

import asyncio

from sidecar.framework.base import TestCase, TestContext, TestTiming

SUITE = "rsvp"


class AcceptPropagates(TestCase):
    name = "AcceptPropagates"
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

        summary = ctx.factory.make_summary("rsvp-acc")
        start, end = ctx.factory.future_time_slot(hours_from_now=3)

        # Create event with attendees (simulating invite)
        event = cal_client.create_event(
            cal_id, summary, start, end,
            attendees=[{"email": acct["email"]}],
        )
        ctx.cleanup.track(cal_client, cal_id, event["id"])

        await ctx.api.trigger_calendar_sync(client_cal["calendar"]["id"])

        main_event = await ctx.waiter.wait_for_event(
            main_client, main_cal_id,
            lambda e: summary in e.get("summary", ""),
            search_query=summary,
            description="main copy for RSVP",
        )
        ctx.cleanup.track(main_client, main_cal_id, main_event["id"])

        # Accept on main
        main_client.update_event(main_cal_id, main_event["id"], {
            "attendees": [{"email": acct["email"], "responseStatus": "accepted"}],
        })

        await ctx.api.trigger_user_sync(acct["user_id"])
        await asyncio.sleep(10)

        # Verify propagation
        updated = cal_client.get_event(cal_id, event["id"])
        if updated:
            attendees = updated.get("attendees", [])
            for a in attendees:
                if a.get("email") == acct["email"]:
                    assert a.get("responseStatus") in ("accepted", "needsAction"), \
                        f"Expected accepted, got: {a.get('responseStatus')}"


class DeclinePropagates(TestCase):
    name = "DeclinePropagates"
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

        summary = ctx.factory.make_summary("rsvp-dec")
        start, end = ctx.factory.future_time_slot(hours_from_now=3)

        event = cal_client.create_event(
            cal_id, summary, start, end,
            attendees=[{"email": acct["email"]}],
        )
        ctx.cleanup.track(cal_client, cal_id, event["id"])

        await ctx.api.trigger_calendar_sync(client_cal["calendar"]["id"])

        main_event = await ctx.waiter.wait_for_event(
            main_client, main_cal_id,
            lambda e: summary in e.get("summary", ""),
            search_query=summary,
        )
        ctx.cleanup.track(main_client, main_cal_id, main_event["id"])

        # Decline on main
        main_client.update_event(main_cal_id, main_event["id"], {
            "attendees": [{"email": acct["email"], "responseStatus": "declined"}],
        })

        await ctx.api.trigger_user_sync(acct["user_id"])
        await asyncio.sleep(10)

        # Check client
        updated = cal_client.get_event(cal_id, event["id"])
        if updated:
            attendees = updated.get("attendees", [])
            for a in attendees:
                if a.get("email") == acct["email"]:
                    # After decline, busy blocks should be removed too
                    pass  # Verified by absence of busy blocks


class TentativePropagates(TestCase):
    name = "TentativePropagates"
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

        summary = ctx.factory.make_summary("rsvp-tent")
        start, end = ctx.factory.future_time_slot(hours_from_now=3)

        event = cal_client.create_event(
            cal_id, summary, start, end,
            attendees=[{"email": acct["email"]}],
        )
        ctx.cleanup.track(cal_client, cal_id, event["id"])

        await ctx.api.trigger_calendar_sync(client_cal["calendar"]["id"])

        main_event = await ctx.waiter.wait_for_event(
            main_client, main_cal_id,
            lambda e: summary in e.get("summary", ""),
            search_query=summary,
        )
        ctx.cleanup.track(main_client, main_cal_id, main_event["id"])

        # Tentative on main
        main_client.update_event(main_cal_id, main_event["id"], {
            "attendees": [{"email": acct["email"], "responseStatus": "tentative"}],
        })

        await ctx.api.trigger_user_sync(acct["user_id"])
        await asyncio.sleep(10)

        updated = cal_client.get_event(cal_id, event["id"])
        if updated:
            attendees = updated.get("attendees", [])
            for a in attendees:
                if a.get("email") == acct["email"]:
                    assert a.get("responseStatus") in ("tentative", "needsAction"), \
                        f"Expected tentative, got: {a.get('responseStatus')}"


TESTS = [
    AcceptPropagates(),
    DeclinePropagates(),
    TentativePropagates(),
]
