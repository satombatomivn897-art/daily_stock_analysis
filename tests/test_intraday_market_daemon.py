# -*- coding: utf-8 -*-
"""Tests for local intraday market daemon scheduling."""

from datetime import date, datetime
from zoneinfo import ZoneInfo
import unittest

from src.core.intraday_market_daemon import (
    build_execution_key,
    get_due_intraday_digest_triggers,
    get_intraday_digest_triggers,
)


class IntradayMarketDaemonTestCase(unittest.TestCase):
    def test_cn_trigger_plan_matches_delivery_rules(self) -> None:
        triggers = get_intraday_digest_triggers("cn")

        self.assertEqual(
            [(item.trigger_label, item.slot_label) for item in triggers],
            [
                ("09:24", "09:30"),
                ("10:30", "10:30"),
                ("11:30", "11:30"),
                ("13:30", "13:30"),
                ("14:30", "14:30"),
                ("15:03", "15:00"),
            ],
        )

    def test_auction_slot_runs_within_grace_window(self) -> None:
        now_local = datetime(2026, 3, 27, 9, 28, tzinfo=ZoneInfo("Asia/Shanghai"))

        due = get_due_intraday_digest_triggers(
            region="cn",
            now_local=now_local,
            executed_keys=set(),
            grace_minutes=10,
        )

        self.assertEqual([(item.trigger_label, item.slot_label) for item in due], [("09:24", "09:30")])

    def test_executed_key_prevents_duplicate_dispatch(self) -> None:
        now_local = datetime(2026, 3, 27, 10, 35, tzinfo=ZoneInfo("Asia/Shanghai"))
        trigger = get_intraday_digest_triggers("cn")[1]
        executed_keys = {build_execution_key(trigger, date(2026, 3, 27))}

        due = get_due_intraday_digest_triggers(
            region="cn",
            now_local=now_local,
            executed_keys=executed_keys,
            grace_minutes=10,
        )

        self.assertEqual(due, [])

    def test_close_slot_uses_post_close_trigger(self) -> None:
        now_local = datetime(2026, 3, 27, 15, 9, tzinfo=ZoneInfo("Asia/Shanghai"))

        due = get_due_intraday_digest_triggers(
            region="cn",
            now_local=now_local,
            executed_keys=set(),
            grace_minutes=10,
        )

        self.assertEqual([(item.trigger_label, item.slot_label) for item in due], [("15:03", "15:00")])


if __name__ == "__main__":
    unittest.main()
