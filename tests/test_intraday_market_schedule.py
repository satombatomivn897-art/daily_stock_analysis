# -*- coding: utf-8 -*-
"""Tests for intraday market slot resolution."""

from datetime import datetime, timezone
import unittest

from src.core.intraday_market_schedule import resolve_intraday_slots


class IntradayMarketScheduleTestCase(unittest.TestCase):
    def test_resolve_cn_open_auction_slot(self) -> None:
        slots = resolve_intraday_slots(
            now_utc=datetime(2026, 3, 26, 1, 35, tzinfo=timezone.utc),
            tolerance_minutes=20,
        )

        self.assertEqual([(slot.region, slot.label) for slot in slots], [("cn", "09:30")])
        self.assertEqual(slots[0].stage, "auction")
        self.assertFalse(slots[0].is_final)

    def test_resolve_cn_close_slot(self) -> None:
        slots = resolve_intraday_slots(
            now_utc=datetime(2026, 3, 26, 7, 5, tzinfo=timezone.utc),
            tolerance_minutes=20,
        )

        self.assertEqual([(slot.region, slot.label) for slot in slots], [("cn", "15:00")])
        self.assertEqual(slots[0].stage, "close")
        self.assertTrue(slots[0].is_final)

    def test_skip_cn_lunch_break(self) -> None:
        slots = resolve_intraday_slots(
            now_utc=datetime(2026, 3, 26, 4, 5, tzinfo=timezone.utc),
            tolerance_minutes=20,
        )

        self.assertEqual(slots, [])

    def test_resolve_us_dst_open_slot(self) -> None:
        slots = resolve_intraday_slots(
            now_utc=datetime(2026, 3, 26, 13, 40, tzinfo=timezone.utc),
            tolerance_minutes=20,
        )

        self.assertEqual([(slot.region, slot.label) for slot in slots], [("us", "09:30")])
        self.assertEqual(slots[0].stage, "open")

    def test_resolve_us_standard_time_open_slot(self) -> None:
        slots = resolve_intraday_slots(
            now_utc=datetime(2026, 1, 5, 14, 40, tzinfo=timezone.utc),
            tolerance_minutes=20,
        )

        self.assertEqual([(slot.region, slot.label) for slot in slots], [("us", "09:30")])
        self.assertEqual(slots[0].stage, "open")


if __name__ == "__main__":
    unittest.main()
