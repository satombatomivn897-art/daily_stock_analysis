# -*- coding: utf-8 -*-
"""Tests for intraday market digest markdown rendering."""

import unittest

from src.core.intraday_market_digest import (
    IntradayDigestContext,
    IntradayDigestEntry,
    IntradayMarketSlot,
    render_intraday_digest_markdown,
)


class IntradayMarketDigestRenderTestCase(unittest.TestCase):
    def test_cn_open_digest_includes_auction_limit_up_section(self) -> None:
        slot = IntradayMarketSlot(
            region="cn",
            label="09:30",
            stage="auction",
            market_name="A股",
            local_date="2026-03-26",
            timezone_name="Asia/Shanghai",
            is_final=False,
        )
        report = render_intraday_digest_markdown(
            IntradayDigestContext(
                slot=slot,
                headline="A股开盘后情绪偏强，竞价资金向科技与机器人方向集中。",
                capital_flow_summary="大盘主力净流入转正，电子与机器人概念居前。",
                sector_leaders=[
                    IntradayDigestEntry(name="机器人", value="+3.20%", extra="净流入 6.8 亿"),
                ],
                stock_leaders=[
                    IntradayDigestEntry(name="力源信息", value="+20.04%", extra="华为海思概念"),
                ],
                auction_limit_ups=[
                    IntradayDigestEntry(name="某竞价涨停股", value="09:25 封板", extra="机器人"),
                ],
            )
        )

        self.assertIn("集合竞价观察", report)
        self.assertIn("竞价涨停 Top 10", report)
        self.assertNotIn("当日涨停 Top 20", report)

    def test_cn_close_digest_includes_daily_limit_up_reason_section(self) -> None:
        slot = IntradayMarketSlot(
            region="cn",
            label="15:00",
            stage="close",
            market_name="A股",
            local_date="2026-03-26",
            timezone_name="Asia/Shanghai",
            is_final=True,
        )
        report = render_intraday_digest_markdown(
            IntradayDigestContext(
                slot=slot,
                headline="A股收盘维持强势，短线情绪聚焦算力和机器人。",
                capital_flow_summary="主力资金净流入集中在算力、消费电子和机器人链。",
                sector_leaders=[
                    IntradayDigestEntry(name="算力", value="+5.80%", extra="净流入 18.3 亿"),
                ],
                stock_leaders=[
                    IntradayDigestEntry(name="某龙头股", value="+10.01%", extra="成交额 24.6 亿"),
                ],
                investment_direction="短线继续关注算力、机器人和高景气消费电子，但不追高，优先等待分歧回踩后的承接确认。",
                previous_day_comparison="与前一交易日相比，指数涨幅扩大，主力净流入由分散转为集中，情绪由轮动切换为主线强化。",
                weekly_summary="本周资金主攻算力、机器人和消费电子，强势股集中在高弹性科技方向，风险偏好整体抬升。",
                daily_limit_ups=[
                    IntradayDigestEntry(name="某涨停股", value="3 连板", extra="机器人催化"),
                ],
            )
        )

        self.assertIn("当日涨停 Top 20", report)
        self.assertIn("涨停原因简析", report)
        self.assertIn("前一交易日对比", report)
        self.assertIn("本周总结", report)


if __name__ == "__main__":
    unittest.main()
