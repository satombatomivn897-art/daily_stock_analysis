# -*- coding: utf-8 -*-
"""Intraday market digest data structures and markdown rendering."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import List

from src.core.intraday_market_schedule import IntradayMarketSlot


@dataclass(frozen=True)
class IntradayDigestEntry:
    """Compact digest row for sectors, stocks, and limit-up pools."""

    name: str
    value: str
    extra: str = ""


@dataclass
class IntradayDigestContext:
    """Render-ready context for one intraday market digest."""

    slot: IntradayMarketSlot
    headline: str = ""
    indices_summary: str = ""
    market_stats_summary: str = ""
    capital_flow_summary: str = ""
    sector_leaders: List[IntradayDigestEntry] = field(default_factory=list)
    stock_leaders: List[IntradayDigestEntry] = field(default_factory=list)
    auction_limit_ups: List[IntradayDigestEntry] = field(default_factory=list)
    daily_limit_ups: List[IntradayDigestEntry] = field(default_factory=list)
    news_highlights: List[str] = field(default_factory=list)
    investment_direction: str = ""
    previous_day_comparison: str = ""
    weekly_summary: str = ""
    disclaimer: str = "建议仅供参考，不构成投资建议。"


def _render_entry_list(entries: List[IntradayDigestEntry]) -> List[str]:
    lines: List[str] = []
    for index, entry in enumerate(entries, 1):
        suffix = f" | {entry.extra}" if entry.extra else ""
        lines.append(f"{index}. **{entry.name}**: {entry.value}{suffix}")
    return lines


def render_intraday_digest_markdown(context: IntradayDigestContext) -> str:
    """Render a detailed markdown report for email / PDF delivery."""

    slot = context.slot
    heading = (
        f"## {slot.local_date} {slot.market_name}{slot.label} 收盘综述"
        if slot.is_final
        else f"## {slot.local_date} {slot.market_name}{slot.label} 盘中综述"
    )

    lines: List[str] = [heading, ""]

    if context.headline:
        lines.extend(["### 一、核心结论", context.headline, ""])

    if context.indices_summary or context.market_stats_summary:
        lines.append("### 二、指数与市场面")
        if context.indices_summary:
            lines.append(context.indices_summary)
        if context.market_stats_summary:
            lines.append(context.market_stats_summary)
        lines.append("")

    if context.capital_flow_summary:
        lines.extend(["### 三、资金流向", context.capital_flow_summary, ""])

    if context.sector_leaders:
        lines.extend(["### 四、行业与主题强弱", *_render_entry_list(context.sector_leaders), ""])

    if context.stock_leaders:
        lines.extend(["### 五、领涨个股", *_render_entry_list(context.stock_leaders), ""])

    if slot.region == "cn" and slot.stage == "auction":
        lines.append("### 六、集合竞价观察")
        if context.auction_limit_ups:
            lines.extend(["#### 竞价涨停 Top 10", *_render_entry_list(context.auction_limit_ups)])
        else:
            lines.append("暂无竞价涨停名单。")
        lines.append("")
    elif slot.region == "cn" and slot.is_final:
        lines.append("### 六、涨停复盘")
        if context.daily_limit_ups:
            lines.extend(
                [
                    "#### 当日涨停 Top 20",
                    *_render_entry_list(context.daily_limit_ups),
                    "",
                    "#### 涨停原因简析",
                    "结合行业催化、资金共振、连板结构与新闻事件，逐只解释强势封板的主要驱动。",
                ]
            )
        else:
            lines.append("暂无当日涨停复盘数据。")
        lines.append("")

    if context.news_highlights:
        lines.extend(["### 七、新闻与催化", *[f"- {item}" for item in context.news_highlights], ""])

    if context.investment_direction:
        lines.extend(["### 八、投资方向建议", context.investment_direction, ""])

    if context.previous_day_comparison:
        lines.extend(["### 九、前一交易日对比", context.previous_day_comparison, ""])

    if context.weekly_summary:
        lines.extend(["### 十、本周总结", context.weekly_summary, ""])

    lines.extend(["### 十一、提示", context.disclaimer])
    return "\n".join(lines).strip() + "\n"


__all__ = [
    "IntradayDigestContext",
    "IntradayDigestEntry",
    "IntradayMarketSlot",
    "render_intraday_digest_markdown",
]
