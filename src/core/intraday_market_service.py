# -*- coding: utf-8 -*-
"""Intraday market digest service."""

from __future__ import annotations

import logging
import re
from collections import Counter
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple

import pandas as pd

from data_provider.base import DataFetcherManager
from src.core.intraday_market_digest import (
    IntradayDigestContext,
    IntradayDigestEntry,
    render_intraday_digest_markdown,
)
from src.core.intraday_market_schedule import (
    IntradayMarketSlot,
    build_intraday_slot,
    resolve_intraday_slots,
)
from src.market_analyzer import MarketAnalyzer


logger = logging.getLogger(__name__)


def _safe_float(value: Any) -> Optional[float]:
    if value is None:
        return None
    if isinstance(value, (int, float)):
        try:
            return float(value)
        except (TypeError, ValueError):
            return None
    text = str(value).strip().replace(",", "").replace("%", "")
    if not text or text in {"-", "None", "nan"}:
        return None
    try:
        return float(text)
    except (TypeError, ValueError):
        return None


def _safe_str(value: Any) -> str:
    if value is None:
        return ""
    return str(value).strip()


def _pick_column(columns: Iterable[Any], keywords: Sequence[str]) -> Optional[str]:
    for column in columns:
        column_text = str(column)
        if any(keyword in column_text for keyword in keywords):
            return str(column)
    return None


def _format_money(value: Optional[float], unit_hint: str = "") -> str:
    if value is None:
        return "暂无数据"
    numeric = float(value)
    if abs(numeric) >= 1_000_000:
        return f"{numeric / 1e8:+.2f}亿"
    if unit_hint == "usd":
        if abs(numeric) >= 1_000_000_000:
            return f"{numeric / 1e9:.2f}B 美元"
        if abs(numeric) >= 1_000_000:
            return f"{numeric / 1e6:.0f}M 美元"
        return f"{numeric:.0f} 美元"
    return f"{numeric:+.2f}亿"


def _format_percent(value: Optional[float]) -> str:
    if value is None:
        return "暂无数据"
    return f"{float(value):+.2f}%"


def _parse_limit_strength(value: Any) -> int:
    text = _safe_str(value)
    if not text:
        return 0
    match = re.match(r"(\d+)", text)
    return int(match.group(1)) if match else 0


def _normalize_time_text(value: Any) -> str:
    text = _safe_str(value).replace(":", "")
    if len(text) == 6 and text.isdigit():
        return f"{text[:2]}:{text[2:4]}"
    return _safe_str(value)


def _today_compact(slot: IntradayMarketSlot) -> str:
    return slot.local_date.replace("-", "")


class IntradayMarketDigestService:
    """Generate and send intraday market digest reports."""

    _US_SECTOR_ETFS: Tuple[Tuple[str, str], ...] = (
        ("XLK", "科技"),
        ("SMH", "半导体"),
        ("XLE", "能源"),
        ("XLF", "金融"),
        ("XLV", "医疗"),
        ("XLI", "工业"),
        ("XLY", "可选消费"),
        ("XLP", "必选消费"),
        ("XLC", "通信服务"),
        ("XLB", "材料"),
        ("XLU", "公用事业"),
        ("XLRE", "房地产"),
    )

    def __init__(self, notifier, analyzer=None, search_service=None):
        self.notifier = notifier
        self.analyzer = analyzer
        self.search_service = search_service
        self.data_manager = DataFetcherManager()
        self.reports_dir = Path(__file__).resolve().parents[2] / "reports"
        self.reports_dir.mkdir(parents=True, exist_ok=True)

    def run(
        self,
        *,
        region_override: str = "auto",
        slot_override: str = "auto",
        send_notification: bool = True,
        tolerance_minutes: int = 20,
    ) -> List[str]:
        slots = self._resolve_slots(
            region_override=region_override,
            slot_override=slot_override,
            tolerance_minutes=tolerance_minutes,
        )
        if not slots:
            logger.info("当前未命中盘中大盘综述执行窗口，跳过。")
            return []

        reports: List[str] = []
        for slot in slots:
            context = self._build_context(slot)
            report = self._generate_report(context)
            filename = self._report_filename(slot)
            self.notifier.save_report_to_file(report, filename)
            if send_notification and self.notifier.is_available():
                self.notifier.send(report, email_send_to_all=True)
            reports.append(report)
        return reports

    def _resolve_slots(
        self,
        *,
        region_override: str,
        slot_override: str,
        tolerance_minutes: int,
    ) -> List[IntradayMarketSlot]:
        if region_override != "auto":
            if slot_override == "auto":
                return [
                    slot
                    for slot in resolve_intraday_slots(tolerance_minutes=tolerance_minutes)
                    if slot.region == region_override
                ]
            return [build_intraday_slot(region_override, slot_override)]
        if slot_override != "auto":
            raise ValueError("intraday slot override requires an explicit intraday region")
        return resolve_intraday_slots(tolerance_minutes=tolerance_minutes)

    def _build_context(self, slot: IntradayMarketSlot) -> IntradayDigestContext:
        if slot.region == "cn":
            return self._build_cn_context(slot)
        return self._build_us_context(slot)

    def _generate_report(self, context: IntradayDigestContext) -> str:
        enriched = self._enrich_context_with_llm(context)
        return render_intraday_digest_markdown(enriched)

    def _build_cn_context(self, slot: IntradayMarketSlot) -> IntradayDigestContext:
        market_analyzer = MarketAnalyzer(
            search_service=self.search_service,
            analyzer=self.analyzer,
            region="cn",
        )
        overview = market_analyzer.get_market_overview()
        news = market_analyzer.search_market_news() if self.search_service else []
        flow_bundle = self._get_cn_flow_bundle()
        sector_leaders = self._get_cn_sector_leaders()
        stock_leaders = self._get_cn_stock_leaders()
        flow_stock_leaders = self._get_cn_flow_stock_leaders()
        auction_limit_ups = self._get_cn_limit_up_pool(slot, top_n=10, auction_only=True) if slot.stage == "auction" else []
        daily_limit_ups = self._get_cn_limit_up_pool(slot, top_n=20, auction_only=False) if slot.is_final else []
        previous_day_comparison = (
            self._build_cn_previous_day_comparison(flow_bundle, sector_leaders, slot.local_date)
            if slot.is_final
            else ""
        )
        weekly_summary = (
            self._build_cn_weekly_summary(slot, flow_bundle, sector_leaders, daily_limit_ups)
            if slot.is_final and self._is_friday(slot)
            else ""
        )

        return IntradayDigestContext(
            slot=slot,
            headline=self._build_cn_headline(overview, sector_leaders, stock_leaders),
            indices_summary=self._format_indices_summary(overview.indices),
            market_stats_summary=self._format_cn_market_stats(overview),
            capital_flow_summary=self._build_cn_capital_flow_summary(flow_bundle, flow_stock_leaders, sector_leaders),
            sector_leaders=sector_leaders,
            stock_leaders=stock_leaders,
            auction_limit_ups=auction_limit_ups,
            daily_limit_ups=daily_limit_ups,
            news_highlights=self._extract_news_highlights(news),
            investment_direction=self._build_cn_investment_direction(slot, flow_bundle, sector_leaders, stock_leaders),
            previous_day_comparison=previous_day_comparison,
            weekly_summary=weekly_summary,
        )

    def _build_us_context(self, slot: IntradayMarketSlot) -> IntradayDigestContext:
        market_analyzer = MarketAnalyzer(
            search_service=self.search_service,
            analyzer=self.analyzer,
            region="us",
        )
        overview = market_analyzer.get_market_overview()
        news = market_analyzer.search_market_news() if self.search_service else []
        us_spot = self._get_us_spot_frame()
        sector_leaders = self._get_us_sector_leaders()
        stock_leaders = self._get_us_stock_leaders(us_spot)
        turnover_entries = self._get_us_turnover_leaders(us_spot)

        return IntradayDigestContext(
            slot=slot,
            headline=self._build_us_headline(overview, sector_leaders, stock_leaders),
            indices_summary=self._format_indices_summary(overview.indices),
            market_stats_summary=self._format_us_market_stats(turnover_entries),
            capital_flow_summary=self._build_us_capital_flow_summary(sector_leaders, turnover_entries),
            sector_leaders=sector_leaders,
            stock_leaders=stock_leaders,
            news_highlights=self._extract_news_highlights(news),
            investment_direction=self._build_us_investment_direction(sector_leaders, stock_leaders),
        )

    def _build_cn_headline(
        self,
        overview,
        sector_leaders: List[IntradayDigestEntry],
        stock_leaders: List[IntradayDigestEntry],
    ) -> str:
        lead_index = overview.indices[0] if overview.indices else None
        mood = "偏强" if lead_index and (lead_index.change_pct or 0) >= 0 else "偏弱"
        breadth = "普涨" if overview.up_count > overview.down_count else "分化"
        top_sector = sector_leaders[0].name if sector_leaders else "热点轮动"
        top_stock = stock_leaders[0].name if stock_leaders else "强势股"
        return (
            f"当前 A 股盘面整体 {mood}，市场情绪以 {breadth} 为主，主线集中在 {top_sector}。"
            f" 领涨个股中以 {top_stock} 为代表，短线资金更偏好高弹性科技与题材共振方向。"
        )

    def _build_us_headline(
        self,
        overview,
        sector_leaders: List[IntradayDigestEntry],
        stock_leaders: List[IntradayDigestEntry],
    ) -> str:
        lead_index = overview.indices[0] if overview.indices else None
        mood = "risk-on" if lead_index and (lead_index.change_pct or 0) >= 0 else "risk-off"
        top_sector = sector_leaders[0].name if sector_leaders else "broad market"
        top_stock = stock_leaders[0].name if stock_leaders else "top movers"
        return (
            f"当前美股更接近 {mood} 风格，盘面强势集中在 {top_sector}。"
            f" 高弹性成交集中在 {top_stock} 等强势股，适合把注意力放在主线行业与高流动性龙头。"
        )

    def _build_cn_capital_flow_summary(
        self,
        flow_bundle: Dict[str, Any],
        flow_stock_leaders: List[IntradayDigestEntry],
        sector_leaders: List[IntradayDigestEntry],
    ) -> str:
        latest = flow_bundle.get("latest") or {}
        main_flow = _safe_float(latest.get("main_net_inflow"))
        main_pct = _safe_float(latest.get("main_net_ratio"))
        northbound = _safe_float(flow_bundle.get("northbound_net_inflow"))
        sector_text = "、".join(entry.name for entry in sector_leaders[:3]) or "暂无明显集中方向"
        stock_text = "、".join(entry.name for entry in flow_stock_leaders[:3]) or "暂无明显个股集中"
        return (
            f"大盘主力净流入 { _format_money(main_flow) }，主力净占比 {_format_percent(main_pct)}，"
            f"北向资金净流入 { _format_money(northbound) }。"
            f" 当前资金更集中流向 {sector_text} 等方向，个股层面重点活跃在 {stock_text}。"
        )

    def _build_us_capital_flow_summary(
        self,
        sector_leaders: List[IntradayDigestEntry],
        turnover_entries: List[IntradayDigestEntry],
    ) -> str:
        sector_text = "、".join(entry.name for entry in sector_leaders[:3]) or "暂无明显主线"
        turnover_text = "、".join(entry.name for entry in turnover_entries[:3]) or "暂无明显集中"
        return (
            "美股暂无统一公开的盘中主力净流入口径，本报告以行业 ETF 强弱和高成交额个股作为资金代理观察。"
            f" 当前成交额与换手更集中在 {turnover_text}，行业强弱上由 {sector_text} 领跑。"
        )

    def _build_cn_investment_direction(
        self,
        slot: IntradayMarketSlot,
        flow_bundle: Dict[str, Any],
        sector_leaders: List[IntradayDigestEntry],
        stock_leaders: List[IntradayDigestEntry],
    ) -> str:
        latest = flow_bundle.get("latest") or {}
        main_flow = _safe_float(latest.get("main_net_inflow")) or 0.0
        top_sector = sector_leaders[0].name if sector_leaders else "主线热点"
        top_stock = stock_leaders[0].name if stock_leaders else "龙头股"
        if slot.stage == "auction":
            return (
                f"开盘阶段优先观察 {top_sector} 是否继续获得增量资金确认，"
                f"如 {top_stock} 这类竞价强势股出现分歧回封，可关注龙头确认后的低位补涨。"
                " 若主力资金快速翻绿，则以控制追高和等待二次确认为主。"
            )
        if main_flow >= 0:
            return (
                f"资金仍在围绕 {top_sector} 展开，适合优先跟踪主线龙头与高景气分支。"
                f" 操作上更适合等待核心股分时回踩后的承接，而不是直接追高；"
                f" 若 {top_stock} 一类高辨识度个股继续放量走强，可关注同板块二线补涨机会。"
            )
        return (
            f"当前资金承接偏谨慎，尽量只看 {top_sector} 这类仍有资金回流的强势方向。"
            " 操作上以仓位控制和观察高低切换为主，避免在弱承接环境下追逐尾盘拉升。"
        )

    def _build_us_investment_direction(
        self,
        sector_leaders: List[IntradayDigestEntry],
        stock_leaders: List[IntradayDigestEntry],
    ) -> str:
        top_sector = sector_leaders[0].name if sector_leaders else "主线行业"
        top_stock = stock_leaders[0].name if stock_leaders else "高成交额龙头"
        return (
            f"美股当前更适合围绕 {top_sector} 这些明确走强的行业 ETF 与龙头股做相对强弱跟踪。"
            f" 交易上优先关注 {top_stock} 这类高流动性强势标的，避免在弱成交的小盘股里追涨。"
        )

    def _build_cn_previous_day_comparison(
        self,
        flow_bundle: Dict[str, Any],
        sector_leaders: List[IntradayDigestEntry],
        current_date: str,
    ) -> str:
        history = flow_bundle.get("history")
        if not isinstance(history, pd.DataFrame) or len(history) < 2:
            return ""

        latest = history.iloc[-1]
        prev = history.iloc[-2]
        latest_flow = _safe_float(self._get_series_value(latest, ["主力净流入-净额"])) or 0.0
        prev_flow = _safe_float(self._get_series_value(prev, ["主力净流入-净额"])) or 0.0
        latest_pct = _safe_float(self._get_series_value(latest, ["上证-涨跌幅"])) or 0.0
        prev_pct = _safe_float(self._get_series_value(prev, ["上证-涨跌幅"])) or 0.0
        prev_sector_names = self._extract_ranked_names_from_previous_report(
            region="cn",
            label_suffix="1500",
            section="#### 当日涨停 Top 20",
            before_date=current_date,
        )
        today_sector_text = "、".join(entry.name for entry in sector_leaders[:3]) or "热点分散"
        prev_stock_text = "、".join(prev_sector_names[:3]) if prev_sector_names else "昨日强势股"
        return (
            f"与前一交易日相比，上证涨跌幅由 {prev_pct:+.2f}% 变化为 {latest_pct:+.2f}%，"
            f"主力净流入由 {_format_money(prev_flow)} 变化为 {_format_money(latest_flow)}。"
            f" 资金风格从围绕 {prev_stock_text} 的情绪博弈，切换到今日更偏向 {today_sector_text} 的主线扩散。"
        )

    def _build_cn_weekly_summary(
        self,
        slot: IntradayMarketSlot,
        flow_bundle: Dict[str, Any],
        sector_leaders: List[IntradayDigestEntry],
        daily_limit_ups: List[IntradayDigestEntry],
    ) -> str:
        history = flow_bundle.get("history")
        weekly_flow = 0.0
        if isinstance(history, pd.DataFrame) and not history.empty:
            work_df = history.copy()
            date_col = _pick_column(work_df.columns, ["日期", "交易日"])
            if date_col:
                work_df[date_col] = pd.to_datetime(work_df[date_col], errors="coerce")
                end_date = pd.to_datetime(slot.local_date)
                start_date = end_date - pd.Timedelta(days=7)
                work_df = work_df[(work_df[date_col] >= start_date) & (work_df[date_col] <= end_date)]
            flow_col = _pick_column(work_df.columns, ["主力净流入-净额"])
            if flow_col:
                weekly_flow = float(pd.to_numeric(work_df[flow_col], errors="coerce").fillna(0).sum())

        sector_counter = Counter(self._extract_weekly_ranked_names("### 四、行业与主题强弱", slot.local_date))
        stock_counter = Counter(self._extract_weekly_ranked_names("#### 当日涨停 Top 20", slot.local_date))
        for entry in sector_leaders[:3]:
            sector_counter[entry.name] += 1
        for entry in daily_limit_ups[:20]:
            stock_counter[entry.name] += 1

        top_sectors = "、".join(name for name, _ in sector_counter.most_common(3)) or "热点轮动"
        top_stocks = "、".join(name for name, _ in stock_counter.most_common(5)) or "强势股轮动"
        return (
            f"本周主力资金累计净流入 {_format_money(weekly_flow)}，"
            f"资金最强的方向主要集中在 {top_sectors}。"
            f" 本周反复走强、表现最强劲的前排股票以 {top_stocks} 为代表，"
            "整体风格仍偏向高景气科技与情绪龙头。"
        )

    def _format_indices_summary(self, indices: Sequence[Any]) -> str:
        lines: List[str] = []
        for index in indices[:5]:
            change_pct = getattr(index, "change_pct", 0.0) or 0.0
            amount = getattr(index, "amount", None)
            amount_text = _format_money(_safe_float(amount))
            lines.append(
                f"- {getattr(index, 'name', '')}: {getattr(index, 'current', 0.0):.2f} "
                f"({_format_percent(change_pct)}) | 成交额 {amount_text}"
            )
        return "\n".join(lines)

    def _format_cn_market_stats(self, overview) -> str:
        return (
            f"上涨 {overview.up_count} 家，下跌 {overview.down_count} 家，平盘 {overview.flat_count} 家；"
            f"涨停 {overview.limit_up_count} 家，跌停 {overview.limit_down_count} 家；"
            f"两市成交额 {overview.total_amount:.0f} 亿。"
        )

    def _format_us_market_stats(self, turnover_entries: List[IntradayDigestEntry]) -> str:
        turnover_text = "、".join(entry.name for entry in turnover_entries[:5]) or "暂无集中成交"
        return f"当前高成交额集中在 {turnover_text}，更适合作为盘中资金关注的代理口径。"

    def _extract_news_highlights(self, news: Sequence[Any]) -> List[str]:
        highlights: List[str] = []
        for item in news[:5]:
            title = getattr(item, "title", None) or (item.get("title") if isinstance(item, dict) else "")
            snippet = getattr(item, "snippet", None) or (item.get("snippet") if isinstance(item, dict) else "")
            if title:
                merged = title.strip()
                if snippet:
                    merged += f"：{snippet.strip()[:80]}"
                highlights.append(merged[:160])
        return highlights

    def _get_cn_flow_bundle(self) -> Dict[str, Any]:
        history = self._call_akshare_df_candidates([("stock_market_fund_flow", {})])
        latest_payload: Dict[str, Any] = {}
        if history is not None and not history.empty:
            latest_row = history.iloc[-1]
            latest_payload = {
                "main_net_inflow": self._get_series_value(latest_row, ["主力净流入-净额"]),
                "main_net_ratio": self._get_series_value(latest_row, ["主力净流入-净占比"]),
            }

        northbound = None
        north_df = self._call_akshare_df_candidates([("stock_hsgt_fund_flow_summary_em", {})])
        if north_df is not None and not north_df.empty:
            direction_col = _pick_column(north_df.columns, ["资金方向"])
            value_col = _pick_column(north_df.columns, ["成交净买额", "资金净流入"])
            if direction_col and value_col:
                work_df = north_df.copy()
                try:
                    work_df[value_col] = pd.to_numeric(work_df[value_col], errors="coerce")
                    northbound = float(
                        work_df[work_df[direction_col].astype(str).str.contains("北向", na=False)][value_col]
                        .fillna(0)
                        .sum()
                    )
                except Exception:
                    northbound = None

        return {
            "history": history,
            "latest": latest_payload,
            "northbound_net_inflow": northbound,
        }

    def _get_cn_sector_leaders(self) -> List[IntradayDigestEntry]:
        df = self._call_akshare_df_candidates([
            ("stock_sector_fund_flow_rank", {"indicator": "今日"}),
            ("stock_sector_fund_flow_rank", {"indicator": "即时"}),
            ("stock_sector_fund_flow_rank", {}),
        ])
        if df is None or df.empty:
            return []

        name_col = _pick_column(df.columns, ["行业", "板块", "名称"])
        change_col = _pick_column(df.columns, ["涨跌幅"])
        net_col = _pick_column(df.columns, ["净额", "净流入"])
        leader_col = _pick_column(df.columns, ["领涨股"])
        if not name_col:
            return []
        work_df = df.copy()
        if change_col:
            work_df[change_col] = pd.to_numeric(work_df[change_col], errors="coerce")
            work_df = work_df.sort_values(change_col, ascending=False, na_position="last")
        entries: List[IntradayDigestEntry] = []
        for _, row in work_df.head(8).iterrows():
            name = _safe_str(row.get(name_col))
            if not name:
                continue
            value = _format_percent(_safe_float(row.get(change_col))) if change_col else "热点活跃"
            extra_parts: List[str] = []
            if net_col:
                extra_parts.append(f"净流入 {_format_money(_safe_float(row.get(net_col)))}")
            if leader_col and _safe_str(row.get(leader_col)):
                extra_parts.append(f"领涨股 {_safe_str(row.get(leader_col))}")
            entries.append(IntradayDigestEntry(name=name, value=value, extra=" | ".join(extra_parts)))
        return entries

    def _get_cn_stock_leaders(self) -> List[IntradayDigestEntry]:
        df = self._call_akshare_df_candidates([("stock_zh_a_spot_em", {})])
        if df is None or df.empty:
            return []
        name_col = _pick_column(df.columns, ["名称"])
        change_col = _pick_column(df.columns, ["涨跌幅"])
        amount_col = _pick_column(df.columns, ["成交额"])
        if not name_col or not change_col:
            return []
        work_df = df.copy()
        work_df[change_col] = pd.to_numeric(work_df[change_col], errors="coerce")
        work_df = work_df.sort_values(change_col, ascending=False, na_position="last")
        entries: List[IntradayDigestEntry] = []
        for _, row in work_df.head(10).iterrows():
            name = _safe_str(row.get(name_col))
            if not name:
                continue
            extra = f"成交额 {_format_money(_safe_float(row.get(amount_col)))}" if amount_col else ""
            entries.append(
                IntradayDigestEntry(
                    name=name,
                    value=_format_percent(_safe_float(row.get(change_col))),
                    extra=extra,
                )
            )
        return entries

    def _get_cn_flow_stock_leaders(self) -> List[IntradayDigestEntry]:
        df = self._call_akshare_df_candidates([
            ("stock_individual_fund_flow_rank", {"indicator": "今日"}),
            ("stock_individual_fund_flow_rank", {"indicator": "即时"}),
            ("stock_individual_fund_flow_rank", {}),
            ("stock_main_fund_flow", {}),
        ])
        if df is None or df.empty:
            return []
        name_col = _pick_column(df.columns, ["名称"])
        flow_col = _pick_column(df.columns, ["主力净流入", "净额", "净流入"])
        change_col = _pick_column(df.columns, ["涨跌幅"])
        if not name_col or not flow_col:
            return []
        work_df = df.copy()
        work_df[flow_col] = pd.to_numeric(work_df[flow_col], errors="coerce")
        work_df = work_df.sort_values(flow_col, ascending=False, na_position="last")
        entries: List[IntradayDigestEntry] = []
        for _, row in work_df.head(8).iterrows():
            name = _safe_str(row.get(name_col))
            if not name:
                continue
            extra = _format_percent(_safe_float(row.get(change_col))) if change_col else ""
            entries.append(
                IntradayDigestEntry(
                    name=name,
                    value=f"主力净流入 {_format_money(_safe_float(row.get(flow_col)))}",
                    extra=extra,
                )
            )
        return entries

    def _get_cn_limit_up_pool(
        self,
        slot: IntradayMarketSlot,
        *,
        top_n: int,
        auction_only: bool,
    ) -> List[IntradayDigestEntry]:
        df = self._call_akshare_df_candidates([("stock_zt_pool_em", {"date": _today_compact(slot)})])
        if df is None or df.empty:
            return []

        name_col = _pick_column(df.columns, ["名称"])
        industry_col = _pick_column(df.columns, ["所属行业"])
        strength_col = _pick_column(df.columns, ["涨停统计", "连板"])
        first_col = _pick_column(df.columns, ["首次封板时间"])
        fund_col = _pick_column(df.columns, ["封单资金", "成交额"])
        if not name_col:
            return []

        work_df = df.copy()
        if auction_only and first_col:
            first_series = work_df[first_col].astype(str).str.replace(":", "")
            work_df = work_df[first_series <= "093000"]
        if strength_col:
            work_df["_strength"] = work_df[strength_col].map(_parse_limit_strength)
        else:
            work_df["_strength"] = 0
        if fund_col:
            work_df["_fund"] = pd.to_numeric(work_df[fund_col], errors="coerce").fillna(0)
        else:
            work_df["_fund"] = 0
        work_df = work_df.sort_values(["_strength", "_fund"], ascending=[False, False], na_position="last")

        entries: List[IntradayDigestEntry] = []
        for _, row in work_df.head(top_n).iterrows():
            name = _safe_str(row.get(name_col))
            if not name:
                continue
            strength = _parse_limit_strength(row.get(strength_col))
            industry = _safe_str(row.get(industry_col))
            first_time = _normalize_time_text(row.get(first_col))
            reason = self._build_limit_up_reason(
                stock_name=name,
                industry=industry,
                strength=strength,
                first_time=first_time,
            )
            value = f"{strength}连板" if strength > 0 else "首板"
            extra_parts = [part for part in [industry, first_time, reason] if part]
            entries.append(IntradayDigestEntry(name=name, value=value, extra=" | ".join(extra_parts)))
        return entries

    def _build_limit_up_reason(
        self,
        *,
        stock_name: str,
        industry: str,
        strength: int,
        first_time: str,
    ) -> str:
        parts: List[str] = []
        if industry:
            parts.append(f"{industry}主线带动")
        if strength >= 2:
            parts.append("连板情绪延续")
        if first_time and first_time <= "09:30":
            parts.append("竞价资金一致性较强")
        return "，".join(parts) if parts else f"{stock_name} 主要受短线资金和情绪驱动"

    def _get_us_spot_frame(self) -> Optional[pd.DataFrame]:
        return self._call_akshare_df_candidates([("stock_us_spot_em", {})])

    def _get_us_stock_leaders(self, df: Optional[pd.DataFrame]) -> List[IntradayDigestEntry]:
        if df is None or df.empty:
            return []
        name_col = _pick_column(df.columns, ["名称"])
        change_col = _pick_column(df.columns, ["涨跌幅"])
        amount_col = _pick_column(df.columns, ["成交额"])
        if not name_col or not change_col:
            return []
        work_df = df.copy()
        work_df[change_col] = pd.to_numeric(work_df[change_col], errors="coerce")
        work_df = work_df.sort_values(change_col, ascending=False, na_position="last")
        entries: List[IntradayDigestEntry] = []
        for _, row in work_df.head(10).iterrows():
            name = _safe_str(row.get(name_col))
            if not name:
                continue
            extra = f"成交额 {_format_money(_safe_float(row.get(amount_col)), unit_hint='usd')}" if amount_col else ""
            entries.append(
                IntradayDigestEntry(
                    name=name,
                    value=_format_percent(_safe_float(row.get(change_col))),
                    extra=extra,
                )
            )
        return entries

    def _get_us_turnover_leaders(self, df: Optional[pd.DataFrame]) -> List[IntradayDigestEntry]:
        if df is None or df.empty:
            return []
        name_col = _pick_column(df.columns, ["名称"])
        amount_col = _pick_column(df.columns, ["成交额"])
        change_col = _pick_column(df.columns, ["涨跌幅"])
        if not name_col or not amount_col:
            return []
        work_df = df.copy()
        work_df[amount_col] = pd.to_numeric(work_df[amount_col], errors="coerce")
        work_df = work_df.sort_values(amount_col, ascending=False, na_position="last")
        entries: List[IntradayDigestEntry] = []
        for _, row in work_df.head(8).iterrows():
            name = _safe_str(row.get(name_col))
            if not name:
                continue
            extra = _format_percent(_safe_float(row.get(change_col))) if change_col else ""
            entries.append(
                IntradayDigestEntry(
                    name=name,
                    value=f"成交额 {_format_money(_safe_float(row.get(amount_col)), unit_hint='usd')}",
                    extra=extra,
                )
            )
        return entries

    def _get_us_sector_leaders(self) -> List[IntradayDigestEntry]:
        entries: List[IntradayDigestEntry] = []
        for ticker, label in self._US_SECTOR_ETFS:
            quote = self.data_manager.get_realtime_quote(ticker)
            if not quote:
                continue
            pct = getattr(quote, "pct_chg", None)
            amount = getattr(quote, "amount", None)
            entries.append(
                IntradayDigestEntry(
                    name=label,
                    value=_format_percent(_safe_float(pct)),
                    extra=f"{ticker} | {_format_money(_safe_float(amount), unit_hint='usd')}",
                )
            )
        entries.sort(key=lambda item: _safe_float(item.value.replace("%", "")) or -9999, reverse=True)
        return entries[:8]

    def _get_series_value(self, row: pd.Series, keywords: Sequence[str]) -> Any:
        for column in row.index:
            column_text = str(column)
            if any(keyword in column_text for keyword in keywords):
                return row.get(column)
        return None

    def _call_akshare_df_candidates(
        self,
        candidates: Sequence[Tuple[str, Dict[str, Any]]],
    ) -> Optional[pd.DataFrame]:
        try:
            import akshare as ak
        except Exception as exc:
            logger.warning("intraday digest import akshare failed: %s", exc)
            return None

        for func_name, kwargs in candidates:
            func = getattr(ak, func_name, None)
            if func is None:
                continue
            try:
                result = func(**kwargs)
            except Exception as exc:
                logger.debug("intraday digest akshare call failed: %s(%s): %s", func_name, kwargs, exc)
                continue
            if isinstance(result, pd.Series):
                result = result.to_frame().T
            if isinstance(result, pd.DataFrame) and not result.empty:
                return result
        return None

    def _report_filename(self, slot: IntradayMarketSlot) -> str:
        label = slot.label.replace(":", "")
        return f"intraday_market_digest_{slot.region}_{slot.local_date.replace('-', '')}_{label}.md"

    def _is_friday(self, slot: IntradayMarketSlot) -> bool:
        return datetime.strptime(slot.local_date, "%Y-%m-%d").weekday() == 4

    def _extract_weekly_ranked_names(self, section: str, end_date: str) -> List[str]:
        current_week_files = sorted(self.reports_dir.glob("intraday_market_digest_cn_*_1500.md"))
        end_day = datetime.strptime(end_date, "%Y-%m-%d").date()
        start_day = end_day - timedelta(days=end_day.weekday())
        names: List[str] = []
        for path in current_week_files:
            match = re.search(r"intraday_market_digest_cn_(\d{8})_1500\.md$", path.name)
            if not match:
                continue
            file_day = datetime.strptime(match.group(1), "%Y%m%d").date()
            if not (start_day <= file_day <= end_day):
                continue
            try:
                names.extend(self._extract_ranked_names(path.read_text(encoding="utf-8"), section))
            except Exception:
                continue
        return names

    def _extract_ranked_names_from_previous_report(
        self,
        *,
        region: str,
        label_suffix: str,
        section: str,
        before_date: str,
    ) -> List[str]:
        files = sorted(self.reports_dir.glob(f"intraday_market_digest_{region}_*_{label_suffix}.md"))
        if len(files) < 1:
            return []
        filtered: List[Path] = []
        for path in files:
            match = re.search(rf"intraday_market_digest_{region}_(\d{{8}})_{label_suffix}\.md$", path.name)
            if not match:
                continue
            file_date = datetime.strptime(match.group(1), "%Y%m%d").date()
            if file_date < datetime.strptime(before_date, "%Y-%m-%d").date():
                filtered.append(path)
        if not filtered:
            return []
        try:
            return self._extract_ranked_names(filtered[-1].read_text(encoding="utf-8"), section)
        except Exception:
            return []

    @staticmethod
    def _extract_ranked_names(report_text: str, section: str) -> List[str]:
        match = re.search(
            re.escape(section) + r"\n((?:\d+\.\s+\*\*.*\n)+)",
            report_text,
        )
        if not match:
            return []
        return re.findall(r"\d+\.\s+\*\*(.*?)\*\*:", match.group(1))

    def _enrich_context_with_llm(self, context: IntradayDigestContext) -> IntradayDigestContext:
        if not self.analyzer or not hasattr(self.analyzer, "generate_text"):
            return context

        prompt = self._build_llm_enrichment_prompt(context)
        response = self.analyzer.generate_text(prompt, max_tokens=1800, temperature=0.4)
        if not response:
            return context

        enriched = IntradayDigestContext(**context.__dict__)
        for field_name in (
            "headline",
            "capital_flow_summary",
            "investment_direction",
            "previous_day_comparison",
            "weekly_summary",
        ):
            value = self._extract_tagged_block(response, field_name)
            if value:
                setattr(enriched, field_name, value)
        return enriched

    def _build_llm_enrichment_prompt(self, context: IntradayDigestContext) -> str:
        sector_lines = "\n".join(f"- {item.name}: {item.value}; {item.extra}" for item in context.sector_leaders[:6]) or "- 无"
        stock_lines = "\n".join(f"- {item.name}: {item.value}; {item.extra}" for item in context.stock_leaders[:8]) or "- 无"
        auction_lines = "\n".join(f"- {item.name}: {item.value}; {item.extra}" for item in context.auction_limit_ups[:10]) or "- 无"
        limit_lines = "\n".join(f"- {item.name}: {item.value}; {item.extra}" for item in context.daily_limit_ups[:20]) or "- 无"
        news_lines = "\n".join(f"- {item}" for item in context.news_highlights[:5]) or "- 无"
        return f"""你是一位专业市场复盘助手。请基于以下结构化信息，输出详细但克制的中文段落，避免空话和口号。

市场: {context.slot.market_name}
时点: {context.slot.local_date} {context.slot.label}
阶段: {context.slot.stage}

[指数与市场面]
{context.indices_summary}
{context.market_stats_summary}

[当前资金流判断]
{context.capital_flow_summary}

[行业与主题]
{sector_lines}

[领涨个股]
{stock_lines}

[竞价涨停]
{auction_lines}

[当日涨停]
{limit_lines}

[新闻催化]
{news_lines}

[已有对比]
{context.previous_day_comparison or "无"}

[已有周总结]
{context.weekly_summary or "无"}

要求:
1. 输出以下标签块，标签名不要改。
2. 每个块 2-4 句，必须具体说明资金流、行业和操作方向。
3. 如果没有足够信息，保持审慎，不要编造。

[headline]
写详细核心结论，不能泛泛而谈。
[/headline]

[capital_flow_summary]
写详细资金流向分析，必须点明资金集中去向。
[/capital_flow_summary]

[investment_direction]
给出偏交易/投资方向建议，说明优先关注什么，不宜做什么。
[/investment_direction]

[previous_day_comparison]
如果当前是收盘版且已有对比信息，就写与前一交易日相比的连续性分析；否则留空标签。
[/previous_day_comparison]

[weekly_summary]
如果当前是周内最后一份收盘版且已有周总结信息，就写本周总结；否则留空标签。
[/weekly_summary]
"""

    @staticmethod
    def _extract_tagged_block(text: str, tag: str) -> str:
        match = re.search(rf"\[{tag}\]\s*(.*?)\s*\[/{tag}\]", text, flags=re.S)
        if not match:
            return ""
        return match.group(1).strip()


def run_intraday_market_digest(
    *,
    notifier,
    analyzer=None,
    search_service=None,
    region_override: str = "auto",
    slot_override: str = "auto",
    send_notification: bool = True,
    tolerance_minutes: int = 20,
) -> List[str]:
    service = IntradayMarketDigestService(
        notifier=notifier,
        analyzer=analyzer,
        search_service=search_service,
    )
    return service.run(
        region_override=region_override,
        slot_override=slot_override,
        send_notification=send_notification,
        tolerance_minutes=tolerance_minutes,
    )


__all__ = ["IntradayMarketDigestService", "run_intraday_market_digest"]
