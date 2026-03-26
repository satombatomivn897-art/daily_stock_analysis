# -*- coding: utf-8 -*-
"""Intraday market schedule resolution helpers."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from typing import Dict, List, Tuple
from zoneinfo import ZoneInfo

from src.core.trading_calendar import is_market_open


@dataclass(frozen=True)
class IntradayMarketSlot:
    """Resolved market slot for an intraday digest run."""

    region: str
    label: str
    stage: str
    market_name: str
    local_date: str
    timezone_name: str
    is_final: bool = False


_SLOT_CONFIG: Dict[str, Dict[str, object]] = {
    "cn": {
        "timezone_name": "Asia/Shanghai",
        "market_name": "A股",
        "slots": [
            ("09:30", "auction", False),
            ("10:30", "intraday", False),
            ("11:30", "midday", False),
            ("13:30", "intraday", False),
            ("14:30", "intraday", False),
            ("15:00", "close", True),
        ],
    },
    "us": {
        "timezone_name": "America/New_York",
        "market_name": "美股",
        "slots": [
            ("09:30", "open", False),
            ("10:30", "intraday", False),
            ("11:30", "intraday", False),
            ("12:30", "intraday", False),
            ("13:30", "intraday", False),
            ("14:30", "intraday", False),
            ("15:30", "close", True),
        ],
    },
}


def _parse_slot_label(label: str) -> Tuple[int, int]:
    hour_text, minute_text = label.split(":", 1)
    return int(hour_text), int(minute_text)


def build_intraday_slot(
    region: str,
    label: str,
    local_day: date | None = None,
) -> IntradayMarketSlot:
    """Build a slot from region + label, used for manual overrides."""

    config = _SLOT_CONFIG.get(region)
    if config is None:
        raise ValueError(f"unsupported intraday region: {region}")

    slot_map = {
        item_label: (stage, is_final)
        for item_label, stage, is_final in config["slots"]  # type: ignore[index]
    }
    stage, is_final = slot_map[label]
    target_date = local_day or datetime.now(ZoneInfo(config["timezone_name"])).date()  # type: ignore[index]
    return IntradayMarketSlot(
        region=region,
        label=label,
        stage=stage,
        market_name=str(config["market_name"]),
        local_date=target_date.isoformat(),
        timezone_name=str(config["timezone_name"]),
        is_final=bool(is_final),
    )


def resolve_intraday_slots(
    now_utc: datetime | None = None,
    tolerance_minutes: int = 20,
) -> List[IntradayMarketSlot]:
    """
    Resolve which intraday digest slots are active for the given UTC time.

    GitHub Actions schedule may drift by several minutes, so matching uses a
    positive tolerance window after the target slot time.
    """

    if now_utc is None:
        now_utc = datetime.now(timezone.utc)
    if now_utc.tzinfo is None:
        now_utc = now_utc.replace(tzinfo=timezone.utc)
    else:
        now_utc = now_utc.astimezone(timezone.utc)

    tolerance = timedelta(minutes=max(0, tolerance_minutes))
    resolved: List[IntradayMarketSlot] = []

    for region, config in _SLOT_CONFIG.items():
        tz = ZoneInfo(str(config["timezone_name"]))
        local_now = now_utc.astimezone(tz)
        if not is_market_open(region, local_now.date()):
            continue

        for label, stage, is_final in config["slots"]:  # type: ignore[index]
            hour, minute = _parse_slot_label(label)
            slot_dt = local_now.replace(hour=hour, minute=minute, second=0, microsecond=0)
            if slot_dt <= local_now <= slot_dt + tolerance:
                resolved.append(
                    IntradayMarketSlot(
                        region=region,
                        label=label,
                        stage=stage,
                        market_name=str(config["market_name"]),
                        local_date=local_now.date().isoformat(),
                        timezone_name=str(config["timezone_name"]),
                        is_final=bool(is_final),
                    )
                )

    return resolved


__all__ = ["IntradayMarketSlot", "build_intraday_slot", "resolve_intraday_slots"]
