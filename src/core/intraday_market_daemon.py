# -*- coding: utf-8 -*-
"""Local intraday market digest daemon scheduling."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, timedelta
import logging
import time
from typing import Callable, Dict, Iterable, List, Sequence, Set, Tuple
from zoneinfo import ZoneInfo

from src.core.trading_calendar import is_market_open
from src.scheduler import GracefulShutdown


logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class IntradayDigestTrigger:
    """A local wall-clock trigger mapped to a market digest slot."""

    region: str
    trigger_label: str
    slot_label: str
    timezone_name: str


_TRIGGER_CONFIG: Dict[str, Dict[str, object]] = {
    "cn": {
        "timezone_name": "Asia/Shanghai",
        "triggers": [
            ("09:24", "09:30"),
            ("10:30", "10:30"),
            ("11:30", "11:30"),
            ("13:30", "13:30"),
            ("14:30", "14:30"),
            ("15:03", "15:00"),
        ],
    },
}


def _parse_time_label(label: str) -> Tuple[int, int]:
    hour_text, minute_text = label.split(":", 1)
    return int(hour_text), int(minute_text)


def get_intraday_digest_triggers(region: str) -> List[IntradayDigestTrigger]:
    """Return local wall-clock triggers for the requested market region."""

    config = _TRIGGER_CONFIG.get(region)
    if config is None:
        raise ValueError(f"unsupported intraday daemon region: {region}")

    timezone_name = str(config["timezone_name"])
    return [
        IntradayDigestTrigger(
            region=region,
            trigger_label=trigger_label,
            slot_label=slot_label,
            timezone_name=timezone_name,
        )
        for trigger_label, slot_label in config["triggers"]  # type: ignore[index]
    ]


def build_execution_key(trigger: IntradayDigestTrigger, local_day: date) -> str:
    """Build a stable per-day execution key for deduplication."""

    return f"{trigger.region}:{local_day.isoformat()}:{trigger.slot_label}"


def get_due_intraday_digest_triggers(
    *,
    region: str,
    now_local: datetime | None = None,
    executed_keys: Iterable[str] | None = None,
    grace_minutes: int = 10,
) -> List[IntradayDigestTrigger]:
    """
    Resolve due local triggers within the configured grace window.

    This is intended for a self-hosted daemon where we control the local clock
    and can tolerate short process restarts by retrying within a bounded window.
    """

    triggers = get_intraday_digest_triggers(region)
    timezone_name = triggers[0].timezone_name
    tz = ZoneInfo(timezone_name)

    if now_local is None:
        now_local = datetime.now(tz)
    elif now_local.tzinfo is None:
        now_local = now_local.replace(tzinfo=tz)
    else:
        now_local = now_local.astimezone(tz)

    if not is_market_open(region, now_local.date()):
        return []

    executed = set(executed_keys or ())
    grace = timedelta(minutes=max(0, grace_minutes))
    due: List[IntradayDigestTrigger] = []

    for trigger in triggers:
        hour, minute = _parse_time_label(trigger.trigger_label)
        trigger_dt = now_local.replace(hour=hour, minute=minute, second=0, microsecond=0)
        execution_key = build_execution_key(trigger, now_local.date())
        if execution_key in executed:
            continue
        if trigger_dt <= now_local <= trigger_dt + grace:
            due.append(trigger)

    return due


class IntradayMarketDigestDaemon:
    """A local long-running scheduler for intraday market digest delivery."""

    def __init__(
        self,
        *,
        region: str = "cn",
        grace_minutes: int = 10,
        poll_interval_seconds: int = 20,
    ) -> None:
        self.region = region
        self.grace_minutes = max(1, grace_minutes)
        self.poll_interval_seconds = max(5, poll_interval_seconds)
        self.shutdown_handler = GracefulShutdown()
        self._executed_keys: Set[str] = set()

    def _prune_history(self, local_day: date) -> None:
        current_prefix = f"{self.region}:{local_day.isoformat()}:"
        self._executed_keys = {key for key in self._executed_keys if key.startswith(current_prefix)}

    def run(self, runner: Callable[[str], bool]) -> None:
        """Run the daemon loop until interrupted."""

        triggers = get_intraday_digest_triggers(self.region)
        logger.info(
            "盘中盯盘守护器已启动，市场=%s，触发计划=%s，宽限=%s 分钟，轮询=%s 秒",
            self.region,
            ", ".join(f"{item.trigger_label}->{item.slot_label}" for item in triggers),
            self.grace_minutes,
            self.poll_interval_seconds,
        )

        while not self.shutdown_handler.should_shutdown:
            timezone_name = triggers[0].timezone_name
            now_local = datetime.now(ZoneInfo(timezone_name))
            self._prune_history(now_local.date())
            due_triggers = get_due_intraday_digest_triggers(
                region=self.region,
                now_local=now_local,
                executed_keys=self._executed_keys,
                grace_minutes=self.grace_minutes,
            )
            for trigger in due_triggers:
                execution_key = build_execution_key(trigger, now_local.date())
                logger.info(
                    "命中本地盯盘触发窗口：trigger=%s slot=%s local_time=%s",
                    trigger.trigger_label,
                    trigger.slot_label,
                    now_local.strftime("%Y-%m-%d %H:%M:%S"),
                )
                try:
                    if runner(trigger.slot_label):
                        self._executed_keys.add(execution_key)
                        logger.info("盯盘报告执行成功：slot=%s", trigger.slot_label)
                    else:
                        logger.warning("盯盘报告执行未产出结果，将在宽限窗口内继续重试：slot=%s", trigger.slot_label)
                except Exception as exc:
                    logger.exception("盯盘报告执行失败：slot=%s error=%s", trigger.slot_label, exc)
            time.sleep(self.poll_interval_seconds)

        logger.info("盘中盯盘守护器已停止")


__all__ = [
    "IntradayDigestTrigger",
    "IntradayMarketDigestDaemon",
    "build_execution_key",
    "get_due_intraday_digest_triggers",
    "get_intraday_digest_triggers",
]
