# -*- coding: utf-8 -*-
"""Regression tests for local intraday daemon CLI path."""

import sys
import unittest
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

from tests.litellm_stub import ensure_litellm_stub

ensure_litellm_stub()
if "json_repair" not in sys.modules:
    sys.modules["json_repair"] = MagicMock(repair_json=lambda value, **_: value)

import main


class MainIntradayMarketDaemonTestCase(unittest.TestCase):
    def _make_args(self, **overrides):
        defaults = {
            "debug": False,
            "stocks": None,
            "webui": False,
            "webui_only": False,
            "serve": False,
            "serve_only": False,
            "host": "0.0.0.0",
            "port": 8000,
            "backtest": False,
            "market_review": False,
            "intraday_market_digest": False,
            "intraday_market_daemon": True,
            "intraday_region": "cn",
            "intraday_slot": "auto",
            "schedule": False,
            "no_run_immediately": False,
            "no_notify": False,
            "no_market_review": False,
            "dry_run": False,
            "workers": 1,
            "force_run": False,
            "single_notify": False,
            "no_context_snapshot": False,
            "intraday_grace_minutes": 10,
            "intraday_poll_seconds": 20,
        }
        defaults.update(overrides)
        return SimpleNamespace(**defaults)

    def test_intraday_daemon_mode_routes_to_daemon_runner(self) -> None:
        args = self._make_args()
        config = SimpleNamespace(
            log_dir="/tmp",
            webui_enabled=False,
            dingtalk_stream_enabled=False,
            feishu_stream_enabled=False,
            validate=lambda: [],
        )

        with patch("main.parse_arguments", return_value=args), \
             patch("main.get_config", return_value=config), \
             patch("main.setup_logging"), \
             patch("main.run_intraday_market_daemon_mode", return_value=0) as runner:
            exit_code = main.main()

        self.assertEqual(exit_code, 0)
        runner.assert_called_once_with(config, args)


if __name__ == "__main__":
    unittest.main()
