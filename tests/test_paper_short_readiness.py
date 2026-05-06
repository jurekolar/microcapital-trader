import io
import tempfile
import unittest
from contextlib import redirect_stdout
from pathlib import Path

import requests

from microcapital_trader import Config, Journal, StreamExecutionEngine, format_http_error


class HttpErrorFormattingTests(unittest.TestCase):
    def test_formats_json_error_body_and_request_id(self) -> None:
        response = requests.Response()
        response.status_code = 403
        response.reason = "Forbidden"
        response.headers["X-Request-ID"] = "req-123"
        response._content = b'{"message":"shorting disabled","code":40310000}'
        exc = requests.HTTPError("403 Client Error", response=response)

        formatted = format_http_error(exc)

        self.assertIn("HTTP 403 Forbidden", formatted)
        self.assertIn("request_id=req-123", formatted)
        self.assertIn('body={"code":40310000,"message":"shorting disabled"}', formatted)

    def test_formats_non_json_error_body(self) -> None:
        response = requests.Response()
        response.status_code = 500
        response.reason = "Internal Server Error"
        response._content = b"first line\nsecond line"
        exc = requests.HTTPError("500 Server Error", response=response)

        formatted = format_http_error(exc)

        self.assertEqual("HTTP 500 Internal Server Error body=first line second line", formatted)


class EntryRiskCheckTests(unittest.TestCase):
    def make_engine(self) -> StreamExecutionEngine:
        temp_dir = tempfile.TemporaryDirectory()
        self.addCleanup(temp_dir.cleanup)
        root = Path(temp_dir.name)
        config = Config(
            mode="paper",
            asset_class="equity",
            journal_path=str(root / "journal.csv"),
            state_path=str(root / "state.json"),
        )
        engine = StreamExecutionEngine(config, Journal(config.journal_path))
        engine.broker_state.uncertain = False
        engine.broker_state.account_equity = 10_000.0
        engine.broker_state.buying_power = 10_000.0
        engine.broker_state.shorting_enabled = True
        return engine

    def test_short_entry_skips_when_account_shorting_disabled(self) -> None:
        engine = self.make_engine()
        engine.broker_state.shorting_enabled = False

        with redirect_stdout(io.StringIO()):
            allowed = engine._entry_risk_checks(
                "NOW",
                "sell",
                100.0,
                5.0,
                {"ask": 101.0, "spread_bps": 1.0, "timestamp": None},
            )

        self.assertFalse(allowed)

    def test_short_entry_skips_when_short_buying_power_is_insufficient(self) -> None:
        engine = self.make_engine()
        engine.broker_state.buying_power = 520.0

        with redirect_stdout(io.StringIO()):
            allowed = engine._entry_risk_checks(
                "NOW",
                "sell",
                100.0,
                5.0,
                {"ask": 101.0, "spread_bps": 1.0, "timestamp": None},
            )

        self.assertFalse(allowed)

    def test_long_entry_uses_buying_power_not_shorting_flag(self) -> None:
        engine = self.make_engine()
        engine.broker_state.shorting_enabled = False

        allowed = engine._entry_risk_checks(
            "TQQQ",
            "buy",
            100.0,
            5.0,
            {"ask": 101.0, "spread_bps": 1.0, "timestamp": None},
        )

        self.assertTrue(allowed)


if __name__ == "__main__":
    unittest.main()
