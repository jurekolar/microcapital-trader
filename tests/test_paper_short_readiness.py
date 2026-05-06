import asyncio
import io
import json
import tempfile
import unittest
from contextlib import redirect_stdout
from pathlib import Path
from unittest.mock import patch

import pandas as pd
import requests

from microcapital_trader import Config, Journal, LiveOrder, LivePosition, Signal, StreamExecutionEngine, format_http_error, utc_now


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

    def test_entry_checks_allow_non_broker_safety_conditions(self) -> None:
        engine = self.make_engine()
        engine.broker_state.uncertain = True
        engine.broker_state.kill_switch_active = True
        engine.broker_state.shorting_enabled = False
        engine.broker_state.buying_power = 0.0
        engine.broker_state.cooldowns["NOW"] = utc_now() + pd.Timedelta(minutes=30)
        engine.broker_state.recovery_only_symbols.add("NOW")
        engine.broker_state.reserved_notional_by_symbol["NOW"] = 50_000.0
        engine.last_market_data_time = utc_now() - pd.Timedelta(hours=1)

        with redirect_stdout(io.StringIO()):
            allowed = engine._entry_risk_checks(
                "NOW",
                "sell",
                100.0,
                5_000.0,
                {
                    "ask": 101.0,
                    "spread_bps": 10_000.0,
                    "timestamp": utc_now() - pd.Timedelta(hours=1),
                },
            )

        self.assertTrue(allowed)
        self.assertTrue(engine._can_trade_symbol("NOW"))

    def test_entry_checks_allow_missing_quote_payload(self) -> None:
        engine = self.make_engine()

        allowed = engine._entry_risk_checks("NOW", "sell", 100.0, 5.0, {})

        self.assertTrue(allowed)

    def test_missing_stream_quote_falls_back_to_signal_price(self) -> None:
        engine = self.make_engine()

        quote = engine._stream_quote_snapshot_or_fallback("TQQQ", 70.15, pd.Timestamp("2026-05-06T14:45:00Z"))

        self.assertEqual(70.15, quote["mid"])
        self.assertEqual(0.0, quote["spread_bps"])


class EntrySubmissionRelaxationTests(unittest.TestCase):
    class StaticStrategy:
        def generate_signal(self, symbol: str, history: pd.DataFrame, config: Config, estimated_spread_bps: float) -> Signal:
            return Signal(
                strategy="momentum",
                symbol=symbol,
                side="buy",
                entry=100.0,
                stop=95.0,
                target_1=105.0,
                target_2=110.0,
                reason="test_signal",
                bar_time=history.iloc[-1]["timestamp"],
            )

    class CapturingExecutor:
        def __init__(self) -> None:
            self.submitted: list[tuple[Signal, float, str]] = []

        def submit_entry_order(self, signal: Signal, qty: float, client_order_id: str) -> dict[str, str]:
            self.submitted.append((signal, qty, client_order_id))
            return {"status": "accepted", "filled_qty": "0", "filled_avg_price": "0", "id": "broker-1"}

    def make_engine(self) -> StreamExecutionEngine:
        temp_dir = tempfile.TemporaryDirectory()
        self.addCleanup(temp_dir.cleanup)
        root = Path(temp_dir.name)
        config = Config(
            mode="paper",
            asset_class="equity",
            symbols=["AAPL"],
            journal_path=str(root / "journal.csv"),
            state_path=str(root / "state.json"),
        )
        engine = StreamExecutionEngine(config, Journal(config.journal_path))
        engine.strategy = self.StaticStrategy()
        engine.executor = self.CapturingExecutor()
        engine.broker_state.uncertain = False
        engine.broker_state.kill_switch_active = True
        engine.broker_state.buying_power = 0.0
        engine.broker_state.shorting_enabled = False
        engine.broker_state.cooldowns["AAPL"] = utc_now() + pd.Timedelta(minutes=30)
        engine.broker_state.recovery_only_symbols.add("AAPL")
        engine.history["AAPL"] = pd.DataFrame(
            [
                {
                    "timestamp": pd.Timestamp("2026-05-06T14:30:00Z"),
                    "open": 99.0,
                    "high": 100.0,
                    "low": 98.0,
                    "close": 99.5,
                    "volume": 1000,
                    "symbol": "AAPL",
                }
            ]
        )
        return engine

    def test_closed_bar_submits_with_missing_quote_and_relaxed_local_flags(self) -> None:
        engine = self.make_engine()
        bar = pd.Series(
            {
                "timestamp": pd.Timestamp("2026-05-06T14:45:00Z"),
                "open": 100.0,
                "high": 101.0,
                "low": 99.0,
                "close": 100.0,
                "volume": 1500,
            }
        )

        asyncio.run(engine._on_closed_bar("AAPL", bar))

        executor = engine.executor
        self.assertEqual(1, len(executor.submitted))
        order = next(iter(engine.state_machine.orders.values()))
        self.assertEqual(100.0, order.intended_price)
        self.assertEqual(0.0, order.expected_spread_bps)


class SubmitFillReconciliationTests(unittest.TestCase):
    def make_engine(self) -> StreamExecutionEngine:
        temp_dir = tempfile.TemporaryDirectory()
        self.addCleanup(temp_dir.cleanup)
        root = Path(temp_dir.name)
        config = Config(
            mode="paper",
            asset_class="equity",
            journal_path=str(root / "journal.csv"),
            state_path=str(root / "state.json"),
            alpaca_api_key="key",
            alpaca_secret_key="secret",
        )
        engine = StreamExecutionEngine(config, Journal(config.journal_path))
        return engine

    def make_order(self, qty: float = 5.0) -> LiveOrder:
        return LiveOrder(
            client_order_id="mc-p-momentum-entry-AAPL-test",
            symbol="AAPL",
            strategy="momentum",
            side="buy",
            intended_qty=qty,
            intended_price=100.0,
            signal_price=100.0,
            stop_price=95.0,
            entry_exit="entry",
            bar_time=pd.Timestamp("2026-05-06T14:45:00Z"),
        )

    def test_immediate_filled_submit_rebuilds_position(self) -> None:
        engine = self.make_engine()
        order = self.make_order()
        engine.state_machine.register_intent(order)

        asyncio.run(
            engine._submit_order_with_retry(
                order,
                lambda: {"status": "filled", "filled_qty": "5", "filled_avg_price": "101", "id": "broker-1"},
            )
        )

        position = engine.broker_state.positions["AAPL"]
        self.assertEqual(5.0, position.qty)
        self.assertEqual(101.0, position.avg_entry_price)
        self.assertEqual(95.0, position.stop_price)
        self.assertEqual(6.0, position.initial_risk_per_unit)
        self.assertEqual(5.0, engine.state_machine.orders[order.client_order_id].last_processed_fill_qty)

    def test_immediate_partially_filled_submit_rebuilds_position_once(self) -> None:
        engine = self.make_engine()
        order = self.make_order()
        engine.state_machine.register_intent(order)

        asyncio.run(
            engine._submit_order_with_retry(
                order,
                lambda: {"status": "partially_filled", "filled_qty": "2", "filled_avg_price": "101", "id": "broker-1"},
            )
        )
        tracked = engine.state_machine.apply_update(
            order.client_order_id,
            "partially_filled",
            2.0,
            101.0,
            broker_order_id="broker-1",
        )
        engine._rebuild_position_from_fills(tracked)

        self.assertEqual(2.0, engine.broker_state.positions["AAPL"].qty)
        self.assertEqual(2.0, engine.state_machine.orders[order.client_order_id].last_processed_fill_qty)

    def test_duplicate_client_order_rejection_recovers_filled_broker_order(self) -> None:
        engine = self.make_engine()
        position = LivePosition(
            symbol="AAPL",
            strategy="momentum",
            side="buy",
            qty=5.0,
            avg_entry_price=100.0,
            stop_price=95.0,
            initial_risk_per_unit=5.0,
            entry_time=pd.Timestamp("2026-05-06T14:45:00Z"),
            available_qty=5.0,
        )
        engine.broker_state.positions["AAPL"] = position
        order = LiveOrder(
            client_order_id="mc-p-momentum-exitschedule-AAPL-test",
            symbol="AAPL",
            strategy="momentum",
            side="sell",
            intended_qty=5.0,
            intended_price=101.0,
            signal_price=101.0,
            stop_price=95.0,
            entry_exit="exit",
        )
        engine.state_machine.register_intent(order)
        response = requests.Response()
        response.status_code = 422
        response.reason = "Unprocessable Entity"
        response._content = b'{"message":"client_order_id must be unique"}'
        exc = requests.HTTPError("422 Client Error", response=response)

        def submitter() -> dict[str, str]:
            raise exc

        class LookupResponse:
            def json(self) -> list[dict[str, str]]:
                return [
                    {
                        "client_order_id": order.client_order_id,
                        "status": "filled",
                        "filled_qty": "5",
                        "filled_avg_price": "101",
                        "id": "broker-filled-1",
                    }
                ]

            def raise_for_status(self) -> None:
                return None

        with patch("microcapital_trader.requests.get", return_value=LookupResponse()):
            asyncio.run(engine._submit_order_with_retry(order, submitter))

        self.assertEqual("filled", engine.state_machine.orders[order.client_order_id].status)
        self.assertNotIn("AAPL", engine.broker_state.positions)


class ExitSubmissionSizingTests(unittest.TestCase):
    class CapturingExitExecutor:
        def __init__(self) -> None:
            self.submitted: list[tuple[LivePosition, float, str, str]] = []

        def submit_exit_order(self, position: LivePosition, qty: float, client_order_id: str, reason: str) -> dict[str, str]:
            self.submitted.append((position, qty, client_order_id, reason))
            return {"status": "accepted", "filled_qty": "0", "filled_avg_price": "0", "id": "broker-exit-1"}

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
        engine.executor = self.CapturingExitExecutor()
        engine.broker_state.uncertain = False
        return engine

    def make_position(self, qty: float = 14.2379, available_qty: float | None = None) -> LivePosition:
        return LivePosition(
            symbol="TQQQ",
            strategy="momentum",
            side="buy",
            qty=qty,
            avg_entry_price=70.21,
            stop_price=69.32,
            initial_risk_per_unit=0.89,
            entry_time=pd.Timestamp("2026-05-06T14:46:00Z"),
            available_qty=available_qty,
        )

    def test_scheduled_exit_skips_when_broker_available_qty_is_zero(self) -> None:
        engine = self.make_engine()
        position = self.make_position(available_qty=0.0)
        engine.broker_state.positions["TQQQ"] = position

        asyncio.run(engine._submit_exit("TQQQ", position, position.qty, "scheduled_close", pd.Timestamp("2026-05-06T19:59:00Z")))

        executor = engine.executor
        self.assertEqual([], executor.submitted)
        self.assertFalse(engine.state_machine.orders)

    def test_scheduled_exit_clamps_to_unreserved_available_qty(self) -> None:
        engine = self.make_engine()
        position = self.make_position(available_qty=14.2379)
        engine.broker_state.positions["TQQQ"] = position
        engine.state_machine.orders["existing-exit"] = LiveOrder(
            client_order_id="existing-exit",
            symbol="TQQQ",
            strategy="momentum",
            side="sell",
            intended_qty=10.0,
            intended_price=71.0,
            signal_price=71.0,
            stop_price=69.32,
            status="accepted",
            entry_exit="exit",
        )

        asyncio.run(engine._submit_exit("TQQQ", position, position.qty, "scheduled_close", pd.Timestamp("2026-05-06T19:59:00Z")))

        executor = engine.executor
        self.assertEqual(1, len(executor.submitted))
        self.assertEqual(4.2379, executor.submitted[0][1])
        new_orders = [order for order in engine.state_machine.orders.values() if order.client_order_id != "existing-exit"]
        self.assertEqual(1, len(new_orders))
        self.assertEqual(4.2379, new_orders[0].intended_qty)


class ReconcileStateTests(unittest.TestCase):
    class FakeResponse:
        def __init__(self, payload: object):
            self.payload = payload

        def json(self) -> object:
            return self.payload

        def raise_for_status(self) -> None:
            return None

    def make_engine(self) -> StreamExecutionEngine:
        temp_dir = tempfile.TemporaryDirectory()
        self.addCleanup(temp_dir.cleanup)
        root = Path(temp_dir.name)
        config = Config(
            mode="paper",
            asset_class="equity",
            journal_path=str(root / "journal.csv"),
            state_path=str(root / "state.json"),
            alpaca_api_key="key",
            alpaca_secret_key="secret",
        )
        return StreamExecutionEngine(config, Journal(config.journal_path))

    def fake_get(self, positions: list[dict[str, str]]):
        def _fake_get(url: str, **kwargs: object) -> ReconcileStateTests.FakeResponse:
            if url.endswith("/v2/orders"):
                return self.FakeResponse([])
            if url.endswith("/v2/positions"):
                return self.FakeResponse(positions)
            if url.endswith("/v2/account"):
                return self.FakeResponse(
                    {
                        "equity": "10000",
                        "cash": "10000",
                        "buying_power": "0",
                        "shorting_enabled": False,
                    }
                )
            raise AssertionError(f"Unexpected URL: {url}")

        return _fake_get

    def write_state(self, engine: StreamExecutionEngine, payload: dict[str, object]) -> None:
        engine.state_path.write_text(json.dumps(payload))

    def test_reconcile_preserves_known_order_and_recovers_position_risk(self) -> None:
        engine = self.make_engine()
        client_order_id = "mc-p-momentum-entry-AAPL-test"
        self.write_state(
            engine,
            {
                "orders": {
                    client_order_id: {
                        "client_order_id": client_order_id,
                        "symbol": "AAPL",
                        "strategy": "momentum",
                        "side": "buy",
                        "intended_qty": 5.0,
                        "intended_price": 100.0,
                        "signal_price": 100.0,
                        "stop_price": 95.0,
                        "status": "filled",
                        "filled_qty": 5.0,
                        "last_processed_fill_qty": 5.0,
                        "avg_fill_price": 101.0,
                        "last_update_time": "2026-05-06T14:46:00+00:00",
                        "entry_exit": "entry",
                    }
                },
                "positions": {},
            },
        )

        with patch(
            "microcapital_trader.requests.get",
            side_effect=self.fake_get([{"symbol": "AAPL", "qty": "5", "avg_entry_price": "101", "unrealized_pl": "0"}]),
        ):
            engine.reconcile_state()

        self.assertIn(client_order_id, engine.state_machine.orders)
        position = engine.broker_state.positions["AAPL"]
        self.assertFalse(position.recovery_only)
        self.assertEqual(95.0, position.stop_price)
        self.assertEqual(6.0, position.initial_risk_per_unit)

    def test_reconcile_keeps_missing_risk_position_unmanaged_but_not_symbol_blocked(self) -> None:
        engine = self.make_engine()
        self.write_state(engine, {"orders": {}, "positions": {}, "recovery_only": ["AAPL"]})

        with patch(
            "microcapital_trader.requests.get",
            side_effect=self.fake_get([{"symbol": "AAPL", "qty": "5", "avg_entry_price": "101", "unrealized_pl": "0"}]),
        ):
            engine.reconcile_state()

        position = engine.broker_state.positions["AAPL"]
        self.assertTrue(position.recovery_only)
        self.assertNotIn("AAPL", engine.broker_state.recovery_only_symbols)
        self.assertTrue(engine._can_trade_symbol("AAPL"))


class LiveConfigValidationTests(unittest.TestCase):
    def test_paper_mode_rejects_live_endpoint(self) -> None:
        temp_dir = tempfile.TemporaryDirectory()
        self.addCleanup(temp_dir.cleanup)
        root = Path(temp_dir.name)
        config = Config(
            mode="paper",
            journal_path=str(root / "journal.csv"),
            state_path=str(root / "state.json"),
            alpaca_api_key="key",
            alpaca_secret_key="secret",
            alpaca_base_url="https://api.alpaca.markets",
        )
        engine = StreamExecutionEngine(config, Journal(config.journal_path))

        with self.assertRaisesRegex(ValueError, "paper mode requires"):
            engine._validate_live_config()


if __name__ == "__main__":
    unittest.main()
