import asyncio
import csv
import io
import json
import tempfile
import unittest
from contextlib import redirect_stdout
from pathlib import Path
from unittest.mock import AsyncMock, patch

import pandas as pd
import requests

import microcapital_trader as trader
from microcapital_trader import (
    BacktestEngine,
    Config,
    DataLoader,
    Journal,
    LiveOrder,
    LivePosition,
    OptimizationWindow,
    Signal,
    StreamExecutionEngine,
    StreamQuote,
    SymbolBasket,
    VWAPMomentumBreakoutStrategy,
    format_http_error,
    optimize_for_total_return,
    utc_now,
)


def journal_rows(path: str) -> list[dict[str, str]]:
    with Path(path).open(newline="") as handle:
        return list(csv.DictReader(handle))


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


class MomentumSignalRobustnessTests(unittest.TestCase):
    def test_signal_skips_rows_with_pd_na_boolean_inputs(self) -> None:
        config = Config(allow_short=True)
        history = pd.DataFrame(
            [
                {
                    "timestamp": pd.Timestamp("2026-05-06T14:45:00Z"),
                    "open": 100.0,
                    "high": 101.0,
                    "low": 99.0,
                    "close": 98.0,
                    "volume": 1000,
                    "ema_fast": 97.0,
                    "ema_slow": 99.0,
                    "vwap": 100.0,
                    "recent_high": 101.0,
                    "recent_low": 99.0,
                    "rvol": 2.0,
                    "strong_close": 0.2,
                    "weak_close": pd.NA,
                }
            ]
        )

        signal = VWAPMomentumBreakoutStrategy().generate_signal("AAPL", history, config, estimated_spread_bps=4.0)

        self.assertIsNone(signal)


class OptimizationRunnerTests(unittest.TestCase):
    def test_optimizer_writes_guarded_ranked_results(self) -> None:
        temp_dir = tempfile.TemporaryDirectory()
        self.addCleanup(temp_dir.cleanup)
        output_path = str(Path(temp_dir.name) / "optimization_results.csv")
        config = Config(
            mode="optimize",
            symbols=["AAA", "BBB"],
            optimization_output_path=output_path,
        )
        baskets = [SymbolBasket("baseline_default", ("AAA", "BBB"))]
        windows = [
            OptimizationWindow("current_30d", 30, "2026-05-07T00:00:00+00:00"),
            OptimizationWindow("prior_30d", 30, "2026-04-07T00:00:00+00:00"),
        ]

        def fake_run(engine: BacktestEngine) -> dict[str, object]:
            total_return = 12.0 if not engine.config.allow_short else 5.0
            return {
                "strategy": "momentum",
                "ending_equity": 1000.0 + total_return,
                "total_return": total_return,
                "win_rate": 55.0 if not engine.config.allow_short else 45.0,
                "average_r": 0.4 if not engine.config.allow_short else 0.1,
                "max_drawdown": -3.0,
                "trade_count": 20,
                "avg_slippage": 0.0,
                "avg_spread_cost": 0.0,
                "avg_holding_time_hours": 1.0,
                "rejected_trade_count": 1,
                "realized_reward_to_risk": 1.5,
                "data_sources": "{}",
                "data_source_errors": "{}",
                "trades_by_session_bucket": "{}",
                "quote_source_counts": "{}",
                "fallback_count": 0,
                "modeled_spread_bps": 4.0,
                "modeled_slippage_bps": 2.0,
                "risk_profile": engine.config.risk_profile,
                "capital_deployment_fraction": engine.config.capital_deployment_fraction,
                "max_buying_power_used": 1000.0,
                "synthetic_data_used": False,
                "performance_warning": "",
            }

        with patch.object(BacktestEngine, "run", fake_run):
            frame = optimize_for_total_return(
                config,
                symbol_baskets=baskets,
                timeframes=["15Min"],
                windows=windows,
                short_modes=[True, False],
                risk_profiles=["conservative"],
            )

        self.assertTrue(Path(output_path).exists())
        required_columns = {
            "candidate_name",
            "symbols",
            "timeframe",
            "allow_short",
            "risk_profile",
            "total_return",
            "max_drawdown",
            "win_rate",
            "average_r",
            "trade_count",
            "rejected_trade_count",
            "realized_reward_to_risk",
            "synthetic_data_used",
        }
        self.assertTrue(required_columns.issubset(frame.columns))
        self.assertFalse(bool(frame.iloc[0]["allow_short"]))
        self.assertTrue(bool(frame.iloc[0]["validation_pass"]))
        self.assertTrue(bool(frame.iloc[0]["recommended_default"]))


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
        self.assertEqual("fallback_signal", quote["quote_source"])
        self.assertTrue(quote["fallback_used"])
        self.assertEqual(0.0, quote["quote_age_seconds"])

    def test_stream_quote_records_non_blocking_submission_metadata(self) -> None:
        engine = self.make_engine()
        quote_time = utc_now() - pd.Timedelta(seconds=45)
        engine.quote_cache["TQQQ"] = StreamQuote(
            symbol="TQQQ",
            bid=70.0,
            ask=70.7,
            mid=70.35,
            spread_bps=99.502488,
            timestamp=quote_time,
        )

        quote = engine._stream_quote_snapshot_or_fallback("TQQQ", 70.15, pd.Timestamp("2026-05-06T14:45:00Z"))

        self.assertEqual("stream", quote["quote_source"])
        self.assertFalse(quote["fallback_used"])
        self.assertGreaterEqual(quote["quote_age_seconds"], 45.0)
        self.assertEqual(99.502488, quote["spread_bps"])


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
        self.assertEqual("fallback_signal", order.quote_source)
        self.assertTrue(order.fallback_used)

        rows = journal_rows(engine.config.journal_path)
        new_row = next(row for row in rows if row["event"] == "new")
        self.assertIn("quote_source=fallback_signal", new_row["notes"])
        self.assertIn("fallback_used=true", new_row["notes"])
        self.assertIn("quote_age_seconds=0.0", new_row["notes"])
        self.assertIn("spread_bps=0.0", new_row["notes"])
        self.assertIn("broker_rejected=false", new_row["notes"])


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
        filled_row = next(row for row in journal_rows(engine.config.journal_path) if row["event"] == "filled")
        self.assertIn("actual_slippage_bps=100.0", filled_row["notes"])
        self.assertIn("broker_rejected=false", filled_row["notes"])

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

    def test_broker_rejection_notes_non_blocking_metric(self) -> None:
        engine = self.make_engine()
        order = self.make_order()
        engine.state_machine.register_intent(order)
        response = requests.Response()
        response.status_code = 403
        response.reason = "Forbidden"
        response._content = b'{"message":"insufficient buying power"}'
        exc = requests.HTTPError("403 Client Error", response=response)

        class EmptyLookupResponse:
            def json(self) -> list[dict[str, str]]:
                return []

            def raise_for_status(self) -> None:
                return None

        with patch("microcapital_trader.requests.get", return_value=EmptyLookupResponse()):
            asyncio.run(
                engine._submit_order_with_retry(
                    order,
                    lambda: (_ for _ in ()).throw(exc),
                )
            )

        rejected_row = next(row for row in journal_rows(engine.config.journal_path) if row["event"] == "rejected")
        self.assertIn("HTTP 403 Forbidden", rejected_row["rejection_reason"])
        self.assertIn("broker_rejected=true", rejected_row["notes"])
        self.assertIn("AAPL", engine.broker_state.cooldowns)

    def test_429_retry_respects_retry_after_then_recovers_fill(self) -> None:
        engine = self.make_engine()
        order = self.make_order()
        engine.state_machine.register_intent(order)
        response = requests.Response()
        response.status_code = 429
        response.reason = "Too Many Requests"
        response.headers["Retry-After"] = "7"
        response._content = b'{"message":"rate limited"}'
        exc = requests.HTTPError("429 Client Error", response=response)
        attempts = {"count": 0}

        def submitter() -> dict[str, str]:
            attempts["count"] += 1
            if attempts["count"] == 1:
                raise exc
            return {"status": "filled", "filled_qty": "5", "filled_avg_price": "101", "id": "broker-1"}

        with patch("asyncio.sleep", new_callable=AsyncMock) as sleep_mock:
            asyncio.run(engine._submit_order_with_retry(order, submitter))

        self.assertEqual(2, attempts["count"])
        sleep_mock.assert_awaited_once_with(7.0)
        self.assertEqual("filled", engine.state_machine.orders[order.client_order_id].status)

    def test_expanded_order_statuses_are_supported(self) -> None:
        engine = self.make_engine()
        order = self.make_order()
        engine.state_machine.register_intent(order)

        engine.state_machine.apply_update(order.client_order_id, "pending_new", 0.0, 0.0)
        tracked = engine.state_machine.apply_update(order.client_order_id, "done_for_day", 0.0, 0.0)

        self.assertEqual("done_for_day", tracked.status)


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

    def fake_get(self, positions: list[dict[str, str]], account: dict[str, str | bool] | None = None):
        account_payload = account or {
            "equity": "10000",
            "cash": "10000",
            "buying_power": "0",
            "shorting_enabled": False,
        }

        def _fake_get(url: str, **kwargs: object) -> ReconcileStateTests.FakeResponse:
            if url.endswith("/v2/orders"):
                return self.FakeResponse([])
            if url.endswith("/v2/positions"):
                return self.FakeResponse(positions)
            if url.endswith("/v2/account"):
                return self.FakeResponse(account_payload)
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

    def test_reconcile_logs_recovery_only_once_per_session(self) -> None:
        engine = self.make_engine()
        self.write_state(engine, {"orders": {}, "positions": {}})
        positions = [{"symbol": "AAPL", "qty": "5", "avg_entry_price": "101", "unrealized_pl": "0"}]

        with patch("microcapital_trader.requests.get", side_effect=self.fake_get(positions)):
            engine.reconcile_state()
            engine.reconcile_state()

        recovery_rows = [row for row in journal_rows(engine.config.journal_path) if row["event"] == "recovery_only"]
        self.assertEqual(1, len(recovery_rows))
        self.assertEqual("state", recovery_rows[0]["entry_exit"])
        self.assertIn("position_state=recovery_only", recovery_rows[0]["notes"])
        self.assertIn("reason=missing_stop_or_risk_metadata", recovery_rows[0]["notes"])

    def test_reconcile_logs_account_snapshot_only_when_daily_pnl_changes(self) -> None:
        engine = self.make_engine()
        self.write_state(engine, {"orders": {}, "positions": {}, "daily_realized_pnl": 0.0})

        with patch("microcapital_trader.requests.get", side_effect=self.fake_get([])):
            engine.reconcile_state()
            engine.reconcile_state()

        account_rows = [row for row in journal_rows(engine.config.journal_path) if row["event"] == "account_snapshot"]
        self.assertEqual(1, len(account_rows))
        self.assertIn("daily_realized_pnl=0.0", account_rows[0]["notes"])
        self.assertIn("daily_unrealized_pnl=0.0", account_rows[0]["notes"])

        self.write_state(engine, {"orders": {}, "positions": {}, "daily_realized_pnl": 12.5})
        with patch("microcapital_trader.requests.get", side_effect=self.fake_get([])):
            engine.reconcile_state()

        account_rows = [row for row in journal_rows(engine.config.journal_path) if row["event"] == "account_snapshot"]
        self.assertEqual(2, len(account_rows))
        self.assertIn("daily_realized_pnl=12.5", account_rows[-1]["notes"])


class BacktestMetricsTests(unittest.TestCase):
    class OneSignalStrategy:
        def generate_signal(self, symbol: str, history: pd.DataFrame, config: Config, estimated_spread_bps: float) -> Signal | None:
            if len(history) != 2:
                return None
            return Signal(
                strategy="metric_test",
                symbol=symbol,
                side="buy",
                entry=100.0,
                stop=95.0,
                target_1=105.0,
                target_2=110.0,
                reason="test_signal",
                bar_time=history.iloc[-1]["timestamp"],
            )

    def test_backtest_journal_and_summary_include_modeled_metrics_without_extra_rejections(self) -> None:
        temp_dir = tempfile.TemporaryDirectory()
        self.addCleanup(temp_dir.cleanup)
        root = Path(temp_dir.name)
        config = Config(
            mode="backtest",
            strategy="metric_test",
            asset_class="equity",
            symbols=["AAPL"],
            journal_path=str(root / "journal.csv"),
            state_path=str(root / "state.json"),
            spread_bps=7.0,
            slippage_bps=3.0,
        )
        data = pd.DataFrame(
            [
                {"timestamp": pd.Timestamp("2026-05-06T14:30:00Z"), "open": 99.0, "high": 101.0, "low": 98.0, "close": 100.0, "volume": 1000, "ema_fast": 100.0, "symbol": "AAPL"},
                {"timestamp": pd.Timestamp("2026-05-06T14:45:00Z"), "open": 100.0, "high": 101.0, "low": 99.0, "close": 100.0, "volume": 1200, "ema_fast": 100.0, "symbol": "AAPL"},
                {"timestamp": pd.Timestamp("2026-05-06T15:00:00Z"), "open": 100.0, "high": 101.0, "low": 94.0, "close": 95.0, "volume": 1300, "ema_fast": 100.0, "symbol": "AAPL"},
            ]
        )

        with patch.dict(trader.STRATEGIES, {"metric_test": self.OneSignalStrategy()}):
            with patch("microcapital_trader.DataLoader.load_historical_data", return_value=data):
                result = BacktestEngine(config, Journal(config.journal_path)).run()

        self.assertEqual(1, result["trade_count"])
        self.assertEqual(0, result["fallback_count"])
        self.assertEqual('{"bar_model": 1}', result["quote_source_counts"])
        self.assertEqual(7.0, result["modeled_spread_bps"])
        self.assertEqual(3.0, result["modeled_slippage_bps"])
        self.assertEqual(0, result["rejected_trade_count"])

        trade_row = next(row for row in journal_rows(config.journal_path) if row["entry_exit"] == "round_trip")
        self.assertIn("quote_source=bar_model", trade_row["notes"])
        self.assertIn("fallback_used=false", trade_row["notes"])
        self.assertIn("quote_age_seconds=0", trade_row["notes"])
        self.assertIn("spread_bps=7.0", trade_row["notes"])
        self.assertIn("modeled_slippage_bps=3.0", trade_row["notes"])


class AggressiveBacktestParityTests(unittest.TestCase):
    class TwoSymbolSignalStrategy:
        def generate_signal(self, symbol: str, history: pd.DataFrame, config: Config, estimated_spread_bps: float) -> Signal | None:
            if len(history) != 2:
                return None
            return Signal(
                strategy="parity_test",
                symbol=symbol,
                side="buy",
                entry=100.0,
                stop=95.0,
                target_1=105.0,
                target_2=110.0,
                reason="test_signal",
                bar_time=history.iloc[-1]["timestamp"],
            )

    def make_data(self, symbol: str) -> pd.DataFrame:
        return pd.DataFrame(
            [
                {"timestamp": pd.Timestamp("2026-05-06T14:30:00Z"), "open": 99.0, "high": 101.0, "low": 98.0, "close": 100.0, "volume": 1000, "ema_fast": 90.0, "symbol": symbol},
                {"timestamp": pd.Timestamp("2026-05-06T14:45:00Z"), "open": 100.0, "high": 101.0, "low": 99.0, "close": 100.0, "volume": 1200, "ema_fast": 90.0, "symbol": symbol},
                {"timestamp": pd.Timestamp("2026-05-06T15:00:00Z"), "open": 100.0, "high": 101.0, "low": 99.0, "close": 100.0, "volume": 1300, "ema_fast": 90.0, "symbol": symbol},
            ]
        )

    def test_chronological_backtest_uses_margin_budget_across_symbols(self) -> None:
        temp_dir = tempfile.TemporaryDirectory()
        self.addCleanup(temp_dir.cleanup)
        root = Path(temp_dir.name)
        config = Config(
            mode="backtest",
            strategy="parity_test",
            risk_profile="aggressive_margin",
            asset_class="equity",
            symbols=["AAA", "BBB"],
            starting_capital=1000.0,
            max_gross_exposure=3000.0,
            risk_per_trade=0.01,
            journal_path=str(root / "journal.csv"),
            state_path=str(root / "state.json"),
        )

        with patch.dict(trader.STRATEGIES, {"parity_test": self.TwoSymbolSignalStrategy()}):
            with patch("microcapital_trader.DataLoader.load_historical_data", side_effect=lambda symbol: self.make_data(symbol)):
                result = BacktestEngine(config, Journal(config.journal_path)).run()

        self.assertEqual(2, result["trade_count"])
        self.assertEqual("aggressive_margin", result["risk_profile"])
        self.assertGreater(result["max_buying_power_used"], config.starting_capital)
        rows = [row for row in journal_rows(config.journal_path) if row["entry_exit"] == "round_trip"]
        self.assertEqual({"AAA", "BBB"}, {row["symbol"] for row in rows})
        self.assertTrue(all("execution_model=closed_bar_market" in row["notes"] for row in rows))

    def test_strict_data_refuses_synthetic_fallback(self) -> None:
        temp_dir = tempfile.TemporaryDirectory()
        self.addCleanup(temp_dir.cleanup)
        root = Path(temp_dir.name)
        config = Config(symbols=["NOLOCAL"], data_dir=str(root / "missing"), strict_data=True)

        with self.assertRaisesRegex(RuntimeError, "Strict data mode refused synthetic"):
            DataLoader(config).load_historical_data("NOLOCAL")


class StreamLifecycleTests(unittest.TestCase):
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
            alpaca_api_key="key",
            alpaca_secret_key="secret",
        )
        return StreamExecutionEngine(config, Journal(config.journal_path))

    def test_stream_run_does_not_print_implementation_plan_by_default(self) -> None:
        engine = self.make_engine()
        history = pd.DataFrame(
            [
                {
                    "timestamp": pd.Timestamp("2026-05-06T14:30:00Z"),
                    "open": 100.0,
                    "high": 101.0,
                    "low": 99.0,
                    "close": 100.5,
                    "volume": 1000,
                    "symbol": "AAPL",
                }
            ]
        )

        with patch("microcapital_trader.ALPACA_AVAILABLE", True):
            with patch("microcapital_trader.TradingClient", return_value=object()):
                with patch("microcapital_trader.DataLoader.load_historical_data", return_value=history):
                    with patch.object(StreamExecutionEngine, "_validate_live_config"):
                        with patch.object(StreamExecutionEngine, "_safe_reconcile"):
                            with patch.object(
                                StreamExecutionEngine,
                                "_run_streams",
                                side_effect=asyncio.CancelledError,
                            ):
                                output = io.StringIO()
                                with redirect_stdout(output):
                                    with self.assertRaises(asyncio.CancelledError):
                                        asyncio.run(engine.run())

        self.assertNotIn("Implementation plan", output.getvalue())

    def test_data_stream_auth_failure_bubbles_and_closes_connection(self) -> None:
        class FailingDataStream:
            def __init__(self) -> None:
                self._running = False
                self.closed = False

            async def _start_ws(self) -> None:
                raise ValueError("connection limit exceeded")

            async def _send_subscribe_msg(self) -> None:
                raise AssertionError("subscribe should not run after auth failure")

            async def _consume(self) -> None:
                raise AssertionError("consume should not run after auth failure")

            async def close(self) -> None:
                self.closed = True

        engine = self.make_engine()
        stream = FailingDataStream()

        with self.assertRaisesRegex(ValueError, "connection limit exceeded"):
            asyncio.run(engine._run_data_stream(stream))

        self.assertFalse(stream._running)
        self.assertTrue(stream.closed)

    def test_shutdown_signals_streams_before_forcing_task_cancellation(self) -> None:
        class StoppableStream:
            def __init__(self) -> None:
                self.stop_event = asyncio.Event()
                self.calls: list[str] = []

            async def stop_ws(self) -> None:
                self.calls.append("stop_ws")
                self.stop_event.set()

            async def close(self) -> None:
                self.calls.append("close")

        async def run_case() -> tuple[StoppableStream, StoppableStream, bool, bool, bool]:
            engine = self.make_engine()
            data_stream = StoppableStream()
            trading_stream = StoppableStream()

            async def wait_for_stop(stream: StoppableStream) -> None:
                await stream.stop_event.wait()

            async def wait_forever() -> None:
                await asyncio.Event().wait()

            data_task = asyncio.create_task(wait_for_stop(data_stream))
            trading_task = asyncio.create_task(wait_for_stop(trading_stream))
            watcher_task = asyncio.create_task(wait_forever())

            await engine._shutdown_streams(
                streams=(data_stream, trading_stream),
                stream_tasks=(data_task, trading_task),
                watcher_task=watcher_task,
            )

            return (
                data_stream,
                trading_stream,
                data_task.done(),
                trading_task.done(),
                watcher_task.cancelled(),
            )

        data_stream, trading_stream, data_done, trading_done, watcher_cancelled = asyncio.run(
            run_case()
        )

        self.assertEqual(["stop_ws", "close"], data_stream.calls)
        self.assertEqual(["stop_ws", "close"], trading_stream.calls)
        self.assertTrue(data_done)
        self.assertTrue(trading_done)
        self.assertTrue(watcher_cancelled)


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
