#!/usr/bin/env python3
from __future__ import annotations

import argparse
import asyncio
import csv
import hashlib
import json
import math
import os
import random
from abc import ABC, abstractmethod
from dataclasses import dataclass, field, replace
from datetime import UTC, date, datetime, time, timedelta
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

import pandas as pd
import requests
from dotenv import load_dotenv

try:
    from alpaca.data.enums import CryptoFeed, DataFeed
    from alpaca.data.historical import CryptoHistoricalDataClient, StockHistoricalDataClient
    from alpaca.data.live import CryptoDataStream, StockDataStream
    from alpaca.data.requests import (
        CryptoBarsRequest,
        CryptoLatestQuoteRequest,
        StockBarsRequest,
        StockLatestQuoteRequest,
    )
    from alpaca.data.timeframe import TimeFrame, TimeFrameUnit
    from alpaca.trading.client import TradingClient
    from alpaca.trading.requests import GetOrdersRequest
    from alpaca.trading.stream import TradingStream

    ALPACA_AVAILABLE = True
except ImportError:  # pragma: no cover - optional dependency path
    ALPACA_AVAILABLE = False


PLAN_TEXT = """Implementation plan
- Modules/classes: Config, Journal, DataLoader, Strategy base, VWAPMomentumBreakoutStrategy, MeanReversionStrategy, BacktestEngine, OrderStateMachine, BrokerState, EquitiesExecutor, CryptoExecutor, StreamExecutionEngine, TradingBot.
- Data flow: historical/stream data -> indicators/bar aggregation -> strategy signal on bar close -> order intent/submission -> broker updates/fills -> order/position state -> CSV journal and backtest metrics.
- Strategy plug-in: strategies expose generate_signal(history, symbol, config, spread_bps); bot selects from STRATEGIES by name and can compare them in backtests.
- Modes: backtest replays bars with modeled spread/slippage; paper/live use Alpaca REST for startup/reconcile/order submit and WebSocket streams for market/order events.
- WebSocket execution: market data stream feeds bar aggregation, closed bars trigger signals, trading stream feeds order state transitions and fills.
- State machine: explicit new/accepted/partially_filled/filled/canceled/rejected transitions with validation; positions are rebuilt from actual fills only.
- Equities vs crypto: separate executors and separate data streams/quote fetches; equities support fractional shares with stock rules, crypto uses crypto-specific submission behavior.
- Slippage: backtest applies spread + slippage at entry and exit; paper/live log signal price, intended price, actual fill, estimated spread cost, estimated slippage.
- Reconciliation: reconcile_state() runs on startup, after exceptions, after reconnects, and before new orders when state is uncertain by fetching open orders and positions and rebuilding local state.
"""

SCHEDULED_MODE_MAP: dict[str, str] = {
    "scheduled_paper": "paper",
    "scheduled_live": "live",
}


ORDER_TRANSITIONS: dict[str, set[str]] = {
    "new": {"accepted", "partially_filled", "filled", "canceled", "rejected", "pending_reconcile"},
    "pending_reconcile": {"accepted", "partially_filled", "filled", "canceled", "rejected", "pending_reconcile"},
    "accepted": {"partially_filled", "filled", "canceled", "rejected"},
    "partially_filled": {"partially_filled", "filled", "canceled", "rejected"},
    "filled": set(),
    "canceled": set(),
    "rejected": set(),
}


@dataclass
class Config:
    mode: str = "backtest"
    asset_class: str = "equity"
    strategy: str = "momentum"
    compare_strategies: list[str] = field(default_factory=lambda: ["momentum", "mean_reversion", "bb_mean_reversion_long", "premarket_regression"])
    symbols: list[str] = field(default_factory=lambda: ["AAPL", "MSFT", "AMD"])
    timeframe: str = "15Min"
    lookback_days: int = 30
    starting_capital: float = 1_000.0
    risk_per_trade: float = 0.005
    max_daily_loss: float = 0.02
    max_trades_per_day: int = 4
    max_open_positions: int = 3
    max_symbols_per_run: int = 3
    allow_short: bool = True
    allow_fractional_equities: bool = True
    spread_bps: float = 4.0
    slippage_bps: float = 2.0
    fee_per_trade: float = 0.0
    breakout_lookback: int = 20
    ema_fast: int = 9
    ema_slow: int = 20
    vwap_window: int = 30
    rvol_window: int = 20
    rvol_threshold: float = 1.5
    candle_body_threshold: float = 0.6
    mean_reversion_window: int = 20
    mean_reversion_zscore: float = 1.2
    mean_reversion_trend_window: int = 100
    mean_reversion_relative_weakness_threshold: float = -0.01
    bb_mean_reversion_window: int = 20
    bb_mean_reversion_stddev: float = 1.7
    bb_mean_reversion_stop_loss_pct: float = 0.006
    bb_mean_reversion_order_size: float = 100.0
    bb_mean_reversion_pyramiding: int = 3
    stop_buffer_pct: float = 0.0025
    partial_take_profit_r: float = 1.0
    final_take_profit_r: float = 2.0
    compare_output_path: str = "strategy_comparison.csv"
    journal_path: str = "trade_journal.csv"
    state_path: str = "live_state.json"
    data_dir: str = "data"
    session_start_hour_utc: int = 13
    session_end_hour_utc: int = 20
    paper_feed: str = "iex"
    live_feed: str = "sip"
    crypto_feed: str = "us"
    reconnect_backoff_seconds: float = 2.0
    reconnect_backoff_max_seconds: float = 30.0
    reconnect_jitter_seconds: float = 1.0
    market_data_stale_seconds: int = 120
    trade_update_stale_seconds: int = 180
    stream_startup_grace_seconds: int = 45
    websocket_ping_interval_seconds: int = 20
    websocket_ping_timeout_seconds: int = 60
    max_gross_exposure: float = 900.0
    max_symbol_exposure: float = 300.0
    max_position_notional: float = 300.0
    max_spread_bps_live: float = 12.0
    max_slippage_deviation_bps: float = 20.0
    cooldown_minutes_after_rejection: int = 30
    cooldown_minutes_after_stop: int = 30
    recent_order_lookup_minutes: int = 240
    max_submit_retries: int = 2
    market_timezone: str = "America/New_York"
    market_open_hour_local: int = 9
    market_open_minute_local: int = 30
    market_close_hour_local: int = 16
    market_close_minute_local: int = 0
    premarket_regression_symbol: str = "SPY"
    premarket_regression_lookback_minutes: int = 120
    premarket_regression_allocation_fraction: float = 1.0
    schedule_start_minutes_before_open: int = 30
    schedule_flatten_minutes_before_close: int = 1
    schedule_shutdown_minutes_after_close: int = 5
    schedule_poll_seconds: int = 30
    alpaca_api_key: str = ""
    alpaca_secret_key: str = ""
    alpaca_base_url: str = "https://paper-api.alpaca.markets"
    alpaca_data_url: str = "https://data.alpaca.markets"

    @classmethod
    def from_env(cls) -> "Config":
        load_dotenv()
        config = cls(
            alpaca_api_key=os.getenv("ALPACA_API_KEY", ""),
            alpaca_secret_key=os.getenv("ALPACA_SECRET_KEY", ""),
            alpaca_base_url=os.getenv("ALPACA_BASE_URL", "https://paper-api.alpaca.markets"),
            asset_class=os.getenv("ASSET_CLASS", "equity"),
            symbols=[s.strip() for s in os.getenv("SYMBOLS", "AAPL,MSFT,AMD").split(",") if s.strip()],
        )
        if os.getenv("MODE"):
            config.mode = os.getenv("MODE", config.mode)
        if os.getenv("STRATEGY"):
            config.strategy = os.getenv("STRATEGY", config.strategy)
        if os.getenv("RISK_PER_TRADE"):
            config.risk_per_trade = float(os.getenv("RISK_PER_TRADE", config.risk_per_trade))
        if os.getenv("SLIPPAGE_BPS"):
            config.slippage_bps = float(os.getenv("SLIPPAGE_BPS", config.slippage_bps))
        if os.getenv("SPREAD_BPS"):
            config.spread_bps = float(os.getenv("SPREAD_BPS", config.spread_bps))
        if os.getenv("TIMEFRAME"):
            config.timeframe = os.getenv("TIMEFRAME", config.timeframe)
        if os.getenv("PREMARKET_REGRESSION_SYMBOL"):
            config.premarket_regression_symbol = os.getenv("PREMARKET_REGRESSION_SYMBOL", config.premarket_regression_symbol).strip().upper()
        return config


def utc_now() -> pd.Timestamp:
    return pd.Timestamp.now(tz="UTC")


def execution_mode(mode: str) -> str:
    return SCHEDULED_MODE_MAP.get(mode, mode)


def is_scheduled_mode(mode: str) -> bool:
    return mode in SCHEDULED_MODE_MAP


def parse_timeframe(timeframe: str) -> tuple[int, str]:
    if timeframe.endswith("Min"):
        return int(timeframe[:-3]), "minute"
    if timeframe.endswith("Hour"):
        return int(timeframe[:-4]) * 60, "minute"
    if timeframe.endswith("Day"):
        return int(timeframe[:-3]) * 1_440, "minute"
    raise ValueError(f"Unsupported timeframe: {timeframe}")


def canonical_symbol(symbol: str) -> str:
    normalized = symbol.strip().upper()
    if ":" in normalized:
        normalized = normalized.split(":", 1)[1]
    return normalized


def symbol_data_key(symbol: str) -> str:
    canonical = canonical_symbol(symbol)
    return canonical.replace("/", "_").replace("-", "_")


def annualization_factor(timeframe: str) -> float:
    minutes, _ = parse_timeframe(timeframe)
    bars_per_day = max(int(390 / min(minutes, 390)), 1)
    return 252 * bars_per_day


def timeframe_to_pandas_freq(timeframe: str) -> str:
    minutes, _ = parse_timeframe(timeframe)
    return f"{minutes}min"


def timeframe_to_alpaca(timeframe: str) -> Any:
    minutes, _ = parse_timeframe(timeframe)
    if not ALPACA_AVAILABLE:
        return None
    if minutes < 60:
        return TimeFrame(minutes, TimeFrameUnit.Minute)
    if minutes % 60 == 0 and minutes < 1_440:
        return TimeFrame(minutes // 60, TimeFrameUnit.Hour)
    if minutes % 1_440 == 0:
        return TimeFrame(minutes // 1_440, TimeFrameUnit.Day)
    raise ValueError(f"Unsupported Alpaca timeframe: {timeframe}")


def _enum_value(value: Any) -> str:
    raw = getattr(value, "value", value)
    return str(raw).strip()


def utc_timestamp(value: Any) -> pd.Timestamp:
    ts = pd.Timestamp(value)
    return ts.tz_convert("UTC") if ts.tzinfo else ts.tz_localize("UTC")


def coerce_stock_feed(feed: Any) -> Any:
    normalized = _enum_value(feed).lower()
    if not ALPACA_AVAILABLE:
        return normalized
    try:
        return DataFeed(normalized)
    except ValueError as exc:
        raise ValueError(f"Unsupported stock feed: {feed}") from exc


def coerce_crypto_feed(feed: Any) -> Any:
    normalized = _enum_value(feed).lower()
    if not ALPACA_AVAILABLE:
        return normalized
    try:
        return CryptoFeed(normalized)
    except ValueError as exc:
        raise ValueError(f"Unsupported crypto feed: {feed}") from exc


def floor_timestamp(ts: pd.Timestamp, timeframe: str) -> pd.Timestamp:
    minutes, _ = parse_timeframe(timeframe)
    ts_utc = utc_timestamp(ts)
    epoch_minutes = int(ts_utc.timestamp() // 60)
    floored_minutes = epoch_minutes - (epoch_minutes % minutes)
    return pd.Timestamp(floored_minutes * 60, unit="s", tz="UTC")


def ema(series: pd.Series, span: int) -> pd.Series:
    return series.ewm(span=span, adjust=False).mean()


def rolling_vwap(df: pd.DataFrame, window: int) -> pd.Series:
    typical_price = (df["high"] + df["low"] + df["close"]) / 3.0
    price_volume = typical_price * df["volume"]
    denom = df["volume"].rolling(window).sum().replace(0, pd.NA)
    return price_volume.rolling(window).sum() / denom


def relative_volume(volume: pd.Series, window: int) -> pd.Series:
    return volume / volume.rolling(window).mean().replace(0, pd.NA)


def strong_close_fraction(df: pd.DataFrame) -> pd.Series:
    candle_range = (df["high"] - df["low"]).replace(0, pd.NA)
    return (df["close"] - df["low"]) / candle_range


def weak_close_fraction(df: pd.DataFrame) -> pd.Series:
    candle_range = (df["high"] - df["low"]).replace(0, pd.NA)
    return (df["high"] - df["close"]) / candle_range


def max_drawdown(equity_curve: pd.Series) -> float:
    if equity_curve.empty:
        return 0.0
    running_peak = equity_curve.cummax()
    drawdown = (equity_curve / running_peak) - 1.0
    return float(drawdown.min())


def position_size_from_risk(equity: float, entry: float, stop: float, risk_fraction: float) -> float:
    if entry <= 0 or equity <= 0:
        return 0.0
    risk_amount = equity * risk_fraction
    risk_per_unit = abs(entry - stop)
    if risk_amount <= 0 or risk_per_unit <= 0:
        return 0.0
    units_by_risk = risk_amount / risk_per_unit
    units_by_cash = equity / entry
    return max(min(units_by_risk, units_by_cash), 0.0)


def normalize_qty(asset_class: str, qty: float) -> float:
    precision = 4 if asset_class == "equity" else 6
    return round(max(qty, 0.0), precision)


def estimate_costs(price: float, qty: float, spread_bps: float, slippage_bps: float) -> tuple[float, float]:
    spread_cost = price * qty * (spread_bps / 10_000.0)
    slippage_cost = price * qty * (slippage_bps / 10_000.0)
    return spread_cost, slippage_cost


def within_session(ts: pd.Timestamp, config: Config) -> bool:
    if config.asset_class == "crypto":
        return True
    market_open, market_close = market_session_bounds(ts, config)
    ts_utc = utc_timestamp(ts)
    return market_open <= ts_utc < market_close


def market_timezone(config: Config) -> ZoneInfo:
    return ZoneInfo(config.market_timezone)


def market_session_bounds(ts: pd.Timestamp, config: Config) -> tuple[pd.Timestamp, pd.Timestamp]:
    local_ts = utc_timestamp(ts).tz_convert(market_timezone(config))
    session_day = local_ts.date()
    open_local = pd.Timestamp(
        datetime.combine(
            session_day,
            time(config.market_open_hour_local, config.market_open_minute_local),
            tzinfo=market_timezone(config),
        )
    )
    close_local = pd.Timestamp(
        datetime.combine(
            session_day,
            time(config.market_close_hour_local, config.market_close_minute_local),
            tzinfo=market_timezone(config),
        )
    )
    return open_local.tz_convert("UTC"), close_local.tz_convert("UTC")


def linear_regression_slope(values: pd.Series) -> float:
    numeric = pd.to_numeric(values, errors="coerce").dropna().astype("float64")
    count = len(numeric)
    if count < 2:
        return 0.0
    x_mean = (count - 1) / 2.0
    y_mean = float(numeric.mean())
    numerator = 0.0
    denominator = 0.0
    for idx, value in enumerate(numeric):
        dx = idx - x_mean
        numerator += dx * (value - y_mean)
        denominator += dx * dx
    if denominator == 0:
        return 0.0
    return numerator / denominator


def deterministic_client_order_id(
    mode: str,
    strategy: str,
    symbol: str,
    side: str,
    action: str,
    bar_time: pd.Timestamp,
) -> str:
    key = f"{mode}|{strategy}|{symbol}|{side}|{action}|{pd.Timestamp(bar_time).isoformat()}"
    digest = hashlib.sha1(key.encode("utf-8")).hexdigest()[:20]
    safe_strategy = strategy.replace("_", "")[:12]
    safe_action = action.replace("_", "")[:12]
    safe_symbol = symbol.replace("/", "").replace("-", "")[:8]
    return f"mc-{mode[:1]}-{safe_strategy}-{safe_action}-{safe_symbol}-{digest}"


class Journal:
    def __init__(self, path: str):
        self.path = Path(path)
        self.fields = [
            "timestamp",
            "symbol",
            "strategy",
            "side",
            "mode",
            "event",
            "entry_exit",
            "stop",
            "size",
            "signal_price",
            "intended_price",
            "fill_price",
            "slippage",
            "spread_cost",
            "execution_cost",
            "pnl",
            "r_multiple",
            "session_bucket",
            "client_order_id",
            "order_status",
            "filled_qty",
            "remaining_qty",
            "rejection_reason",
            "notes",
        ]
        if not self.path.exists():
            with self.path.open("w", newline="") as handle:
                writer = csv.DictWriter(handle, fieldnames=self.fields)
                writer.writeheader()

    def log(self, row: dict[str, Any]) -> None:
        payload = {field: row.get(field, "") for field in self.fields}
        with self.path.open("a", newline="") as handle:
            writer = csv.DictWriter(handle, fieldnames=self.fields)
            writer.writerow(payload)


def add_indicators(df: pd.DataFrame, config: Config) -> pd.DataFrame:
    data = df.copy()
    data["ema_fast"] = ema(data["close"], config.ema_fast)
    data["ema_slow"] = ema(data["close"], config.ema_slow)
    data["vwap"] = rolling_vwap(data, config.vwap_window)
    data["rvol"] = relative_volume(data["volume"], config.rvol_window)
    data["strong_close"] = strong_close_fraction(data)
    data["weak_close"] = weak_close_fraction(data)
    data["recent_high"] = data["high"].rolling(config.breakout_lookback).max().shift(1)
    data["recent_low"] = data["low"].rolling(config.breakout_lookback).min().shift(1)
    data["mean"] = data["close"].rolling(config.mean_reversion_window).mean()
    data["std"] = data["close"].rolling(config.mean_reversion_window).std()
    data["zscore"] = (data["close"] - data["mean"]) / data["std"].replace(0, pd.NA)
    data["trend_mean"] = data["close"].rolling(config.mean_reversion_trend_window).mean()
    data["trend_relative"] = (data["close"] / data["trend_mean"]) - 1.0
    data["bb_middle"] = data["close"].rolling(config.bb_mean_reversion_window).mean()
    data["bb_std"] = data["close"].rolling(config.bb_mean_reversion_window).std()
    band_width = data["bb_std"] * config.bb_mean_reversion_stddev
    data["bb_upper"] = data["bb_middle"] + band_width
    data["bb_lower"] = data["bb_middle"] - band_width
    previous_close = data["close"].shift(1)
    previous_lower = data["bb_lower"].shift(1)
    data["bb_long_entry"] = (previous_close >= previous_lower) & (data["close"] < data["bb_lower"])
    return data


def session_bucket(ts: pd.Timestamp, config: Config) -> str:
    ts_utc = utc_timestamp(ts)
    if config.asset_class == "crypto":
        hour = ts_utc.hour
        if 0 <= hour < 8:
            return "asia"
        if 8 <= hour < 13:
            return "europe"
        if 13 <= hour < 20:
            return "us"
        return "overnight"
    minutes = ts_utc.hour * 60 + ts_utc.minute
    start = config.session_start_hour_utc * 60
    end = config.session_end_hour_utc * 60
    if minutes < start or minutes >= end:
        return "off_session"
    elapsed = minutes - start
    session_minutes = max(end - start, 1)
    if elapsed < session_minutes * 0.25:
        return "open"
    if elapsed < session_minutes * 0.75:
        return "midday"
    return "close"


class DataLoader:
    def __init__(self, config: Config):
        self.config = config

    def load_historical_data(self, symbol: str) -> pd.DataFrame:
        csv_path = Path(self.config.data_dir) / f"{symbol_data_key(symbol)}_{self.config.timeframe}.csv"
        if csv_path.exists():
            df = pd.read_csv(csv_path, parse_dates=["timestamp"])
            return self._prepare_with_source(df, symbol, f"local_csv:{csv_path}")
        fetch_error = ""
        try:
            fetched = self._fetch_historical_data(symbol)
            if fetched is not None and not fetched.empty:
                return self._prepare_with_source(fetched, symbol, "alpaca")
        except Exception as exc:
            fetch_error = f"{type(exc).__name__}: {exc}"
        fallback = self._prepare_with_source(self._generate_sample_data(symbol), symbol, "synthetic_sample")
        fallback.attrs["source_error"] = fetch_error
        warning = f"Warning: using synthetic sample data for {symbol} ({self.config.timeframe})."
        if fetch_error:
            warning += f" Alpaca fetch failed: {fetch_error}"
        else:
            warning += " No local CSV was found and Alpaca data was unavailable."
        print(warning)
        return fallback

    def latest_quote(self, symbol: str) -> float:
        snapshot = self.latest_quote_snapshot(symbol)
        return float(snapshot.get("mid") or 0.0)

    def latest_quote_snapshot(self, symbol: str) -> dict[str, Any]:
        if not (self.config.alpaca_api_key and self.config.alpaca_secret_key):
            return {"mid": 0.0, "bid": 0.0, "ask": 0.0, "spread_bps": 0.0, "timestamp": None}
        try:
            if ALPACA_AVAILABLE:
                if self.config.asset_class == "equity":
                    client = StockHistoricalDataClient(self.config.alpaca_api_key, self.config.alpaca_secret_key)
                    request = StockLatestQuoteRequest(symbol_or_symbols=symbol, feed=self._stock_feed())
                    quote = client.get_stock_latest_quote(request).get(symbol)
                    if quote:
                        bid = float(quote.bid_price or 0.0)
                        ask = float(quote.ask_price or 0.0)
                        mid = (bid + ask) / 2.0 if bid > 0 and ask > 0 else max(bid, ask)
                        spread_bps = ((ask - bid) / mid) * 10_000.0 if mid > 0 and ask >= bid else 0.0
                        return {
                            "mid": mid,
                            "bid": bid,
                            "ask": ask,
                            "spread_bps": spread_bps,
                            "timestamp": utc_timestamp(getattr(quote, "timestamp", None)) if getattr(quote, "timestamp", None) else None,
                        }
                else:
                    client = CryptoHistoricalDataClient(self.config.alpaca_api_key, self.config.alpaca_secret_key)
                    request = CryptoLatestQuoteRequest(symbol_or_symbols=symbol, feed=self._crypto_feed())
                    quote = client.get_crypto_latest_quote(request).get(symbol)
                    if quote:
                        bid = float(quote.bid_price or 0.0)
                        ask = float(quote.ask_price or 0.0)
                        mid = (bid + ask) / 2.0 if bid > 0 and ask > 0 else max(bid, ask)
                        spread_bps = ((ask - bid) / mid) * 10_000.0 if mid > 0 and ask >= bid else 0.0
                        return {
                            "mid": mid,
                            "bid": bid,
                            "ask": ask,
                            "spread_bps": spread_bps,
                            "timestamp": utc_timestamp(getattr(quote, "timestamp", None)) if getattr(quote, "timestamp", None) else None,
                        }
            endpoint = "stocks" if self.config.asset_class == "equity" else "crypto"
            response = requests.get(
                f"{self.config.alpaca_data_url}/v1beta3/{endpoint}/us/quotes/latest",
                headers=self._alpaca_headers(),
                params={"symbols": symbol},
                timeout=15,
            )
            response.raise_for_status()
            quotes = response.json().get("quotes", {})
            payload = quotes.get(symbol)
            if not payload:
                return {"mid": 0.0, "bid": 0.0, "ask": 0.0, "spread_bps": 0.0, "timestamp": None}
            bid = float(payload.get("bp") or 0.0)
            ask = float(payload.get("ap") or 0.0)
            mid = (bid + ask) / 2.0 if bid > 0 and ask > 0 else max(bid, ask)
            spread_bps = ((ask - bid) / mid) * 10_000.0 if mid > 0 and ask >= bid else 0.0
            return {
                "mid": mid,
                "bid": bid,
                "ask": ask,
                "spread_bps": spread_bps,
                "timestamp": utc_timestamp(payload.get("t")) if payload.get("t") else None,
            }
        except Exception:
            return {"mid": 0.0, "bid": 0.0, "ask": 0.0, "spread_bps": 0.0, "timestamp": None}

    def _alpaca_headers(self) -> dict[str, str]:
        return {
            "APCA-API-KEY-ID": self.config.alpaca_api_key,
            "APCA-API-SECRET-KEY": self.config.alpaca_secret_key,
        }

    def _stock_feed(self) -> Any:
        feed = self.config.paper_feed if "paper" in self.config.alpaca_base_url else self.config.live_feed
        return coerce_stock_feed(feed)

    def _crypto_feed(self) -> Any:
        return coerce_crypto_feed(self.config.crypto_feed)

    def _fetch_historical_data(self, symbol: str) -> pd.DataFrame | None:
        if not (self.config.alpaca_api_key and self.config.alpaca_secret_key):
            return None
        if canonical_symbol(symbol).replace("/", "") == "XAUUSD":
            raise ValueError(
                f"{symbol} is not available from the Alpaca market-data path in this bot. "
                f"Provide local CSV data in {self.config.data_dir}/{symbol_data_key(symbol)}_{self.config.timeframe}.csv."
            )
        end = datetime.now(UTC)
        start = end - timedelta(days=self.config.lookback_days)
        if ALPACA_AVAILABLE:
            timeframe = timeframe_to_alpaca(self.config.timeframe)
            if self.config.asset_class == "equity":
                client = StockHistoricalDataClient(self.config.alpaca_api_key, self.config.alpaca_secret_key)
                request = StockBarsRequest(
                    symbol_or_symbols=symbol,
                    timeframe=timeframe,
                    start=start,
                    end=end,
                    feed=self._stock_feed(),
                )
                bars = client.get_stock_bars(request).df.reset_index()
            else:
                client = CryptoHistoricalDataClient(self.config.alpaca_api_key, self.config.alpaca_secret_key)
                request = CryptoBarsRequest(
                    symbol_or_symbols=symbol,
                    timeframe=timeframe,
                    start=start,
                    end=end,
                    feed=self._crypto_feed(),
                )
                bars = client.get_crypto_bars(request).df.reset_index()
            if bars.empty:
                return None
            bars = bars.rename(columns={"timestamp": "timestamp", "symbol": "symbol"})
            return bars[["timestamp", "open", "high", "low", "close", "volume"]]

        endpoint = "stocks" if self.config.asset_class == "equity" else "crypto/us"
        response = requests.get(
            f"{self.config.alpaca_data_url}/v2/{endpoint}/bars",
            headers=self._alpaca_headers(),
            params={
                "symbols": symbol,
                "timeframe": self.config.timeframe,
                "start": start.isoformat(),
                "end": end.isoformat(),
                "limit": 10_000,
                "adjustment": "raw",
                "feed": _enum_value(self._stock_feed()),
            },
            timeout=30,
        )
        response.raise_for_status()
        bars = response.json().get("bars", {}).get(symbol, [])
        if not bars:
            return None
        rows = []
        for bar in bars:
            rows.append(
                {
                    "timestamp": pd.to_datetime(bar["t"], utc=True),
                    "open": bar["o"],
                    "high": bar["h"],
                    "low": bar["l"],
                    "close": bar["c"],
                    "volume": bar["v"],
                }
            )
        return pd.DataFrame(rows)

    def _generate_sample_data(self, symbol: str) -> pd.DataFrame:
        periods = max(self.config.lookback_days * 26, 250)
        end = datetime.now(UTC).replace(minute=0, second=0, microsecond=0)
        index = pd.date_range(end=end, periods=periods, freq=timeframe_to_pandas_freq(self.config.timeframe), tz="UTC")
        seed = sum(ord(char) for char in canonical_symbol(symbol))
        rows: list[dict[str, Any]] = []
        close = 30.0 + (seed % 50)
        for i, ts in enumerate(index):
            cycle = math.sin(i / 18) * 0.35 + math.cos(i / 9) * 0.18
            micro_trend = ((seed % 7) - 3) * 0.01
            breakout_pulse = 0.0
            if i % 48 in {34, 35, 36, 37}:
                breakout_pulse = 1.1 + (seed % 5) * 0.08
            elif i % 48 in {10, 11, 12, 13}:
                breakout_pulse = -1.0 - (seed % 5) * 0.07
            shock = ((i * ((seed % 11) + 3)) % 17 - 8) * 0.02
            drift = cycle + micro_trend + breakout_pulse + shock
            close = max(5.0, close + drift + shock)
            open_price = max(5.0, close - drift * 0.5)
            wick_bias = 1.006 if breakout_pulse != 0 else 1.003
            high = max(open_price, close) * wick_bias
            low = min(open_price, close) * (2.0 - wick_bias)
            volume_spike = 3.5 if breakout_pulse != 0 else 1.0
            volume = int((20_000 + ((i * 137 + seed * 17) % 8_000)) * volume_spike)
            rows.append(
                {
                    "timestamp": ts,
                    "open": round(open_price, 2),
                    "high": round(high, 2),
                    "low": round(low, 2),
                    "close": round(close, 2),
                    "volume": volume,
                }
            )
        return pd.DataFrame(rows)

    def _prepare_with_source(self, df: pd.DataFrame, symbol: str, source: str) -> pd.DataFrame:
        prepared = self._prepare(df, symbol)
        prepared.attrs["data_source"] = source
        return prepared

    def _prepare(self, df: pd.DataFrame, symbol: str) -> pd.DataFrame:
        clean = df.copy()
        clean["timestamp"] = pd.to_datetime(clean["timestamp"], utc=True)
        clean = clean.sort_values("timestamp").drop_duplicates("timestamp")
        clean["symbol"] = symbol
        return add_indicators(clean.reset_index(drop=True), self.config)


@dataclass
class Signal:
    strategy: str
    symbol: str
    side: str
    entry: float
    stop: float
    target_1: float
    target_2: float
    reason: str
    bar_time: pd.Timestamp


class Strategy(ABC):
    name = "base"

    @abstractmethod
    def generate_signal(
        self,
        symbol: str,
        history: pd.DataFrame,
        config: Config,
        estimated_spread_bps: float,
    ) -> Signal | None:
        raise NotImplementedError

    def position_size(self, capital_base: float, signal: Signal, config: Config) -> float | None:
        return None

    def exit_signal(self, position: Any, history: pd.DataFrame, config: Config) -> tuple[str, float] | None:
        return None

    def max_concurrent_entries(self, config: Config) -> int:
        return 1


class VWAPMomentumBreakoutStrategy(Strategy):
    name = "momentum"

    def generate_signal(
        self,
        symbol: str,
        history: pd.DataFrame,
        config: Config,
        estimated_spread_bps: float,
    ) -> Signal | None:
        row = history.iloc[-1]
        if any(pd.isna(row[key]) for key in ["vwap", "recent_high", "recent_low", "rvol"]):
            return None
        bar_time = row["timestamp"]
        if not within_session(bar_time, config):
            return None

        long_setup = (
            row["close"] > row["vwap"]
            and row["ema_fast"] > row["ema_slow"]
            and row["close"] > row["recent_high"]
            and row["rvol"] > config.rvol_threshold
            and row["strong_close"] >= config.candle_body_threshold
        )
        if long_setup:
            stop = min(row["recent_high"], row["low"]) * (1.0 - config.stop_buffer_pct)
            reward = (row["close"] + abs(row["close"] - stop) * config.final_take_profit_r) - row["close"]
            risk = row["close"] - stop
            if stop < row["close"] and risk > 0 and reward / risk >= 2.0 and estimated_spread_bps <= config.spread_bps * 1.5:
                return Signal(
                    strategy=self.name,
                    symbol=symbol,
                    side="buy",
                    entry=float(row["close"]),
                    stop=float(stop),
                    target_1=float(row["close"] + risk * config.partial_take_profit_r),
                    target_2=float(row["close"] + risk * config.final_take_profit_r),
                    reason="vwap_breakout_long",
                    bar_time=bar_time,
                )

        short_setup = (
            config.allow_short
            and config.asset_class != "crypto"
            and row["close"] < row["vwap"]
            and row["ema_fast"] < row["ema_slow"]
            and row["close"] < row["recent_low"]
            and row["rvol"] > config.rvol_threshold
            and row["weak_close"] >= config.candle_body_threshold
        )
        if short_setup:
            stop = max(row["recent_low"], row["high"]) * (1.0 + config.stop_buffer_pct)
            reward = row["close"] - (row["close"] - abs(stop - row["close"]) * config.final_take_profit_r)
            risk = stop - row["close"]
            if stop > row["close"] and risk > 0 and reward / risk >= 2.0 and estimated_spread_bps <= config.spread_bps * 1.5:
                return Signal(
                    strategy=self.name,
                    symbol=symbol,
                    side="sell",
                    entry=float(row["close"]),
                    stop=float(stop),
                    target_1=float(row["close"] - risk * config.partial_take_profit_r),
                    target_2=float(row["close"] - risk * config.final_take_profit_r),
                    reason="vwap_breakout_short",
                    bar_time=bar_time,
                )
        return None


class MeanReversionStrategy(Strategy):
    name = "mean_reversion"

    def generate_signal(
        self,
        symbol: str,
        history: pd.DataFrame,
        config: Config,
        estimated_spread_bps: float,
    ) -> Signal | None:
        row = history.iloc[-1]
        if any(pd.isna(row[key]) for key in ["zscore", "mean", "vwap", "trend_relative"]):
            return None
        bar_time = row["timestamp"]
        if not within_session(bar_time, config):
            return None

        if (
            row["zscore"] <= -config.mean_reversion_zscore
            and row["close"] < row["vwap"]
            and row["trend_relative"] >= config.mean_reversion_relative_weakness_threshold
        ):
            stop = row["low"] * (1.0 - config.stop_buffer_pct)
            risk = row["close"] - stop
            if stop < row["close"] and risk > 0 and estimated_spread_bps <= config.spread_bps * 1.5:
                return Signal(
                    strategy=self.name,
                    symbol=symbol,
                    side="buy",
                    entry=float(row["close"]),
                    stop=float(stop),
                    target_1=float(row["mean"]),
                    target_2=float(max(row["mean"], row["close"] + risk * config.final_take_profit_r)),
                    reason="mean_reversion_long",
                    bar_time=bar_time,
                )

        if (
            config.allow_short
            and config.asset_class != "crypto"
            and row["zscore"] >= config.mean_reversion_zscore
            and row["close"] > row["vwap"]
            and row["trend_relative"] <= abs(config.mean_reversion_relative_weakness_threshold)
        ):
            stop = row["high"] * (1.0 + config.stop_buffer_pct)
            risk = stop - row["close"]
            if stop > row["close"] and risk > 0 and estimated_spread_bps <= config.spread_bps * 1.5:
                return Signal(
                    strategy=self.name,
                    symbol=symbol,
                    side="sell",
                    entry=float(row["close"]),
                    stop=float(stop),
                    target_1=float(row["mean"]),
                    target_2=float(min(row["mean"], row["close"] - risk * config.final_take_profit_r)),
                    reason="mean_reversion_short",
                    bar_time=bar_time,
                )
        return None


class BollingerMeanReversionLongStrategy(Strategy):
    name = "bb_mean_reversion_long"

    def generate_signal(
        self,
        symbol: str,
        history: pd.DataFrame,
        config: Config,
        estimated_spread_bps: float,
    ) -> Signal | None:
        row = history.iloc[-1]
        if any(pd.isna(row[key]) for key in ["bb_middle", "bb_lower", "bb_long_entry"]):
            return None
        bar_time = row["timestamp"]
        if not within_session(bar_time, config):
            return None
        if estimated_spread_bps > config.spread_bps * 1.5:
            return None
        if not bool(row["bb_long_entry"]):
            return None

        stop = float(row["close"] * (1.0 - config.bb_mean_reversion_stop_loss_pct))
        if stop >= row["close"]:
            return None
        return Signal(
            strategy=self.name,
            symbol=symbol,
            side="buy",
            entry=float(row["close"]),
            stop=stop,
            target_1=float(row["bb_middle"]),
            target_2=float(row["bb_middle"]),
            reason="bb_mean_reversion_long",
            bar_time=bar_time,
        )

    def position_size(self, capital_base: float, signal: Signal, config: Config) -> float | None:
        if signal.entry <= 0 or capital_base <= 0:
            return 0.0
        target_qty = max(config.bb_mean_reversion_order_size, 0.0)
        cash_capped_qty = capital_base / signal.entry
        return min(target_qty, cash_capped_qty)

    def max_concurrent_entries(self, config: Config) -> int:
        return max(config.bb_mean_reversion_pyramiding, 1)

    def exit_signal(self, position: Any, history: pd.DataFrame, config: Config) -> tuple[str, float] | None:
        if history.empty:
            return None
        row = history.iloc[-1]
        if pd.isna(row["bb_middle"]):
            return None
        if row["low"] <= position.stop_price:
            return "stop_loss", float(position.stop_price)
        if row["high"] >= row["bb_middle"]:
            return "middle_band_target", float(row["bb_middle"])
        return None


class PremarketRegressionStrategy(Strategy):
    name = "premarket_regression"

    def generate_signal(
        self,
        symbol: str,
        history: pd.DataFrame,
        config: Config,
        estimated_spread_bps: float,
    ) -> Signal | None:
        target_symbol = config.premarket_regression_symbol.upper()
        if symbol.upper() != target_symbol or history.empty:
            return None
        row = history.iloc[-1]
        bar_time = utc_timestamp(row["timestamp"])
        market_open, _ = market_session_bounds(bar_time, config)
        timeframe_minutes, _ = parse_timeframe(config.timeframe)
        entry_cutoff = market_open + pd.Timedelta(minutes=timeframe_minutes)
        if bar_time < market_open or bar_time >= entry_cutoff:
            return None

        lookback_start = market_open - pd.Timedelta(minutes=config.premarket_regression_lookback_minutes)
        premarket = history[(history["timestamp"] >= lookback_start) & (history["timestamp"] < market_open)].copy()
        if len(premarket) < 2:
            return None

        slope = linear_regression_slope(premarket["close"])
        if slope > 0:
            side = "buy"
            stop = float(min(premarket["low"].min(), row["close"] * (1.0 - config.stop_buffer_pct)))
        elif slope < 0 and config.allow_short and config.asset_class != "crypto":
            side = "sell"
            stop = float(max(premarket["high"].max(), row["close"] * (1.0 + config.stop_buffer_pct)))
        else:
            return None

        return Signal(
            strategy=self.name,
            symbol=target_symbol,
            side=side,
            entry=float(row["close"]),
            stop=stop,
            target_1=float(row["close"]),
            target_2=float(row["close"]),
            reason=f"premarket_regression_{'long' if side == 'buy' else 'short'}",
            bar_time=bar_time,
        )

    def position_size(self, capital_base: float, signal: Signal, config: Config) -> float | None:
        allocation = max(min(config.premarket_regression_allocation_fraction, 1.0), 0.0)
        if capital_base <= 0 or signal.entry <= 0 or allocation <= 0:
            return 0.0
        return (capital_base * allocation) / signal.entry

    def exit_signal(self, position: Any, history: pd.DataFrame, config: Config) -> tuple[str, float] | None:
        if history.empty:
            return None
        row = history.iloc[-1]
        bar_time = utc_timestamp(row["timestamp"])
        _, market_close = market_session_bounds(bar_time, config)
        if bar_time >= market_close:
            return "market_close", float(row["close"])
        return None


STRATEGIES: dict[str, Strategy] = {
    VWAPMomentumBreakoutStrategy.name: VWAPMomentumBreakoutStrategy(),
    MeanReversionStrategy.name: MeanReversionStrategy(),
    BollingerMeanReversionLongStrategy.name: BollingerMeanReversionLongStrategy(),
    PremarketRegressionStrategy.name: PremarketRegressionStrategy(),
}


@dataclass
class LiveOrder:
    client_order_id: str
    symbol: str
    strategy: str
    side: str
    intended_qty: float
    intended_price: float
    signal_price: float
    stop_price: float
    status: str = "new"
    filled_qty: float = 0.0
    remaining_qty: float = 0.0
    avg_fill_price: float = 0.0
    last_update_time: pd.Timestamp = field(default_factory=lambda: pd.Timestamp.now(tz="UTC"))
    rejection_reason: str = ""
    broker_order_id: str = ""
    entry_exit: str = "entry"
    last_fill_delta: float = 0.0
    last_processed_fill_qty: float = 0.0
    expected_spread_bps: float = 0.0
    estimated_spread_cost: float = 0.0
    session_bucket: str = ""
    bar_time: pd.Timestamp | None = None

    def __post_init__(self) -> None:
        self.remaining_qty = round(max(self.intended_qty - self.filled_qty, 0.0), 8)


@dataclass
class LivePosition:
    symbol: str
    strategy: str
    side: str
    qty: float
    avg_entry_price: float
    stop_price: float
    initial_risk_per_unit: float
    entry_time: pd.Timestamp
    entry_count: int = 1
    partial_taken: bool = False
    realized_pnl: float = 0.0
    recovery_only: bool = False
    last_fill_time: pd.Timestamp = field(default_factory=utc_now)


class OrderStateMachine:
    def __init__(self, journal: Journal, mode: str):
        self.orders: dict[str, LiveOrder] = {}
        self.journal = journal
        self.mode = mode

    def register_intent(self, order: LiveOrder) -> None:
        self.orders[order.client_order_id] = order
        self._journal(order, "new", notes="intent_registered")

    def apply_update(
        self,
        client_order_id: str,
        status: str,
        filled_qty: float,
        avg_fill_price: float,
        broker_order_id: str = "",
        rejection_reason: str = "",
        timestamp: pd.Timestamp | None = None,
    ) -> LiveOrder:
        if client_order_id not in self.orders:
            raise KeyError(f"Unknown client_order_id: {client_order_id}")
        order = self.orders[client_order_id]
        status = status.lower()
        if status not in ORDER_TRANSITIONS:
            raise ValueError(f"Unsupported order status: {status}")
        if status != order.status and status not in ORDER_TRANSITIONS[order.status]:
            raise ValueError(f"Invalid order transition {order.status} -> {status} for {client_order_id}")
        previous_filled = order.filled_qty
        order.status = status
        order.filled_qty = round(max(filled_qty, 0.0), 8)
        order.remaining_qty = round(max(order.intended_qty - order.filled_qty, 0.0), 8)
        order.last_fill_delta = round(max(order.filled_qty - previous_filled, 0.0), 8)
        order.avg_fill_price = avg_fill_price or order.avg_fill_price
        order.broker_order_id = broker_order_id or order.broker_order_id
        order.rejection_reason = rejection_reason
        order.last_update_time = timestamp or pd.Timestamp.now(tz="UTC")
        self._journal(order, status)
        return order

    def has_open_order(self, symbol: str, side: str, entry_exit: str) -> bool:
        for order in self.orders.values():
            if order.symbol == symbol and order.side == side and order.entry_exit == entry_exit and order.status in {
                "new",
                "pending_reconcile",
                "accepted",
                "partially_filled",
            }:
                return True
        return False

    def replace_open_orders(self, open_orders: list[LiveOrder]) -> None:
        retained: dict[str, LiveOrder] = {}
        for order in open_orders:
            retained[order.client_order_id] = order
        for client_order_id, existing in list(self.orders.items()):
            if existing.status in {"filled", "canceled", "rejected", "pending_reconcile"}:
                retained.setdefault(client_order_id, existing)
        self.orders = retained

    def _journal(self, order: LiveOrder, event: str, notes: str = "") -> None:
        execution_cost = ""
        if order.avg_fill_price and order.filled_qty > 0:
            execution_cost = round(
                order.estimated_spread_cost + abs(order.avg_fill_price - order.intended_price) * order.filled_qty,
                6,
            )
        self.journal.log(
            {
                "timestamp": order.last_update_time.isoformat(),
                "symbol": order.symbol,
                "strategy": order.strategy,
                "side": order.side,
                "mode": self.mode,
                "event": event,
                "entry_exit": order.entry_exit,
                "stop": round(order.stop_price, 6),
                "size": round(order.intended_qty, 6),
                "signal_price": round(order.signal_price, 6),
                "intended_price": round(order.intended_price, 6),
                "fill_price": round(order.avg_fill_price, 6) if order.avg_fill_price else "",
                "slippage": round(order.avg_fill_price - order.intended_price, 6) if order.avg_fill_price else "",
                "spread_cost": round(order.estimated_spread_cost, 6),
                "execution_cost": execution_cost,
                "pnl": "",
                "r_multiple": "",
                "session_bucket": order.session_bucket,
                "client_order_id": order.client_order_id,
                "order_status": order.status,
                "filled_qty": round(order.filled_qty, 6),
                "remaining_qty": round(order.remaining_qty, 6),
                "rejection_reason": order.rejection_reason,
                "notes": notes,
            }
        )


@dataclass
class BrokerState:
    positions: dict[str, LivePosition] = field(default_factory=dict)
    uncertain: bool = True
    open_orders_by_symbol: dict[str, list[str]] = field(default_factory=dict)
    recovery_only_symbols: set[str] = field(default_factory=set)
    cooldowns: dict[str, pd.Timestamp] = field(default_factory=dict)
    reserved_notional_by_symbol: dict[str, float] = field(default_factory=dict)
    reserved_exit_qty_by_symbol: dict[str, float] = field(default_factory=dict)
    daily_realized_pnl: float = 0.0
    daily_unrealized_pnl: float = 0.0
    kill_switch_active: bool = False
    account_equity: float = 0.0
    account_cash: float = 0.0
    buying_power: float = 0.0


@dataclass
class StreamQuote:
    symbol: str
    bid: float
    ask: float
    mid: float
    spread_bps: float
    timestamp: pd.Timestamp


@dataclass
class BacktestPosition:
    symbol: str
    strategy: str
    side: str
    entry_time: pd.Timestamp
    entry_price: float
    signal_price: float
    stop_price: float
    initial_stop_price: float
    qty: float
    open_qty: float
    initial_risk_per_unit: float
    estimated_spread_cost: float
    estimated_entry_slippage: float
    partial_taken: bool = False
    partial_realized_pnl: float = 0.0
    exit_time: pd.Timestamp | None = None
    exit_price: float | None = None
    exit_reason: str = ""
    pnl: float = 0.0
    r_multiple: float = 0.0
    exit_spread_cost: float = 0.0
    exit_slippage: float = 0.0
    entry_count: int = 1


class BacktestEngine:
    def __init__(self, config: Config, journal: Journal):
        self.config = config
        self.journal = journal

    def run(self, strategy_name: str) -> dict[str, Any]:
        strategy = STRATEGIES[strategy_name]
        equity = self.config.starting_capital
        daily_loss = 0.0
        trades_today = 0
        current_day: date | None = None
        positions: list[BacktestPosition] = []
        trade_pnls: list[float] = []
        r_multiples: list[float] = []
        equity_points: list[dict[str, Any]] = []
        spread_costs: list[float] = []
        slippages: list[float] = []
        holding_hours: list[float] = []
        rejected_trade_count = 0
        session_bucket_counts: dict[str, int] = {}
        data_sources: dict[str, str] = {}
        data_source_errors: dict[str, str] = {}

        for symbol in self.config.symbols:
            data = DataLoader(self.config).load_historical_data(symbol)
            data_sources[symbol] = str(data.attrs.get("data_source", "unknown"))
            source_error = str(data.attrs.get("source_error", "")).strip()
            if source_error:
                data_source_errors[symbol] = source_error
            positions = []
            current_day = None
            daily_loss = 0.0
            trades_today = 0

            for i in range(1, len(data)):
                history = data.iloc[: i + 1]
                row = history.iloc[-1]
                day = row["timestamp"].date()
                if current_day != day:
                    current_day = day
                    daily_loss = 0.0
                    trades_today = 0

                next_positions: list[BacktestPosition] = []
                for position in positions:
                    position_strategy = STRATEGIES[position.strategy]
                    custom_exit = position_strategy.exit_signal(position, history, self.config)
                    if custom_exit is not None:
                        reason, exit_price = custom_exit
                        closed = self._close_position(position, row["timestamp"], float(exit_price), reason)
                    elif position_strategy.name == PremarketRegressionStrategy.name:
                        closed = None
                    elif position_strategy is strategy:
                        closed = self._manage_open_position(position, row)
                    else:
                        closed = None
                    if closed is not None:
                        equity += closed.pnl
                        daily_loss += min(closed.pnl, 0.0)
                        trade_pnls.append(closed.pnl)
                        r_multiples.append(closed.r_multiple)
                        spread_costs.append(closed.estimated_spread_cost + closed.exit_spread_cost)
                        slippages.append(closed.estimated_entry_slippage + closed.exit_slippage)
                        if closed.exit_time is not None:
                            holding_hours.append((closed.exit_time - closed.entry_time).total_seconds() / 3600.0)
                        self._journal_trade(closed)
                    else:
                        next_positions.append(position)
                positions = next_positions

                equity_points.append({"timestamp": row["timestamp"], "equity": equity})
                if trades_today >= self.config.max_trades_per_day:
                    continue
                if abs(daily_loss) >= equity * self.config.max_daily_loss:
                    continue
                symbol_positions = [position for position in positions if position.symbol == symbol and position.strategy == strategy_name]
                if len(symbol_positions) >= strategy.max_concurrent_entries(self.config):
                    continue

                signal = strategy.generate_signal(symbol, history, self.config, self.config.spread_bps)
                if signal is None:
                    continue

                reserved_capital = sum(position.open_qty * position.entry_price for position in positions)
                available_capital = max(equity - reserved_capital, 0.0)
                custom_qty = strategy.position_size(available_capital, signal, self.config)
                qty_basis = custom_qty if custom_qty is not None else position_size_from_risk(
                    available_capital,
                    signal.entry,
                    signal.stop,
                    self.config.risk_per_trade,
                )
                qty = normalize_qty(self.config.asset_class, qty_basis)
                if qty <= 0:
                    rejected_trade_count += 1
                    continue

                spread_half = signal.entry * (self.config.spread_bps / 20_000.0)
                slip = signal.entry * (self.config.slippage_bps / 10_000.0)
                if signal.side == "buy":
                    entry_fill = signal.entry + spread_half + slip
                else:
                    entry_fill = signal.entry - spread_half - slip
                spread_cost, entry_slippage = estimate_costs(signal.entry, qty, self.config.spread_bps, self.config.slippage_bps)
                positions.append(
                    BacktestPosition(
                    symbol=symbol,
                    strategy=strategy_name,
                    side=signal.side,
                    entry_time=row["timestamp"],
                    entry_price=entry_fill,
                    signal_price=signal.entry,
                    stop_price=signal.stop,
                    initial_stop_price=signal.stop,
                    qty=qty,
                    open_qty=qty,
                    initial_risk_per_unit=abs(entry_fill - signal.stop),
                    estimated_spread_cost=spread_cost,
                    estimated_entry_slippage=entry_slippage,
                    )
                )
                trades_today += 1
                bucket = session_bucket(row["timestamp"], self.config)
                session_bucket_counts[bucket] = session_bucket_counts.get(bucket, 0) + 1

            for position in positions:
                last_row = data.iloc[-1]
                closed = self._close_position(position, last_row["timestamp"], float(last_row["close"]), "end_of_data")
                equity += closed.pnl
                trade_pnls.append(closed.pnl)
                r_multiples.append(closed.r_multiple)
                spread_costs.append(closed.estimated_spread_cost + closed.exit_spread_cost)
                slippages.append(closed.estimated_entry_slippage + closed.exit_slippage)
                if closed.exit_time is not None:
                    holding_hours.append((closed.exit_time - closed.entry_time).total_seconds() / 3600.0)
                self._journal_trade(closed)

        equity_curve = pd.DataFrame(equity_points)
        equity_series = equity_curve["equity"] if not equity_curve.empty else pd.Series([self.config.starting_capital], dtype="float64")
        total_return = (equity / self.config.starting_capital) - 1.0
        wins = sum(1 for pnl in trade_pnls if pnl > 0)
        win_rate = wins / len(trade_pnls) if trade_pnls else 0.0
        positive_r = [value for value in r_multiples if value > 0]
        negative_r = [abs(value) for value in r_multiples if value < 0]
        realized_reward_to_risk = (
            round((sum(positive_r) / len(positive_r)) / (sum(negative_r) / len(negative_r)), 2)
            if positive_r and negative_r and sum(negative_r) > 0
            else 0.0
        )
        return {
            "strategy": strategy_name,
            "ending_equity": round(equity, 2),
            "total_return": round(total_return * 100, 2),
            "win_rate": round(win_rate * 100, 2),
            "average_r": round(sum(r_multiples) / len(r_multiples), 2) if r_multiples else 0.0,
            "max_drawdown": round(max_drawdown(equity_series) * 100, 2),
            "trade_count": len(trade_pnls),
            "avg_slippage": round(sum(slippages) / len(slippages), 4) if slippages else 0.0,
            "avg_spread_cost": round(sum(spread_costs) / len(spread_costs), 4) if spread_costs else 0.0,
            "avg_holding_time_hours": round(sum(holding_hours) / len(holding_hours), 2) if holding_hours else 0.0,
            "rejected_trade_count": rejected_trade_count,
            "realized_reward_to_risk": realized_reward_to_risk,
            "data_sources": json.dumps(data_sources, sort_keys=True),
            "data_source_errors": json.dumps(data_source_errors, sort_keys=True),
            "trades_by_session_bucket": json.dumps(session_bucket_counts, sort_keys=True),
        }

    def _manage_open_position(self, position: BacktestPosition, row: pd.Series) -> BacktestPosition | None:
        target_1 = (
            position.entry_price + position.initial_risk_per_unit * self.config.partial_take_profit_r
            if position.side == "buy"
            else position.entry_price - position.initial_risk_per_unit * self.config.partial_take_profit_r
        )
        target_2 = (
            position.entry_price + position.initial_risk_per_unit * self.config.final_take_profit_r
            if position.side == "buy"
            else position.entry_price - position.initial_risk_per_unit * self.config.final_take_profit_r
        )

        if position.side == "buy":
            if row["low"] <= position.stop_price:
                return self._close_position(position, row["timestamp"], float(position.stop_price), "stop_loss")
            if not position.partial_taken and row["high"] >= target_1:
                self._take_partial(position, target_1)
            if row["high"] >= target_2 or row["close"] < row["ema_fast"]:
                return self._close_position(position, row["timestamp"], float(min(target_2, row["close"])), "target_or_structure")
        else:
            if row["high"] >= position.stop_price:
                return self._close_position(position, row["timestamp"], float(position.stop_price), "stop_loss")
            if not position.partial_taken and row["low"] <= target_1:
                self._take_partial(position, target_1)
            if row["low"] <= target_2 or row["close"] > row["ema_fast"]:
                return self._close_position(position, row["timestamp"], float(max(target_2, row["close"])), "target_or_structure")
        return None

    def _take_partial(self, position: BacktestPosition, target_price: float) -> None:
        partial_qty = normalize_qty(self.config.asset_class, position.open_qty * 0.5)
        if partial_qty <= 0:
            return
        direction = 1 if position.side == "buy" else -1
        position.partial_realized_pnl += (target_price - position.entry_price) * partial_qty * direction
        position.open_qty = normalize_qty(self.config.asset_class, position.open_qty - partial_qty)
        position.partial_taken = True
        if position.side == "buy":
            position.stop_price = max(position.stop_price, position.entry_price)
        else:
            position.stop_price = min(position.stop_price, position.entry_price)

    def _close_position(
        self,
        position: BacktestPosition,
        exit_time: pd.Timestamp,
        exit_price: float,
        reason: str,
    ) -> BacktestPosition:
        spread_half = exit_price * (self.config.spread_bps / 20_000.0)
        slip = exit_price * (self.config.slippage_bps / 10_000.0)
        if position.side == "buy":
            fill = exit_price - spread_half - slip
        else:
            fill = exit_price + spread_half + slip
        direction = 1 if position.side == "buy" else -1
        gross_pnl = (fill - position.entry_price) * position.open_qty * direction
        exit_spread_cost, exit_slippage = estimate_costs(exit_price, position.open_qty, self.config.spread_bps, self.config.slippage_bps)
        pnl = position.partial_realized_pnl + gross_pnl - self.config.fee_per_trade
        basis_risk = position.initial_risk_per_unit * position.qty
        position.exit_time = exit_time
        position.exit_price = fill
        position.exit_reason = reason
        position.pnl = round(pnl, 2)
        position.r_multiple = round((pnl / basis_risk) if basis_risk > 0 else 0.0, 2)
        position.exit_spread_cost = exit_spread_cost
        position.exit_slippage = exit_slippage
        return position

    def _journal_trade(self, position: BacktestPosition) -> None:
        self.journal.log(
            {
                "timestamp": position.exit_time.isoformat() if position.exit_time is not None else "",
                "symbol": position.symbol,
                "strategy": position.strategy,
                "side": position.side,
                "mode": "backtest",
                "event": position.exit_reason,
                "entry_exit": "round_trip",
                "stop": round(position.initial_stop_price, 6),
                "size": round(position.qty, 6),
                "signal_price": round(position.signal_price, 6),
                "intended_price": round(position.signal_price, 6),
                "fill_price": round(position.exit_price or 0.0, 6),
                "slippage": round(position.estimated_entry_slippage + position.exit_slippage, 6),
                "spread_cost": round(position.estimated_spread_cost + position.exit_spread_cost, 6),
                "execution_cost": round(
                    position.estimated_entry_slippage
                    + position.exit_slippage
                    + position.estimated_spread_cost
                    + position.exit_spread_cost,
                    6,
                ),
                "pnl": position.pnl,
                "r_multiple": position.r_multiple,
                "session_bucket": session_bucket(position.entry_time, self.config),
                "filled_qty": round(position.qty, 6),
                "remaining_qty": 0.0,
                "notes": f"entry={round(position.entry_price, 6)}",
            }
        )


def compare_strategies(config: Config, strategy_names: list[str]) -> pd.DataFrame:
    results: list[dict[str, Any]] = []
    for strategy_name in strategy_names:
        isolated = replace(config, strategy=strategy_name)
        journal = Journal(f"{Path(config.journal_path).stem}_{strategy_name}.csv")
        engine = BacktestEngine(isolated, journal)
        results.append(engine.run(strategy_name))
    frame = pd.DataFrame(results)
    if not frame.empty:
        frame = frame.sort_values(["total_return", "win_rate", "average_r"], ascending=[False, False, False]).reset_index(drop=True)
        frame.to_csv(config.compare_output_path, index=False)
    return frame


def print_backtest_summary(result: dict[str, Any]) -> None:
    print(f"Strategy: {result['strategy']}")
    print(f"Data sources: {result.get('data_sources', '{}')}")
    data_source_errors = result.get("data_source_errors")
    if data_source_errors and data_source_errors != "{}":
        print(f"Data source errors: {data_source_errors}")
    print(f"Ending equity: ${result['ending_equity']:.2f}")
    print(f"Total return: {result['total_return']:.2f}%")
    print(f"Win rate: {result['win_rate']:.2f}%")
    print(f"Average R: {result['average_r']:.2f}")
    print(f"Max drawdown: {result['max_drawdown']:.2f}%")
    print(f"Trade count: {result['trade_count']}")
    print(f"Avg slippage: {result['avg_slippage']:.4f}")
    print(f"Avg spread cost: {result['avg_spread_cost']:.4f}")
    print(f"Avg holding time (hours): {result['avg_holding_time_hours']:.2f}")
    print(f"Rejected trades: {result['rejected_trade_count']}")
    print(f"Realized reward:risk: {result['realized_reward_to_risk']:.2f}")
    print(f"Trades by session bucket: {result['trades_by_session_bucket']}")


def print_strategy_comparison(frame: pd.DataFrame) -> None:
    if frame.empty:
        print("No backtest results available.")
        return
    print(
        frame[
            [
                "strategy",
                "total_return",
                "win_rate",
                "average_r",
                "max_drawdown",
                "trade_count",
                "avg_slippage",
                "avg_spread_cost",
                "avg_holding_time_hours",
                "rejected_trade_count",
                "realized_reward_to_risk",
                "data_sources",
                "trades_by_session_bucket",
            ]
        ].to_string(index=False)
    )


class BaseExecutor(ABC):
    def __init__(self, config: Config):
        self.config = config

    def _headers(self) -> dict[str, str]:
        return {
            "APCA-API-KEY-ID": self.config.alpaca_api_key,
            "APCA-API-SECRET-KEY": self.config.alpaca_secret_key,
            "Content-Type": "application/json",
        }

    @abstractmethod
    def validate_order(self, side: str, qty: float, order_type: str, time_in_force: str) -> None:
        raise NotImplementedError

    @abstractmethod
    def build_order_payload(
        self,
        *,
        symbol: str,
        side: str,
        qty: float,
        client_order_id: str,
        order_type: str,
        time_in_force: str,
    ) -> dict[str, Any]:
        raise NotImplementedError

    @abstractmethod
    def submit_entry_order(self, signal: Signal, qty: float, client_order_id: str) -> dict[str, Any]:
        raise NotImplementedError

    @abstractmethod
    def submit_exit_order(self, position: LivePosition, qty: float, client_order_id: str, reason: str) -> dict[str, Any]:
        raise NotImplementedError


class EquitiesExecutor(BaseExecutor):
    def validate_order(self, side: str, qty: float, order_type: str, time_in_force: str) -> None:
        if side not in {"buy", "sell"}:
            raise ValueError(f"Unsupported equity side: {side}")
        if qty <= 0:
            raise ValueError("Equity quantity must be positive.")
        if order_type not in {"market"}:
            raise ValueError(f"Unsupported equity order type: {order_type}")
        if time_in_force.lower() != "day":
            raise ValueError("Equity orders require DAY time_in_force in this bot.")
        if round(qty, 4) != qty:
            raise ValueError("Equity quantity exceeds supported precision.")
        if not self.config.allow_fractional_equities and not float(qty).is_integer():
            raise ValueError("Fractional equity orders are disabled by config.")

    def build_order_payload(
        self,
        *,
        symbol: str,
        side: str,
        qty: float,
        client_order_id: str,
        order_type: str,
        time_in_force: str,
    ) -> dict[str, Any]:
        self.validate_order(side, qty, order_type, time_in_force)
        return {
            "symbol": symbol,
            "side": side,
            "type": order_type,
            "time_in_force": time_in_force.lower(),
            "qty": round(qty, 4),
            "client_order_id": client_order_id,
        }

    def submit_entry_order(self, signal: Signal, qty: float, client_order_id: str) -> dict[str, Any]:
        payload = self.build_order_payload(
            symbol=signal.symbol,
            side=signal.side,
            qty=qty,
            client_order_id=client_order_id,
            order_type="market",
            time_in_force="day",
        )
        response = requests.post(
            f"{self.config.alpaca_base_url}/v2/orders",
            headers=self._headers(),
            json=payload,
            timeout=15,
        )
        response.raise_for_status()
        return response.json()

    def submit_exit_order(self, position: LivePosition, qty: float, client_order_id: str, reason: str) -> dict[str, Any]:
        close_side = "sell" if position.side == "buy" else "buy"
        payload = self.build_order_payload(
            symbol=position.symbol,
            side=close_side,
            qty=qty,
            client_order_id=client_order_id,
            order_type="market",
            time_in_force="day",
        )
        response = requests.post(
            f"{self.config.alpaca_base_url}/v2/orders",
            headers=self._headers(),
            json=payload,
            timeout=15,
        )
        response.raise_for_status()
        return response.json()


class CryptoExecutor(BaseExecutor):
    def validate_order(self, side: str, qty: float, order_type: str, time_in_force: str) -> None:
        if side not in {"buy", "sell"}:
            raise ValueError(f"Unsupported crypto side: {side}")
        if qty <= 0:
            raise ValueError("Crypto quantity must be positive.")
        if order_type not in {"market"}:
            raise ValueError(f"Unsupported crypto order type: {order_type}")
        if time_in_force.lower() not in {"gtc", "ioc"}:
            raise ValueError("Crypto orders in this bot support GTC or IOC only.")
        if round(qty, 6) != qty:
            raise ValueError("Crypto quantity exceeds supported precision.")

    def build_order_payload(
        self,
        *,
        symbol: str,
        side: str,
        qty: float,
        client_order_id: str,
        order_type: str,
        time_in_force: str,
    ) -> dict[str, Any]:
        self.validate_order(side, qty, order_type, time_in_force)
        return {
            "symbol": symbol,
            "side": side,
            "type": order_type,
            "time_in_force": time_in_force.lower(),
            "qty": round(qty, 6),
            "client_order_id": client_order_id,
        }

    def submit_entry_order(self, signal: Signal, qty: float, client_order_id: str) -> dict[str, Any]:
        payload = self.build_order_payload(
            symbol=signal.symbol,
            side=signal.side,
            qty=qty,
            client_order_id=client_order_id,
            order_type="market",
            time_in_force="gtc",
        )
        response = requests.post(
            f"{self.config.alpaca_base_url}/v2/orders",
            headers=self._headers(),
            json=payload,
            timeout=15,
        )
        response.raise_for_status()
        return response.json()

    def submit_exit_order(self, position: LivePosition, qty: float, client_order_id: str, reason: str) -> dict[str, Any]:
        close_side = "sell" if position.side == "buy" else "buy"
        payload = self.build_order_payload(
            symbol=position.symbol,
            side=close_side,
            qty=qty,
            client_order_id=client_order_id,
            order_type="market",
            time_in_force="gtc",
        )
        response = requests.post(
            f"{self.config.alpaca_base_url}/v2/orders",
            headers=self._headers(),
            json=payload,
            timeout=15,
        )
        response.raise_for_status()
        return response.json()


class StreamBarAggregator:
    def __init__(self, config: Config):
        self.config = config
        self.current: dict[str, dict[str, Any]] = {}
        self.last_closed_bucket: dict[str, pd.Timestamp] = {}
        self.ignored_late_messages = 0

    def update(self, symbol: str, ts: pd.Timestamp, open_price: float, high: float, low: float, close: float, volume: float) -> pd.DataFrame | None:
        # Live signals fire only on fully closed aggregated bars. Messages for a
        # previously closed bucket are ignored to keep bar semantics deterministic.
        bucket = floor_timestamp(ts, self.config.timeframe)
        last_closed = self.last_closed_bucket.get(symbol)
        if last_closed is not None and bucket <= last_closed:
            self.ignored_late_messages += 1
            return None
        active = self.current.get(symbol)
        if active is None:
            self.current[symbol] = {
                "timestamp": bucket,
                "open": open_price,
                "high": high,
                "low": low,
                "close": close,
                "volume": volume,
            }
            return None
        if active["timestamp"] != bucket:
            closed = pd.DataFrame([active])
            self.last_closed_bucket[symbol] = active["timestamp"]
            self.current[symbol] = {
                "timestamp": bucket,
                "open": open_price,
                "high": high,
                "low": low,
                "close": close,
                "volume": volume,
            }
            return closed
        active["high"] = max(active["high"], high)
        active["low"] = min(active["low"], low)
        active["close"] = close
        active["volume"] += volume
        return None


class StreamExecutionEngine:
    def __init__(self, config: Config, journal: Journal):
        self.config = config
        self.journal = journal
        self.loader = DataLoader(config)
        self.strategy = STRATEGIES[config.strategy]
        self.state_machine = OrderStateMachine(journal, config.mode)
        self.broker_state = BrokerState()
        self.aggregator = StreamBarAggregator(config)
        self.history: dict[str, pd.DataFrame] = {}
        self.executor: BaseExecutor = EquitiesExecutor(config) if config.asset_class == "equity" else CryptoExecutor(config)
        self.trading_client: TradingClient | None = None
        self.state_path = Path(self.config.state_path)
        self.last_market_data_time: pd.Timestamp | None = None
        self.last_trade_update_time: pd.Timestamp | None = None
        self.stream_started_at: pd.Timestamp | None = None
        self.current_session_day = utc_now().date()
        self.quote_cache: dict[str, StreamQuote] = {}
        self._load_local_state()

    async def run(self) -> None:
        if not ALPACA_AVAILABLE:
            raise RuntimeError("alpaca-py is required for paper/live WebSocket execution.")
        self._validate_live_config()
        print(PLAN_TEXT.strip())
        self.trading_client = TradingClient(
            self.config.alpaca_api_key,
            self.config.alpaca_secret_key,
            paper=execution_mode(self.config.mode) == "paper",
        )
        for symbol in self.config.symbols:
            self.history[symbol] = self.loader.load_historical_data(symbol)
        self._safe_reconcile()
        backoff = self.config.reconnect_backoff_seconds
        while True:
            try:
                await self._run_streams()
                backoff = self.config.reconnect_backoff_seconds
            except asyncio.CancelledError:
                self.broker_state.uncertain = True
                self._safe_reconcile()
                raise
            except Exception as exc:
                self.broker_state.uncertain = True
                self._safe_reconcile()
                wait_seconds = min(backoff, self.config.reconnect_backoff_max_seconds) + random.uniform(0.0, self.config.reconnect_jitter_seconds)
                print(f"Stream reconnect required: {exc}. Retrying in {wait_seconds:.1f}s")
                await asyncio.sleep(wait_seconds)
                backoff = min(backoff * 2.0, self.config.reconnect_backoff_max_seconds)

    async def _run_streams(self) -> None:
        websocket_params = {
            "ping_interval": self.config.websocket_ping_interval_seconds,
            "ping_timeout": self.config.websocket_ping_timeout_seconds,
        }
        if self.config.asset_class == "equity":
            data_stream = StockDataStream(
                self.config.alpaca_api_key,
                self.config.alpaca_secret_key,
                feed=self.loader._stock_feed(),
                websocket_params=websocket_params,
            )
        else:
            data_stream = CryptoDataStream(
                self.config.alpaca_api_key,
                self.config.alpaca_secret_key,
                feed=self.loader._crypto_feed(),
                websocket_params=websocket_params,
            )
        trading_stream = TradingStream(
            self.config.alpaca_api_key,
            self.config.alpaca_secret_key,
            paper=execution_mode(self.config.mode) == "paper",
            websocket_params=websocket_params,
        )

        async def on_bar(bar: Any) -> None:
            self.last_market_data_time = utc_now()
            closed = self.aggregator.update(
                getattr(bar, "symbol"),
                utc_timestamp(getattr(bar, "timestamp")),
                float(getattr(bar, "open")),
                float(getattr(bar, "high")),
                float(getattr(bar, "low")),
                float(getattr(bar, "close")),
                float(getattr(bar, "volume")),
            )
            if closed is not None:
                await self._on_closed_bar(getattr(bar, "symbol"), closed.iloc[0])

        async def on_trade_update(update: Any) -> None:
            self.last_trade_update_time = utc_now()
            await self._on_trade_update(update)

        async def on_quote(quote: Any) -> None:
            timestamp = utc_timestamp(getattr(quote, "timestamp"))
            bid = float(getattr(quote, "bid_price", 0.0) or 0.0)
            ask = float(getattr(quote, "ask_price", 0.0) or 0.0)
            mid = (bid + ask) / 2.0 if bid > 0 and ask > 0 else max(bid, ask)
            spread_bps = ((ask - bid) / mid) * 10_000.0 if mid > 0 and ask >= bid else 0.0
            self.quote_cache[getattr(quote, "symbol")] = StreamQuote(
                symbol=getattr(quote, "symbol"),
                bid=bid,
                ask=ask,
                mid=mid,
                spread_bps=spread_bps,
                timestamp=timestamp,
            )
            self.last_market_data_time = utc_now()

        for symbol in self.config.symbols:
            data_stream.subscribe_bars(on_bar, symbol)
            data_stream.subscribe_quotes(on_quote, symbol)
        trading_stream.subscribe_trade_updates(on_trade_update)

        self.broker_state.uncertain = False
        self.stream_started_at = utc_now()
        self.last_market_data_time = utc_now()
        self.last_trade_update_time = utc_now()

        data_task = asyncio.create_task(data_stream._run_forever())
        trading_task = asyncio.create_task(trading_stream._run_forever())
        watcher_task = asyncio.create_task(self._watch_stream_health())

        try:
            done, pending = await asyncio.wait(
                {data_task, trading_task, watcher_task},
                return_when=asyncio.FIRST_EXCEPTION,
            )
            for task in done:
                exc = task.exception()
                if exc is not None:
                    raise exc
            raise RuntimeError("Stream task exited unexpectedly.")
        finally:
            for task in (watcher_task, data_task, trading_task):
                task.cancel()
            for stream in (data_stream, trading_stream):
                for method_name in ("stop_ws", "stop", "close"):
                    method = getattr(stream, method_name, None)
                    if callable(method):
                        result = method()
                        if asyncio.iscoroutine(result):
                            await result
                        break
            await asyncio.gather(data_task, trading_task, watcher_task, return_exceptions=True)

    async def _on_closed_bar(self, symbol: str, bar_row: pd.Series) -> None:
        self._roll_session_day_if_needed(utc_timestamp(bar_row["timestamp"]))
        new_bar = pd.DataFrame([{k: bar_row[k] for k in ["timestamp", "open", "high", "low", "close", "volume"]}])
        new_bar["symbol"] = symbol
        updated = pd.concat([self.history[symbol][["timestamp", "open", "high", "low", "close", "volume", "symbol"]], new_bar], ignore_index=True)
        updated = updated.drop_duplicates("timestamp", keep="last").sort_values("timestamp").reset_index(drop=True)
        self.history[symbol] = add_indicators(updated, self.config)
        await self._manage_position(symbol)
        signal = self.strategy.generate_signal(symbol, self.history[symbol], self.config, self.config.spread_bps)
        if signal is None:
            return
        if self.broker_state.uncertain:
            self.reconcile_state()
        if not self._can_trade_symbol(symbol):
            return
        if symbol not in self.broker_state.positions and len(self.broker_state.positions) >= self.config.max_open_positions:
            return
        if self._active_entry_layers(symbol, signal.strategy) >= self.strategy.max_concurrent_entries(self.config):
            return
        if self.state_machine.has_open_order(symbol, signal.side, "entry"):
            return
        quote = self._stream_quote_snapshot(symbol)
        if quote is None:
            return
        price_reference = float(quote["mid"] or signal.entry)
        capital_base = self._capital_base()
        custom_qty = self.strategy.position_size(capital_base, signal, self.config)
        qty_basis = custom_qty if custom_qty is not None else position_size_from_risk(
            capital_base,
            price_reference,
            signal.stop,
            self.config.risk_per_trade,
        )
        qty = normalize_qty(self.config.asset_class, qty_basis)
        if qty <= 0:
            return
        if not self._entry_risk_checks(symbol, price_reference, qty, quote, signal.strategy):
            return
        client_order_id = deterministic_client_order_id(execution_mode(self.config.mode), signal.strategy, symbol, signal.side, "entry", signal.bar_time)
        if client_order_id in self.state_machine.orders and self.state_machine.orders[client_order_id].status in {"new", "accepted", "partially_filled", "filled"}:
            return
        order = LiveOrder(
            client_order_id=client_order_id,
            symbol=symbol,
            strategy=signal.strategy,
            side=signal.side,
            intended_qty=qty,
            intended_price=price_reference,
            signal_price=signal.entry,
            stop_price=signal.stop,
            entry_exit="entry",
            expected_spread_bps=float(quote["spread_bps"]),
            estimated_spread_cost=estimate_costs(price_reference, qty, float(quote["spread_bps"]), 0.0)[0],
            session_bucket=session_bucket(signal.bar_time, self.config),
            bar_time=signal.bar_time,
        )
        self.state_machine.register_intent(order)
        self._persist_local_state()
        await self._submit_order_with_retry(
            order=order,
            submitter=lambda: self.executor.submit_entry_order(signal, qty, client_order_id),
        )

    async def flatten_for_session_close(self, reason: str = "scheduled_close") -> None:
        self.broker_state.kill_switch_active = True
        self._safe_reconcile()
        self._cancel_open_orders()
        self._safe_reconcile()
        for symbol, position in list(self.broker_state.positions.items()):
            if self.state_machine.has_open_order(symbol, "sell" if position.side == "buy" else "buy", "exit"):
                continue
            bar_time = utc_now()
            quote = self.quote_cache.get(symbol)
            if quote is None:
                snapshot = self.loader.latest_quote_snapshot(symbol)
                quote = StreamQuote(
                    symbol=symbol,
                    bid=float(snapshot.get("bid") or 0.0),
                    ask=float(snapshot.get("ask") or 0.0),
                    mid=float(snapshot.get("mid") or 0.0),
                    spread_bps=float(snapshot.get("spread_bps") or 0.0),
                    timestamp=snapshot.get("timestamp") or utc_now(),
                )
                self.quote_cache[symbol] = quote
            await self._submit_exit(symbol, position, position.qty, reason, bar_time)

    def _cancel_open_orders(self) -> None:
        self._safe_reconcile()
        for order in list(self.state_machine.orders.values()):
            if order.status not in {"new", "pending_reconcile", "accepted", "partially_filled"}:
                continue
            if not order.broker_order_id:
                continue
            try:
                response = requests.delete(
                    f"{self.config.alpaca_base_url}/v2/orders/{order.broker_order_id}",
                    headers=self.loader._alpaca_headers(),
                    timeout=15,
                )
                if response.status_code not in {204, 404}:
                    response.raise_for_status()
            except Exception as exc:
                print(f"Order cancel failed for {order.client_order_id}: {exc}")

    async def _manage_position(self, symbol: str) -> None:
        position = self.broker_state.positions.get(symbol)
        if position is None:
            return
        if position.recovery_only or position.stop_price <= 0 or position.initial_risk_per_unit <= 0:
            self.broker_state.recovery_only_symbols.add(symbol)
            return
        history = self.history[symbol]
        row = history.iloc[-1]
        position_strategy = STRATEGIES.get(position.strategy, self.strategy)
        custom_exit = position_strategy.exit_signal(position, history, self.config)
        if custom_exit is not None:
            reason, _ = custom_exit
            await self._submit_exit(symbol, position, position.qty, reason, row["timestamp"])
            return
        if position_strategy.name == PremarketRegressionStrategy.name:
            return
        if position.side == "buy":
            partial_target = position.avg_entry_price + position.initial_risk_per_unit * self.config.partial_take_profit_r
            final_target = position.avg_entry_price + position.initial_risk_per_unit * self.config.final_take_profit_r
            if not position.partial_taken and row["high"] >= partial_target:
                await self._submit_exit(symbol, position, normalize_qty(self.config.asset_class, position.qty * 0.5), "partial_target", row["timestamp"])
            elif row["low"] <= position.stop_price:
                await self._submit_exit(symbol, position, position.qty, "stop_loss", row["timestamp"])
            elif row["high"] >= final_target or row["close"] < row["ema_fast"]:
                await self._submit_exit(symbol, position, position.qty, "target_or_structure", row["timestamp"])
        else:
            partial_target = position.avg_entry_price - position.initial_risk_per_unit * self.config.partial_take_profit_r
            final_target = position.avg_entry_price - position.initial_risk_per_unit * self.config.final_take_profit_r
            if not position.partial_taken and row["low"] <= partial_target:
                await self._submit_exit(symbol, position, normalize_qty(self.config.asset_class, position.qty * 0.5), "partial_target", row["timestamp"])
            elif row["high"] >= position.stop_price:
                await self._submit_exit(symbol, position, position.qty, "stop_loss", row["timestamp"])
            elif row["low"] <= final_target or row["close"] > row["ema_fast"]:
                await self._submit_exit(symbol, position, position.qty, "target_or_structure", row["timestamp"])

    async def _submit_exit(self, symbol: str, position: LivePosition, qty: float, reason: str, bar_time: pd.Timestamp) -> None:
        if qty <= 0:
            return
        exit_side = "sell" if position.side == "buy" else "buy"
        available_qty = self._available_exit_qty(symbol)
        qty = min(qty, available_qty)
        qty = normalize_qty(self.config.asset_class, qty)
        if qty <= 0 or self.state_machine.has_open_order(symbol, exit_side, "exit"):
            return
        if self.broker_state.uncertain:
            self.reconcile_state()
        client_order_id = deterministic_client_order_id(
            execution_mode(self.config.mode),
            position.strategy,
            symbol,
            exit_side,
            f"exit_{reason}",
            bar_time,
        )
        if client_order_id in self.state_machine.orders and self.state_machine.orders[client_order_id].status in {"new", "accepted", "partially_filled", "filled"}:
            return
        quote = self._stream_quote_snapshot(symbol)
        if quote is None:
            return
        intended_price = float(quote["mid"] or position.avg_entry_price)
        order = LiveOrder(
            client_order_id=client_order_id,
            symbol=symbol,
            strategy=position.strategy,
            side=exit_side,
            intended_qty=qty,
            intended_price=float(intended_price),
            signal_price=float(intended_price),
            stop_price=position.stop_price,
            entry_exit="exit",
            expected_spread_bps=float(quote["spread_bps"]),
            estimated_spread_cost=estimate_costs(intended_price, qty, float(quote["spread_bps"]), 0.0)[0],
            session_bucket=session_bucket(bar_time, self.config),
            bar_time=bar_time,
        )
        self.state_machine.register_intent(order)
        self._persist_local_state()
        await self._submit_order_with_retry(
            order=order,
            submitter=lambda: self.executor.submit_exit_order(position, qty, client_order_id, reason),
        )

    async def _on_trade_update(self, update: Any) -> None:
        event = _enum_value(getattr(update, "event", "") or getattr(update, "event_type", ""))
        order = getattr(update, "order", None)
        if order is None:
            return
        client_order_id = str(getattr(order, "client_order_id", "") or "")
        if not client_order_id or client_order_id not in self.state_machine.orders:
            return
        broker_order_id = str(getattr(order, "id", "") or "")
        filled_qty = float(_enum_value(getattr(order, "filled_qty", 0.0)) or 0.0)
        avg_fill_price = float(_enum_value(getattr(order, "filled_avg_price", 0.0)) or 0.0)
        status_map = {
            "new": "new",
            "accepted": "accepted",
            "partially_filled": "partially_filled",
            "fill": "filled",
            "filled": "filled",
            "canceled": "canceled",
            "rejected": "rejected",
        }
        status = status_map.get(str(event).lower(), _enum_value(getattr(order, "status", "accepted")).lower())
        tracked = self.state_machine.apply_update(
            client_order_id=client_order_id,
            status=status,
            filled_qty=filled_qty,
            avg_fill_price=avg_fill_price,
            broker_order_id=broker_order_id,
            rejection_reason=str(getattr(order, "reject_reason", "") or ""),
            timestamp=pd.Timestamp.now(tz="UTC"),
        )
        self._rebuild_position_from_fills(tracked)
        if tracked.status == "rejected":
            self._set_symbol_cooldown(tracked.symbol, self.config.cooldown_minutes_after_rejection)
        self._persist_local_state()

    def _rebuild_position_from_fills(self, order: LiveOrder) -> None:
        fill_delta = order.last_fill_delta
        if fill_delta <= 0:
            return
        if order.entry_exit == "entry":
            first_fill_for_order = order.last_processed_fill_qty <= 0
            side = "buy" if order.side == "buy" else "sell"
            existing = self.broker_state.positions.get(order.symbol)
            if existing is None:
                self.broker_state.positions[order.symbol] = LivePosition(
                    symbol=order.symbol,
                    strategy=order.strategy,
                    side=side,
                    qty=fill_delta,
                    avg_entry_price=order.avg_fill_price,
                    stop_price=order.stop_price,
                    initial_risk_per_unit=abs(order.avg_fill_price - order.stop_price),
                    entry_time=order.last_update_time,
                    entry_count=1,
                    last_fill_time=order.last_update_time,
                )
            else:
                total_qty = normalize_qty(self.config.asset_class, existing.qty + fill_delta)
                if total_qty > 0:
                    existing.avg_entry_price = (
                        (existing.avg_entry_price * existing.qty) + (order.avg_fill_price * fill_delta)
                    ) / total_qty
                    if order.stop_price > 0:
                        existing.stop_price = (
                            (existing.stop_price * existing.qty) + (order.stop_price * fill_delta)
                        ) / total_qty
                existing.qty = total_qty
                existing.initial_risk_per_unit = abs(existing.avg_entry_price - existing.stop_price) if existing.stop_price > 0 else existing.initial_risk_per_unit
                existing.last_fill_time = order.last_update_time
                existing.strategy = order.strategy
                if first_fill_for_order:
                    existing.entry_count += 1
        else:
            position = self.broker_state.positions.get(order.symbol)
            if position is None:
                return
            direction = 1 if position.side == "buy" else -1
            pnl_delta = (order.avg_fill_price - position.avg_entry_price) * fill_delta * direction
            position.realized_pnl += pnl_delta
            self.broker_state.daily_realized_pnl += pnl_delta
            remaining = normalize_qty(self.config.asset_class, position.qty - fill_delta)
            if remaining <= 0:
                del self.broker_state.positions[order.symbol]
            else:
                position.qty = remaining
                position.last_fill_time = order.last_update_time
                if "partial" in order.client_order_id:
                    position.partial_taken = True
                if "stoploss" in order.client_order_id.replace("_", "").lower():
                    self._set_symbol_cooldown(order.symbol, self.config.cooldown_minutes_after_stop)

        if order.avg_fill_price and order.intended_price > 0:
            deviation_bps = abs(order.avg_fill_price - order.intended_price) / order.intended_price * 10_000.0
            if deviation_bps > self.config.max_slippage_deviation_bps:
                self.broker_state.recovery_only_symbols.add(order.symbol)
        order.last_processed_fill_qty = order.filled_qty

    def reconcile_state(self) -> None:
        if not (self.config.alpaca_api_key and self.config.alpaca_secret_key):
            raise ValueError("Alpaca credentials are required for paper/live execution.")
        open_orders_response = requests.get(
            f"{self.config.alpaca_base_url}/v2/orders",
            headers={
                "APCA-API-KEY-ID": self.config.alpaca_api_key,
                "APCA-API-SECRET-KEY": self.config.alpaca_secret_key,
            },
            params={"status": "open", "nested": "false", "limit": 500},
            timeout=15,
        )
        open_orders_response.raise_for_status()
        positions_response = requests.get(
            f"{self.config.alpaca_base_url}/v2/positions",
            headers={
                "APCA-API-KEY-ID": self.config.alpaca_api_key,
                "APCA-API-SECRET-KEY": self.config.alpaca_secret_key,
            },
            timeout=15,
        )
        positions_response.raise_for_status()
        account_response = requests.get(
            f"{self.config.alpaca_base_url}/v2/account",
            headers={
                "APCA-API-KEY-ID": self.config.alpaca_api_key,
                "APCA-API-SECRET-KEY": self.config.alpaca_secret_key,
            },
            timeout=15,
        )
        account_response.raise_for_status()
        local_state = self._read_state_file()
        account_payload = account_response.json()

        rebuilt_orders: list[LiveOrder] = []
        open_orders_by_symbol: dict[str, list[str]] = {}
        reserved_notional_by_symbol: dict[str, float] = {}
        reserved_exit_qty_by_symbol: dict[str, float] = {}
        for raw in open_orders_response.json():
            client_order_id = raw.get("client_order_id") or raw.get("id", "")
            symbol = raw.get("symbol", "")
            normalized_id = client_order_id.replace("_", "").lower()
            strategy_guess = next(
                (s for s in STRATEGIES if s.replace("_", "")[:12].lower() in normalized_id),
                self.config.strategy,
            )
            persisted_order = local_state.get("orders", {}).get(client_order_id, {})
            stop_price = float(persisted_order.get("stop_price") or 0.0)
            entry_exit = persisted_order.get("entry_exit") or ("exit" if "exit" in normalized_id else "entry")
            intended_price = float(raw.get("limit_price") or persisted_order.get("intended_price") or 0.0)
            order = LiveOrder(
                client_order_id=client_order_id,
                symbol=symbol,
                strategy=str(persisted_order.get("strategy") or strategy_guess),
                side=raw.get("side", ""),
                intended_qty=float(raw.get("qty") or 0.0),
                intended_price=intended_price,
                signal_price=float(persisted_order.get("signal_price") or intended_price or 0.0),
                stop_price=stop_price,
                status=str(raw.get("status", "accepted")).lower(),
                filled_qty=float(raw.get("filled_qty") or 0.0),
                avg_fill_price=float(raw.get("filled_avg_price") or 0.0),
                broker_order_id=raw.get("id", ""),
                entry_exit=entry_exit,
            )
            rebuilt_orders.append(order)
            open_orders_by_symbol.setdefault(symbol, []).append(client_order_id)
            if order.entry_exit == "exit":
                reserved_exit_qty_by_symbol[symbol] = reserved_exit_qty_by_symbol.get(symbol, 0.0) + order.remaining_qty
            else:
                reserved_notional_by_symbol[symbol] = reserved_notional_by_symbol.get(symbol, 0.0) + (
                    order.remaining_qty * max(order.intended_price, 0.0)
                )
        self.state_machine.replace_open_orders(rebuilt_orders)

        rebuilt_positions: dict[str, LivePosition] = {}
        recovery_only_symbols = set(local_state.get("recovery_only", []))
        daily_unrealized_pnl = 0.0
        for raw in positions_response.json():
            qty = abs(float(raw.get("qty") or 0.0))
            if qty <= 0:
                continue
            side = "buy" if float(raw.get("qty") or 0.0) > 0 else "sell"
            symbol = raw["symbol"]
            persisted_position = local_state.get("positions", {}).get(symbol, {})
            stop_price = float(persisted_position.get("stop_price") or 0.0)
            initial_risk = float(persisted_position.get("initial_risk_per_unit") or 0.0)
            recovery_only = bool(persisted_position.get("recovery_only")) or stop_price <= 0 or initial_risk <= 0
            if recovery_only:
                recovery_only_symbols.add(symbol)
            rebuilt_positions[raw["symbol"]] = LivePosition(
                symbol=symbol,
                strategy=str(persisted_position.get("strategy") or self.config.strategy),
                side=side,
                qty=qty,
                avg_entry_price=float(raw.get("avg_entry_price") or 0.0),
                stop_price=stop_price,
                initial_risk_per_unit=initial_risk,
                entry_time=utc_timestamp(persisted_position.get("entry_time")) if persisted_position.get("entry_time") else utc_now(),
                entry_count=int(persisted_position.get("entry_count") or 1),
                partial_taken=bool(persisted_position.get("partial_taken", False)),
                realized_pnl=float(persisted_position.get("realized_pnl") or 0.0),
                recovery_only=recovery_only,
            )
            daily_unrealized_pnl += float(raw.get("unrealized_pl") or 0.0)
        self.broker_state.positions = rebuilt_positions
        self.broker_state.open_orders_by_symbol = open_orders_by_symbol
        self.broker_state.recovery_only_symbols = recovery_only_symbols
        self.broker_state.cooldowns = {
            symbol: utc_timestamp(ts)
            for symbol, ts in local_state.get("cooldowns", {}).items()
            if ts
        }
        self.broker_state.daily_realized_pnl = float(local_state.get("daily_realized_pnl") or 0.0)
        self.broker_state.daily_unrealized_pnl = daily_unrealized_pnl
        self.broker_state.reserved_notional_by_symbol = reserved_notional_by_symbol
        self.broker_state.reserved_exit_qty_by_symbol = reserved_exit_qty_by_symbol
        self.broker_state.account_equity = float(account_payload.get("equity") or 0.0)
        self.broker_state.account_cash = float(account_payload.get("cash") or 0.0)
        self.broker_state.buying_power = float(account_payload.get("buying_power") or 0.0)
        combined_loss = -(self.broker_state.daily_realized_pnl + self.broker_state.daily_unrealized_pnl)
        capital_base = self.broker_state.account_equity or self.config.starting_capital
        self.broker_state.kill_switch_active = combined_loss >= (capital_base * self.config.max_daily_loss)
        self.broker_state.uncertain = False
        self._persist_local_state()

    def _validate_live_config(self) -> None:
        trade_mode = execution_mode(self.config.mode)
        if trade_mode not in {"paper", "live"}:
            raise ValueError("Trading mode must be paper or live.")
        if trade_mode == "live" and os.getenv("ALLOW_LIVE", "").lower() != "true":
            raise ValueError("Live mode is blocked unless ALLOW_LIVE=true is set in the environment.")
        if not (self.config.alpaca_api_key and self.config.alpaca_secret_key):
            raise ValueError("Missing Alpaca credentials.")

    async def _watch_stream_health(self) -> None:
        while True:
            await asyncio.sleep(1.0)
            now = utc_now()
            if (
                self.stream_started_at is not None
                and (now - self.stream_started_at).total_seconds() < self.config.stream_startup_grace_seconds
            ):
                continue
            if (
                self._expects_live_market_data(now)
                and self.last_market_data_time
                and (now - self.last_market_data_time).total_seconds() > self.config.market_data_stale_seconds
            ):
                if self._refresh_quote_cache():
                    continue
                raise RuntimeError("Market data stream is stale.")
            if (
                (self.state_machine.orders or self.broker_state.positions)
                and self._has_active_orders()
                and self.last_trade_update_time
                and (now - self.last_trade_update_time).total_seconds() > self.config.trade_update_stale_seconds
            ):
                self._safe_reconcile()
                if not self._has_active_orders():
                    self.last_trade_update_time = utc_now()
                    continue
                raise RuntimeError("Trade update stream is stale.")

    async def _submit_order_with_retry(self, order: LiveOrder, submitter: Any) -> None:
        attempts = 0
        while attempts <= self.config.max_submit_retries:
            try:
                response = submitter()
                self.state_machine.apply_update(
                    client_order_id=order.client_order_id,
                    status=response.get("status", "accepted"),
                    filled_qty=float(response.get("filled_qty") or 0.0),
                    avg_fill_price=float(response.get("filled_avg_price") or 0.0),
                    broker_order_id=response.get("id", ""),
                    timestamp=utc_now(),
                )
                self._persist_local_state()
                return
            except requests.HTTPError as exc:
                status_code = exc.response.status_code if exc.response is not None else 0
                if 400 <= status_code < 500 and status_code != 429:
                    self.state_machine.apply_update(
                        client_order_id=order.client_order_id,
                        status="rejected",
                        filled_qty=order.filled_qty,
                        avg_fill_price=order.avg_fill_price,
                        rejection_reason=str(exc),
                        timestamp=utc_now(),
                    )
                    self._set_symbol_cooldown(order.symbol, self.config.cooldown_minutes_after_rejection)
                    self._persist_local_state()
                    return
                attempts += 1
            except (requests.Timeout, requests.ConnectionError):
                self.state_machine.apply_update(
                    client_order_id=order.client_order_id,
                    status="pending_reconcile",
                    filled_qty=order.filled_qty,
                    avg_fill_price=order.avg_fill_price,
                    rejection_reason="submit_outcome_unknown",
                    timestamp=utc_now(),
                )
                self._safe_reconcile()
                existing = self._lookup_broker_order(order.client_order_id)
                if existing is not None:
                    self.state_machine.apply_update(
                        client_order_id=order.client_order_id,
                        status=str(existing.get("status", "accepted")).lower(),
                        filled_qty=float(existing.get("filled_qty") or 0.0),
                        avg_fill_price=float(existing.get("filled_avg_price") or 0.0),
                        broker_order_id=existing.get("id", ""),
                        timestamp=utc_now(),
                    )
                    self._persist_local_state()
                    return
                attempts += 1
            except Exception as exc:
                attempts += 1
                if attempts > self.config.max_submit_retries:
                    self.state_machine.apply_update(
                        client_order_id=order.client_order_id,
                        status="rejected",
                        filled_qty=order.filled_qty,
                        avg_fill_price=order.avg_fill_price,
                        rejection_reason=str(exc),
                        timestamp=utc_now(),
                    )
                    self._persist_local_state()
                    return

            if attempts <= self.config.max_submit_retries:
                await asyncio.sleep(min(self.config.reconnect_backoff_seconds * attempts, self.config.reconnect_backoff_max_seconds))
        self.state_machine.apply_update(
            client_order_id=order.client_order_id,
            status="rejected",
            filled_qty=order.filled_qty,
            avg_fill_price=order.avg_fill_price,
            rejection_reason="submit_retry_exhausted",
            timestamp=utc_now(),
        )
        self.broker_state.recovery_only_symbols.add(order.symbol)
        self._persist_local_state()

    def _lookup_broker_order(self, client_order_id: str) -> dict[str, Any] | None:
        try:
            after = (datetime.now(UTC) - timedelta(minutes=self.config.recent_order_lookup_minutes)).isoformat()
            response = requests.get(
                f"{self.config.alpaca_base_url}/v2/orders",
                headers={
                    "APCA-API-KEY-ID": self.config.alpaca_api_key,
                    "APCA-API-SECRET-KEY": self.config.alpaca_secret_key,
                },
                params={"status": "all", "nested": "false", "limit": 500, "after": after},
                timeout=15,
            )
            response.raise_for_status()
            for raw in response.json():
                if raw.get("client_order_id") == client_order_id:
                    return raw
        except Exception:
            return None
        return None

    def _entry_risk_checks(self, symbol: str, price_reference: float, qty: float, quote: dict[str, Any], strategy_name: str) -> bool:
        if self.broker_state.uncertain or self.broker_state.kill_switch_active:
            return False
        spread_bps = float(quote.get("spread_bps") or 0.0)
        if spread_bps > self.config.max_spread_bps_live:
            return False
        quote_timestamp = quote.get("timestamp")
        if quote_timestamp is not None and (utc_now() - quote_timestamp).total_seconds() > self.config.market_data_stale_seconds:
            return False
        current_symbol_exposure = self._symbol_exposure(symbol)
        current_gross_exposure = self._gross_exposure()
        proposed_notional = price_reference * qty
        live_capital = self._capital_base()
        position_limit = self.config.max_position_notional
        symbol_limit = self.config.max_symbol_exposure
        gross_limit = self.config.max_gross_exposure
        if strategy_name == PremarketRegressionStrategy.name:
            allocation_cap = live_capital * max(min(self.config.premarket_regression_allocation_fraction, 1.0), 0.0)
            position_limit = max(position_limit, allocation_cap)
            symbol_limit = max(symbol_limit, allocation_cap)
            gross_limit = max(gross_limit, allocation_cap)
        if current_symbol_exposure >= symbol_limit:
            return False
        if current_gross_exposure >= gross_limit:
            return False
        if proposed_notional > position_limit:
            return False
        if current_symbol_exposure + proposed_notional > symbol_limit:
            return False
        if current_gross_exposure + proposed_notional > gross_limit:
            return False
        if proposed_notional > self.broker_state.account_cash > 0:
            return False
        if live_capital <= 0:
            return False
        return True

    def _active_entry_layers(self, symbol: str, strategy_name: str) -> int:
        layers = 0
        position = self.broker_state.positions.get(symbol)
        if position is not None and position.strategy == strategy_name:
            layers += max(position.entry_count, 1)
        for order in self.state_machine.orders.values():
            if order.symbol != symbol or order.strategy != strategy_name or order.entry_exit != "entry":
                continue
            if order.status in {"new", "pending_reconcile", "accepted", "partially_filled"}:
                layers += 1
        return layers

    def _symbol_exposure(self, symbol: str) -> float:
        self._sync_order_reservations()
        exposure = self.broker_state.reserved_notional_by_symbol.get(symbol, 0.0)
        position = self.broker_state.positions.get(symbol)
        if position is not None:
            exposure += position.qty * position.avg_entry_price
        return exposure

    def _gross_exposure(self) -> float:
        self._sync_order_reservations()
        total = sum(position.qty * position.avg_entry_price for position in self.broker_state.positions.values())
        total += sum(self.broker_state.reserved_notional_by_symbol.values())
        return total

    def _available_exit_qty(self, symbol: str) -> float:
        self._sync_order_reservations()
        position = self.broker_state.positions.get(symbol)
        if position is None:
            return 0.0
        reserved = self.broker_state.reserved_exit_qty_by_symbol.get(symbol, 0.0)
        return normalize_qty(self.config.asset_class, max(position.qty - reserved, 0.0))

    def _capital_base(self) -> float:
        if execution_mode(self.config.mode) in {"paper", "live"}:
            return self.broker_state.account_equity or self.config.starting_capital
        return self.config.starting_capital

    def _stream_quote_snapshot(self, symbol: str) -> dict[str, Any] | None:
        quote = self.quote_cache.get(symbol)
        if quote is None:
            return None
        return {
            "mid": quote.mid,
            "bid": quote.bid,
            "ask": quote.ask,
            "spread_bps": quote.spread_bps,
            "timestamp": quote.timestamp,
        }

    def _refresh_quote_cache(self) -> bool:
        refreshed = False
        for symbol in self.config.symbols:
            snapshot = self.loader.latest_quote_snapshot(symbol)
            mid = float(snapshot.get("mid") or 0.0)
            bid = float(snapshot.get("bid") or 0.0)
            ask = float(snapshot.get("ask") or 0.0)
            timestamp = snapshot.get("timestamp")
            if mid <= 0 and bid <= 0 and ask <= 0:
                continue
            self.quote_cache[symbol] = StreamQuote(
                symbol=symbol,
                bid=bid,
                ask=ask,
                mid=mid,
                spread_bps=float(snapshot.get("spread_bps") or 0.0),
                timestamp=timestamp if timestamp is not None else utc_now(),
            )
            refreshed = True
        if refreshed:
            self.last_market_data_time = utc_now()
        return refreshed

    def _sync_order_reservations(self) -> None:
        reserved_notional_by_symbol: dict[str, float] = {}
        reserved_exit_qty_by_symbol: dict[str, float] = {}
        for order in self.state_machine.orders.values():
            if order.status not in {"new", "pending_reconcile", "accepted", "partially_filled"}:
                continue
            if order.entry_exit == "exit":
                reserved_exit_qty_by_symbol[order.symbol] = reserved_exit_qty_by_symbol.get(order.symbol, 0.0) + order.remaining_qty
            else:
                reserved_notional_by_symbol[order.symbol] = reserved_notional_by_symbol.get(order.symbol, 0.0) + (
                    order.remaining_qty * max(order.intended_price, 0.0)
                )
        self.broker_state.reserved_notional_by_symbol = reserved_notional_by_symbol
        self.broker_state.reserved_exit_qty_by_symbol = reserved_exit_qty_by_symbol

    def _can_trade_symbol(self, symbol: str) -> bool:
        if symbol in self.broker_state.recovery_only_symbols:
            return False
        cooldown_until = self.broker_state.cooldowns.get(symbol)
        if cooldown_until and cooldown_until > utc_now():
            return False
        if self.last_market_data_time and (utc_now() - self.last_market_data_time).total_seconds() > self.config.market_data_stale_seconds:
            return False
        return True

    def _has_active_orders(self) -> bool:
        return any(order.status in {"new", "pending_reconcile", "accepted", "partially_filled"} for order in self.state_machine.orders.values())

    def _expects_live_market_data(self, now: pd.Timestamp) -> bool:
        if self.config.asset_class == "crypto":
            return True
        return within_session(now, self.config)

    def _set_symbol_cooldown(self, symbol: str, minutes: int) -> None:
        self.broker_state.cooldowns[symbol] = utc_now() + pd.Timedelta(minutes=minutes)

    def _roll_session_day_if_needed(self, ts: pd.Timestamp) -> None:
        session_day = pd.Timestamp(ts).date()
        if session_day != self.current_session_day:
            self.current_session_day = session_day
            self.broker_state.daily_realized_pnl = 0.0
            self.broker_state.kill_switch_active = False
            self._persist_local_state()

    def _safe_reconcile(self) -> None:
        try:
            self.reconcile_state()
        except Exception as exc:
            print(f"Reconciliation failed: {exc}")

    def _read_state_file(self) -> dict[str, Any]:
        if not self.state_path.exists():
            return {}
        try:
            return json.loads(self.state_path.read_text())
        except Exception:
            return {}

    def _load_local_state(self) -> None:
        state = self._read_state_file()
        loaded_orders: list[LiveOrder] = []
        for payload in state.get("orders", {}).values():
            try:
                loaded_orders.append(
                    LiveOrder(
                        client_order_id=payload["client_order_id"],
                        symbol=payload["symbol"],
                        strategy=payload["strategy"],
                        side=payload["side"],
                        intended_qty=float(payload["intended_qty"]),
                        intended_price=float(payload["intended_price"]),
                        signal_price=float(payload["signal_price"]),
                        stop_price=float(payload.get("stop_price") or 0.0),
                        status=str(payload.get("status") or "new"),
                        filled_qty=float(payload.get("filled_qty") or 0.0),
                        avg_fill_price=float(payload.get("avg_fill_price") or 0.0),
                        last_update_time=utc_timestamp(payload.get("last_update_time")) if payload.get("last_update_time") else utc_now(),
                        rejection_reason=str(payload.get("rejection_reason") or ""),
                        broker_order_id=str(payload.get("broker_order_id") or ""),
                        entry_exit=str(payload.get("entry_exit") or "entry"),
                        expected_spread_bps=float(payload.get("expected_spread_bps") or 0.0),
                        estimated_spread_cost=float(payload.get("estimated_spread_cost") or 0.0),
                        session_bucket=str(payload.get("session_bucket") or ""),
                        bar_time=utc_timestamp(payload.get("bar_time")) if payload.get("bar_time") else None,
                    )
                )
            except Exception:
                continue
        if loaded_orders:
            self.state_machine.replace_open_orders(loaded_orders)
        self._sync_order_reservations()
        self.broker_state.cooldowns = {
            symbol: utc_timestamp(ts)
            for symbol, ts in state.get("cooldowns", {}).items()
            if ts
        }
        self.broker_state.recovery_only_symbols = set(state.get("recovery_only", []))
        if state.get("session_day") == str(self.current_session_day):
            self.broker_state.daily_realized_pnl = float(state.get("daily_realized_pnl") or 0.0)

    def _persist_local_state(self) -> None:
        serialized_orders: dict[str, Any] = {}
        for client_order_id, order in self.state_machine.orders.items():
            serialized_orders[client_order_id] = {
                "client_order_id": order.client_order_id,
                "symbol": order.symbol,
                "strategy": order.strategy,
                "side": order.side,
                "intended_qty": order.intended_qty,
                "intended_price": order.intended_price,
                "signal_price": order.signal_price,
                "stop_price": order.stop_price,
                "status": order.status,
                "filled_qty": order.filled_qty,
                "avg_fill_price": order.avg_fill_price,
                "last_update_time": order.last_update_time.isoformat(),
                "rejection_reason": order.rejection_reason,
                "broker_order_id": order.broker_order_id,
                "entry_exit": order.entry_exit,
                "expected_spread_bps": order.expected_spread_bps,
                "estimated_spread_cost": order.estimated_spread_cost,
                "session_bucket": order.session_bucket,
                "bar_time": order.bar_time.isoformat() if order.bar_time is not None else "",
            }
        serialized_positions: dict[str, Any] = {}
        for symbol, position in self.broker_state.positions.items():
            serialized_positions[symbol] = {
                "symbol": position.symbol,
                "strategy": position.strategy,
                "side": position.side,
                "qty": position.qty,
                "avg_entry_price": position.avg_entry_price,
                "stop_price": position.stop_price,
                "initial_risk_per_unit": position.initial_risk_per_unit,
                "entry_time": position.entry_time.isoformat(),
                "entry_count": position.entry_count,
                "partial_taken": position.partial_taken,
                "realized_pnl": position.realized_pnl,
                "recovery_only": position.recovery_only,
            }
        payload = {
            "session_day": str(self.current_session_day),
            "daily_realized_pnl": self.broker_state.daily_realized_pnl,
            "orders": serialized_orders,
            "positions": serialized_positions,
            "cooldowns": {symbol: ts.isoformat() for symbol, ts in self.broker_state.cooldowns.items()},
            "recovery_only": sorted(self.broker_state.recovery_only_symbols),
        }
        self._sync_order_reservations()
        self.state_path.write_text(json.dumps(payload, indent=2, sort_keys=True))


class ScheduledSessionRunner:
    def __init__(self, config: Config, journal: Journal):
        self.config = config
        self.journal = journal

    async def run(self) -> None:
        trade_mode = execution_mode(self.config.mode)
        if trade_mode not in {"paper", "live"}:
            raise ValueError("Scheduled mode requires paper or live execution.")
        if self.config.asset_class != "equity":
            raise ValueError("Scheduled mode is only implemented for equities.")
        scheduled_config = replace(self.config, mode=trade_mode)
        session = self._next_session_window(utc_now(), scheduled_config)
        await self._sleep_until(session["start"], f"waiting for scheduled start at {session['start'].isoformat()}")
        engine = StreamExecutionEngine(scheduled_config, self.journal)
        engine_task = asyncio.create_task(engine.run())
        flattened = False
        try:
            while True:
                now = utc_now()
                if not flattened and now >= session["flatten"]:
                    print(f"Scheduled flatten window reached at {now.isoformat()}")
                    await engine.flatten_for_session_close(reason="scheduled_close")
                    flattened = True
                if now >= session["stop"]:
                    print(f"Scheduled shutdown window reached at {now.isoformat()}")
                    break
                if engine_task.done():
                    await engine_task
                    return
                await asyncio.sleep(min(self.config.schedule_poll_seconds, 30))
        finally:
            if not engine_task.done():
                engine_task.cancel()
                await asyncio.gather(engine_task, return_exceptions=True)

    async def _sleep_until(self, target: pd.Timestamp, message: str) -> None:
        delay = max((target - utc_now()).total_seconds(), 0.0)
        if delay <= 0:
            return
        print(message)
        await asyncio.sleep(delay)

    def _next_session_window(self, now: pd.Timestamp, config: Config) -> dict[str, pd.Timestamp]:
        now_utc = utc_timestamp(now)
        for session in self._calendar_sessions(config, now_utc.date(), now_utc.date() + timedelta(days=10)):
            if now_utc < session["stop"]:
                return session
        return self._fallback_session_window(now_utc, config)

    def _calendar_sessions(self, config: Config, start: date, end: date) -> list[dict[str, pd.Timestamp]]:
        if not (config.alpaca_api_key and config.alpaca_secret_key):
            return []
        try:
            response = requests.get(
                f"{config.alpaca_base_url}/v2/calendar",
                headers={
                    "APCA-API-KEY-ID": config.alpaca_api_key,
                    "APCA-API-SECRET-KEY": config.alpaca_secret_key,
                },
                params={"start": start.isoformat(), "end": end.isoformat()},
                timeout=15,
            )
            response.raise_for_status()
        except Exception:
            return []
        sessions: list[dict[str, pd.Timestamp]] = []
        for row in response.json():
            session_date = date.fromisoformat(row["date"])
            open_local = pd.Timestamp(
                datetime.combine(
                    session_date,
                    time.fromisoformat(row.get("open", "09:30")),
                    tzinfo=market_timezone(config),
                )
            )
            close_local = pd.Timestamp(
                datetime.combine(
                    session_date,
                    time.fromisoformat(row.get("close", "16:00")),
                    tzinfo=market_timezone(config),
                )
            )
            sessions.append(self._session_window_from_bounds(open_local.tz_convert("UTC"), close_local.tz_convert("UTC"), config))
        return sessions

    def _fallback_session_window(self, now: pd.Timestamp, config: Config) -> dict[str, pd.Timestamp]:
        candidate = now.tz_convert(market_timezone(config)).date()
        while True:
            if candidate.weekday() < 5:
                open_local = pd.Timestamp(
                    datetime.combine(
                        candidate,
                        time(config.market_open_hour_local, config.market_open_minute_local),
                        tzinfo=market_timezone(config),
                    )
                )
                close_local = pd.Timestamp(
                    datetime.combine(
                        candidate,
                        time(config.market_close_hour_local, config.market_close_minute_local),
                        tzinfo=market_timezone(config),
                    )
                )
                session = self._session_window_from_bounds(open_local.tz_convert("UTC"), close_local.tz_convert("UTC"), config)
                if now < session["stop"]:
                    return session
            candidate += timedelta(days=1)

    def _session_window_from_bounds(self, market_open: pd.Timestamp, market_close: pd.Timestamp, config: Config) -> dict[str, pd.Timestamp]:
        return {
            "open": market_open,
            "close": market_close,
            "start": market_open - pd.Timedelta(minutes=config.schedule_start_minutes_before_open),
            "flatten": market_close - pd.Timedelta(minutes=config.schedule_flatten_minutes_before_close),
            "stop": market_close + pd.Timedelta(minutes=config.schedule_shutdown_minutes_after_close),
        }


class TradingBot:
    def __init__(self, config: Config):
        self.config = config

    def _run_async(self, coroutine: Any) -> None:
        try:
            asyncio.run(coroutine)
        except KeyboardInterrupt:
            print("Shutdown requested. Exiting cleanly.")

    def run(self, compare: bool = False, show_plan: bool = False) -> None:
        if show_plan:
            print(PLAN_TEXT.strip())
        if compare:
            frame = compare_strategies(self.config, self.config.compare_strategies)
            print_strategy_comparison(frame)
            return
        if execution_mode(self.config.mode) == "backtest":
            journal = Journal(self.config.journal_path)
            result = BacktestEngine(self.config, journal).run(self.config.strategy)
            print_backtest_summary(result)
            return
        journal = Journal(self.config.journal_path)
        if is_scheduled_mode(self.config.mode):
            self._run_async(ScheduledSessionRunner(self.config, journal).run())
            return
        self._run_async(StreamExecutionEngine(self.config, journal).run())


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Lightweight trading bot for small accounts.")
    parser.add_argument("--mode", choices=["backtest", "paper", "live", "scheduled_paper", "scheduled_live"], default="backtest")
    parser.add_argument("--asset-class", choices=["equity", "crypto"], default=None)
    parser.add_argument("--strategy", choices=sorted(STRATEGIES.keys()), default="momentum")
    parser.add_argument("--compare", action="store_true", help="Run comparison across configured strategies.")
    parser.add_argument("--symbols", nargs="+", help="Override watchlist symbols.")
    parser.add_argument("--capital", type=float, help="Override starting capital.")
    parser.add_argument("--timeframe", default=None, help="Override timeframe such as 5Min or 15Min.")
    parser.add_argument("--show-plan", action="store_true", help="Print the short implementation plan before running.")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    config = Config.from_env()
    config.mode = args.mode
    config.strategy = args.strategy
    if args.asset_class:
        config.asset_class = args.asset_class
    if args.symbols:
        config.symbols = args.symbols
    if args.capital:
        config.starting_capital = args.capital
    if args.timeframe:
        config.timeframe = args.timeframe
    elif config.strategy == BollingerMeanReversionLongStrategy.name and config.timeframe == "15Min":
        config.timeframe = "1Hour"
    if (
        config.strategy == BollingerMeanReversionLongStrategy.name
        and not args.symbols
        and config.symbols == ["AAPL", "MSFT", "AMD"]
    ):
        config.symbols = ["XAUUSD"]
    if config.strategy == PremarketRegressionStrategy.name:
        config.symbols = [config.premarket_regression_symbol]
    if config.strategy not in STRATEGIES:
        raise ValueError(f"Unknown strategy: {config.strategy}")
    TradingBot(config).run(compare=args.compare, show_plan=args.show_plan)


if __name__ == "__main__":
    main()
