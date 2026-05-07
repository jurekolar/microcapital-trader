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
from typing import Any, Callable
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


SCHEDULED_MODE_MAP: dict[str, str] = {
    "scheduled_paper": "paper",
    "scheduled_live": "live",
}

TRUTHY_VALUES = {"1", "true", "yes", "y", "on"}
FALSY_VALUES = {"0", "false", "no", "n", "off"}

RISK_PROFILES = {"conservative", "aggressive_margin"}

OPTIMIZATION_TIMEFRAMES = ["15Min", "30Min", "1Hour", "5Min"]
OPTIMIZATION_MIN_TRADE_COUNT = 15
OPTIMIZATION_MAX_DRAWDOWN = -6.0

ACTIVE_ORDER_STATUSES = {
    "new",
    "pending_new",
    "accepted",
    "accepted_for_bidding",
    "partially_filled",
    "pending_replace",
    "pending_cancel",
    "held",
    "stopped",
    "suspended",
    "pending_reconcile",
}

TERMINAL_ORDER_STATUSES = {
    "filled",
    "canceled",
    "cancelled",
    "expired",
    "done_for_day",
    "rejected",
    "replaced",
    "calculated",
}

KNOWN_ORDER_STATUSES = ACTIVE_ORDER_STATUSES | TERMINAL_ORDER_STATUSES


ORDER_TRANSITIONS: dict[str, set[str]] = {
    status: set(KNOWN_ORDER_STATUSES) for status in ACTIVE_ORDER_STATUSES
}
ORDER_TRANSITIONS.update({status: set() for status in TERMINAL_ORDER_STATUSES})


def new_run_id() -> str:
    stamp = datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")
    return f"run-{stamp}-{random.randint(0, 999999):06d}"


def parse_bool(value: Any, default: bool = False) -> bool:
    if value is None:
        return default
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        normalized = value.strip().lower()
        if normalized in TRUTHY_VALUES:
            return True
        if normalized in FALSY_VALUES:
            return False
        return default
    return bool(value)


def truncate_text(value: str, max_length: int = 500) -> str:
    compact = " ".join(value.split())
    if len(compact) <= max_length:
        return compact
    return f"{compact[: max_length - 3]}..."


def format_http_error(exc: requests.HTTPError) -> str:
    response = exc.response
    if response is None:
        return str(exc)
    status_code = response.status_code
    reason = response.reason or ""
    status = f"HTTP {status_code}"
    if reason:
        status = f"{status} {reason}"
    parts = [status]
    request_id = response.headers.get("X-Request-ID") or response.headers.get("x-request-id")
    if request_id:
        parts.append(f"request_id={request_id}")
    body = ""
    try:
        body = json.dumps(response.json(), sort_keys=True, separators=(",", ":"))
    except ValueError:
        body = response.text or ""
    body = truncate_text(body)
    if body:
        parts.append(f"body={body}")
    return " ".join(parts)


@dataclass
class Config:
    mode: str = "backtest"
    asset_class: str = "equity"
    strategy: str = "momentum"
    risk_profile: str = "conservative"
    symbols: list[str] = field(
        default_factory=lambda: [
            "TQQQ",
            "MSFT",
            "ORCL",
            "NET",
            "PYPL",
            "CAT",
            "NFLX",
            "INTC",
            "PLTR",
            "AMZN",
            "NOW",
            "BABA",
            "ARM",
            "CRWD",
            "QCOM",
        ]
    )
    timeframe: str = "15Min"
    lookback_days: int = 30
    starting_capital: float = 1_000.0
    risk_per_trade: float = 0.05
    capital_deployment_fraction: float = 1.0
    max_daily_loss: float = 0.10
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
    stop_buffer_pct: float = 0.0025
    partial_take_profit_r: float = 1.0
    final_take_profit_r: float = 2.0
    compare_output_path: str = "strategy_comparison.csv"
    optimization_output_path: str = "optimization_results.csv"
    journal_path: str = "trade_journal.csv"
    state_path: str = "live_state.json"
    run_id: str = field(default_factory=new_run_id)
    data_dir: str = "data"
    strict_data: bool = False
    historical_end: str = ""
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
    max_gross_exposure: float = 3_000.0
    max_symbol_exposure: float = 1_000.0
    max_position_notional: float = 1_000.0
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
            risk_profile=os.getenv("RISK_PROFILE", "conservative"),
            run_id=os.getenv("RUN_ID", new_run_id()),
        )
        config.apply_risk_profile_defaults()
        if os.getenv("SYMBOLS"):
            config.symbols = [s.strip() for s in os.getenv("SYMBOLS", "").split(",") if s.strip()]
        if os.getenv("MODE"):
            config.mode = os.getenv("MODE", config.mode)
        if os.getenv("ALLOW_SHORT"):
            config.allow_short = parse_bool(os.getenv("ALLOW_SHORT"), config.allow_short)
        if os.getenv("RISK_PER_TRADE"):
            config.risk_per_trade = float(os.getenv("RISK_PER_TRADE", config.risk_per_trade))
        if os.getenv("CAPITAL_DEPLOYMENT_FRACTION"):
            config.capital_deployment_fraction = float(os.getenv("CAPITAL_DEPLOYMENT_FRACTION", config.capital_deployment_fraction))
        if os.getenv("MAX_DAILY_LOSS"):
            config.max_daily_loss = float(os.getenv("MAX_DAILY_LOSS", config.max_daily_loss))
        if os.getenv("SLIPPAGE_BPS"):
            config.slippage_bps = float(os.getenv("SLIPPAGE_BPS", config.slippage_bps))
        if os.getenv("SPREAD_BPS"):
            config.spread_bps = float(os.getenv("SPREAD_BPS", config.spread_bps))
        if os.getenv("TIMEFRAME"):
            config.timeframe = os.getenv("TIMEFRAME", config.timeframe)
        if os.getenv("LOOKBACK_DAYS"):
            config.lookback_days = int(os.getenv("LOOKBACK_DAYS", config.lookback_days))
        if os.getenv("HISTORICAL_END"):
            config.historical_end = os.getenv("HISTORICAL_END", config.historical_end)
        if os.getenv("OPTIMIZATION_OUTPUT_PATH"):
            config.optimization_output_path = os.getenv("OPTIMIZATION_OUTPUT_PATH", config.optimization_output_path)
        if os.getenv("JOURNAL_PATH"):
            config.journal_path = os.getenv("JOURNAL_PATH", config.journal_path)
        if os.getenv("STATE_PATH"):
            config.state_path = os.getenv("STATE_PATH", config.state_path)
        if os.getenv("STRICT_DATA"):
            config.strict_data = parse_bool(os.getenv("STRICT_DATA"), config.strict_data)
        return config

    def apply_risk_profile_defaults(self) -> None:
        if self.risk_profile not in RISK_PROFILES:
            raise ValueError(f"Unsupported risk profile: {self.risk_profile}")
        if self.risk_profile == "aggressive_margin":
            self.risk_per_trade = 1.0
            self.capital_deployment_fraction = 1.0
            self.max_daily_loss = 1.0


def utc_now() -> pd.Timestamp:
    return pd.Timestamp.now(tz="UTC")


def execution_mode(mode: str) -> str:
    return SCHEDULED_MODE_MAP.get(mode, mode)


def is_scheduled_mode(mode: str) -> bool:
    return mode in SCHEDULED_MODE_MAP


def is_connection_limit_error(exc: BaseException) -> bool:
    seen: set[int] = set()
    current: BaseException | None = exc
    while current is not None and id(current) not in seen:
        seen.add(id(current))
        if "connection limit exceeded" in str(current).lower():
            return True
        current = current.__cause__ or current.__context__
    return False


def normalize_order_status(status: Any) -> str:
    normalized = _enum_value(status).strip().lower()
    if normalized == "cancelled":
        return "canceled"
    return normalized or "accepted"


def is_active_order_status(status: str) -> bool:
    return normalize_order_status(status) in ACTIVE_ORDER_STATUSES


def uses_aggressive_margin(config: Config) -> bool:
    return config.risk_profile == "aggressive_margin"


def buying_power_budget(config: Config, equity: float, buying_power: float = 0.0) -> float:
    if uses_aggressive_margin(config):
        if execution_mode(config.mode) in {"paper", "live"} and buying_power > 0:
            return buying_power * config.capital_deployment_fraction
        return max(config.max_gross_exposure, equity * config.capital_deployment_fraction)
    return equity * config.capital_deployment_fraction


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


def modeled_entry_fill_price(side: str, price: float, config: Config) -> float:
    spread_half = price * (config.spread_bps / 20_000.0)
    slip = price * (config.slippage_bps / 10_000.0)
    return price + spread_half + slip if side == "buy" else price - spread_half - slip


def modeled_exit_fill_price(position_side: str, price: float, config: Config) -> float:
    spread_half = price * (config.spread_bps / 20_000.0)
    slip = price * (config.slippage_bps / 10_000.0)
    return price - spread_half - slip if position_side == "buy" else price + spread_half + slip


def fill_event_from_snapshot(client_order_id: str, snapshot: dict[str, Any], rejection_reason: str = "") -> FillEvent:
    status = normalize_order_status(snapshot.get("status", "accepted"))
    return FillEvent(
        client_order_id=client_order_id,
        status=status,
        filled_qty=float(snapshot.get("filled_qty") or 0.0),
        avg_fill_price=float(snapshot.get("filled_avg_price") or 0.0),
        broker_order_id=str(snapshot.get("id", "") or ""),
        rejection_reason=rejection_reason if status == "rejected" else "",
        timestamp=utc_now(),
    )


def build_trade_intent(
    config: Config,
    mode: str,
    signal: Signal,
    quote: dict[str, Any],
    capital_base: float,
) -> TradeIntent | None:
    price_reference = float(quote.get("mid") or signal.entry)
    qty_basis = position_size_from_risk(
        capital_base,
        price_reference,
        signal.stop,
        config.risk_per_trade,
    )
    qty = normalize_qty(config.asset_class, qty_basis)
    if qty <= 0:
        return None
    client_order_id = deterministic_client_order_id(
        execution_mode(mode),
        signal.strategy,
        signal.symbol,
        signal.side,
        "entry",
        signal.bar_time,
    )
    return TradeIntent(
        signal=signal,
        qty=qty,
        price_reference=price_reference,
        quote=quote,
        client_order_id=client_order_id,
        capital_base=capital_base,
    )


def live_position_from_backtest(position: BacktestPosition) -> LivePosition:
    return LivePosition(
        symbol=position.symbol,
        strategy=position.strategy,
        side=position.side,
        qty=position.open_qty,
        avg_entry_price=position.entry_price,
        stop_price=position.stop_price,
        initial_risk_per_unit=position.initial_risk_per_unit,
        entry_time=position.entry_time,
        partial_taken=position.partial_taken,
        available_qty=position.open_qty,
    )


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


def normalize_base_url(url: str) -> str:
    return url.strip().rstrip("/")


def format_note_value(value: Any) -> str:
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, float):
        if not math.isfinite(value):
            return ""
        return str(round(value, 6))
    if isinstance(value, int):
        return str(value)
    return str(value).strip().replace(" ", "_")


def format_note_pairs(**pairs: Any) -> str:
    parts = []
    for key, value in pairs.items():
        if value is None:
            continue
        formatted = format_note_value(value)
        if formatted == "":
            continue
        parts.append(f"{key}={formatted}")
    return " ".join(parts)


def merge_notes(*notes: str) -> str:
    return " ".join(part.strip() for part in notes if part and part.strip())


def quote_age_seconds(timestamp: pd.Timestamp | None, now: pd.Timestamp | None = None) -> float:
    if timestamp is None:
        return 0.0
    reference = now or utc_now()
    return max((utc_timestamp(reference) - utc_timestamp(timestamp)).total_seconds(), 0.0)


class Journal:
    def __init__(self, path: str, run_id: str = ""):
        self.path = Path(path)
        self.run_id = run_id
        self.fields = [
            "run_id",
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
        else:
            self._ensure_schema()

    def log(self, row: dict[str, Any]) -> None:
        payload = {field: row.get(field, "") for field in self.fields}
        if self.run_id and not payload.get("run_id"):
            payload["run_id"] = self.run_id
        with self.path.open("a", newline="") as handle:
            writer = csv.DictWriter(handle, fieldnames=self.fields)
            writer.writerow(payload)

    def _ensure_schema(self) -> None:
        try:
            with self.path.open(newline="") as handle:
                reader = csv.DictReader(handle)
                existing_fields = reader.fieldnames or []
                if existing_fields == self.fields:
                    return
                if all(field in existing_fields for field in self.fields):
                    return
                rows = list(reader)
            with self.path.open("w", newline="") as handle:
                writer = csv.DictWriter(handle, fieldnames=self.fields)
                writer.writeheader()
                for row in rows:
                    writer.writerow({field: row.get(field, "") for field in self.fields})
        except Exception:
            return


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
        if self.config.strict_data:
            detail = f" Alpaca fetch failed: {fetch_error}" if fetch_error else " No local CSV was found and Alpaca data was unavailable."
            raise RuntimeError(f"Strict data mode refused synthetic sample data for {symbol} ({self.config.timeframe}).{detail}")
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
        end = self._historical_end()
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

    def _historical_end(self) -> datetime:
        if self.config.historical_end:
            return utc_timestamp(self.config.historical_end).to_pydatetime()
        return datetime.now(UTC)

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
        required_signal_fields = [
            "close",
            "high",
            "low",
            "ema_fast",
            "ema_slow",
            "vwap",
            "recent_high",
            "recent_low",
            "rvol",
            "strong_close",
            "weak_close",
        ]
        if any(pd.isna(row[key]) for key in required_signal_fields):
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


STRATEGIES: dict[str, Strategy] = {
    VWAPMomentumBreakoutStrategy.name: VWAPMomentumBreakoutStrategy(),
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
    quote_source: str = ""
    quote_age_seconds: float | None = None
    fallback_used: bool = False
    spread_bps_at_submission: float = 0.0

    def __post_init__(self) -> None:
        self.remaining_qty = round(max(self.intended_qty - self.filled_qty, 0.0), 8)
        if self.spread_bps_at_submission == 0.0 and self.expected_spread_bps:
            self.spread_bps_at_submission = self.expected_spread_bps


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
    partial_taken: bool = False
    realized_pnl: float = 0.0
    recovery_only: bool = False
    last_fill_time: pd.Timestamp = field(default_factory=utc_now)
    available_qty: float | None = None


@dataclass
class TradeIntent:
    signal: Signal
    qty: float
    price_reference: float
    quote: dict[str, Any]
    client_order_id: str
    capital_base: float


@dataclass
class ExitIntent:
    symbol: str
    side: str
    qty: float
    reason: str
    price_reference: float
    quote: dict[str, Any]
    client_order_id: str
    bar_time: pd.Timestamp


@dataclass
class FillEvent:
    client_order_id: str
    status: str
    filled_qty: float
    avg_fill_price: float
    broker_order_id: str = ""
    rejection_reason: str = ""
    timestamp: pd.Timestamp = field(default_factory=utc_now)


class BrokerAdapter(ABC):
    @abstractmethod
    def submit_entry(self, intent: TradeIntent) -> FillEvent:
        raise NotImplementedError

    @abstractmethod
    def submit_exit(self, intent: ExitIntent, position: LivePosition) -> FillEvent:
        raise NotImplementedError


class AlpacaBrokerAdapter(BrokerAdapter):
    def __init__(self, executor: BaseExecutor):
        self.executor = executor

    def submit_entry(self, intent: TradeIntent) -> FillEvent:
        snapshot = self.executor.submit_entry_order(intent.signal, intent.qty, intent.client_order_id)
        return fill_event_from_snapshot(intent.client_order_id, snapshot)

    def submit_exit(self, intent: ExitIntent, position: LivePosition) -> FillEvent:
        snapshot = self.executor.submit_exit_order(position, intent.qty, intent.client_order_id, intent.reason)
        return fill_event_from_snapshot(intent.client_order_id, snapshot)


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
        status = normalize_order_status(status)
        order.status = normalize_order_status(order.status)
        if status not in ORDER_TRANSITIONS:
            raise ValueError(f"Unsupported order status: {status}")
        if status != order.status and status not in ORDER_TRANSITIONS[order.status]:
            raise ValueError(f"Invalid order transition {order.status} -> {status} for {client_order_id}")
        order.status = status
        order.filled_qty = round(max(filled_qty, 0.0), 8)
        order.remaining_qty = round(max(order.intended_qty - order.filled_qty, 0.0), 8)
        order.last_fill_delta = round(max(order.filled_qty - order.last_processed_fill_qty, 0.0), 8)
        order.avg_fill_price = avg_fill_price or order.avg_fill_price
        order.broker_order_id = broker_order_id or order.broker_order_id
        order.rejection_reason = rejection_reason
        order.last_update_time = timestamp or pd.Timestamp.now(tz="UTC")
        self._journal(order, status)
        return order

    def has_open_order(self, symbol: str, side: str, entry_exit: str) -> bool:
        for order in self.orders.values():
            if order.symbol == symbol and order.side == side and order.entry_exit == entry_exit and is_active_order_status(order.status):
                return True
        return False

    def replace_open_orders(self, open_orders: list[LiveOrder]) -> None:
        retained: dict[str, LiveOrder] = {}
        for order in open_orders:
            retained[order.client_order_id] = order
        for client_order_id, existing in list(self.orders.items()):
            if not is_active_order_status(existing.status) or existing.status == "pending_reconcile":
                retained.setdefault(client_order_id, existing)
        self.orders = retained

    def _journal(self, order: LiveOrder, event: str, notes: str = "") -> None:
        execution_cost = ""
        if order.avg_fill_price and order.filled_qty > 0:
            execution_cost = round(
                order.estimated_spread_cost + abs(order.avg_fill_price - order.intended_price) * order.filled_qty,
                6,
            )
        actual_slippage_bps = None
        if order.avg_fill_price and order.intended_price > 0:
            actual_slippage_bps = ((order.avg_fill_price - order.intended_price) / order.intended_price) * 10_000.0
        metrics_notes = format_note_pairs(
            quote_source=order.quote_source or None,
            fallback_used=order.fallback_used if order.quote_source else None,
            quote_age_seconds=order.quote_age_seconds,
            spread_bps=order.spread_bps_at_submission if order.quote_source else None,
            actual_slippage_bps=actual_slippage_bps,
            broker_rejected=event == "rejected" or bool(order.rejection_reason),
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
                "notes": merge_notes(notes, metrics_notes),
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
    shorting_enabled: bool = False


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


class SimulatedBrokerAdapter(BrokerAdapter):
    def __init__(self, config: Config):
        self.config = config

    def submit_entry(self, intent: TradeIntent) -> FillEvent:
        fill_price = modeled_entry_fill_price(intent.signal.side, intent.price_reference, self.config)
        return FillEvent(
            client_order_id=intent.client_order_id,
            status="filled",
            filled_qty=intent.qty,
            avg_fill_price=fill_price,
            broker_order_id=f"sim-{intent.client_order_id}",
        )

    def submit_exit(self, intent: ExitIntent, position: LivePosition) -> FillEvent:
        fill_price = modeled_exit_fill_price(position.side, intent.price_reference, self.config)
        return FillEvent(
            client_order_id=intent.client_order_id,
            status="filled",
            filled_qty=intent.qty,
            avg_fill_price=fill_price,
            broker_order_id=f"sim-{intent.client_order_id}",
        )


class BacktestEngine:
    def __init__(
        self,
        config: Config,
        journal: Any,
        data_loader_factory: Callable[[Config], Any] = DataLoader,
    ):
        self.config = config
        self.journal = journal
        self.data_loader_factory = data_loader_factory

    def run(self) -> dict[str, Any]:
        strategy = STRATEGIES[self.config.strategy]
        broker = SimulatedBrokerAdapter(self.config)
        loader = self.data_loader_factory(self.config)
        equity = self.config.starting_capital
        positions: dict[str, BacktestPosition] = {}
        trade_pnls: list[float] = []
        r_multiples: list[float] = []
        equity_points: list[dict[str, Any]] = []
        spread_costs: list[float] = []
        slippages: list[float] = []
        holding_hours: list[float] = []
        rejected_trade_count = 0
        session_bucket_counts: dict[str, int] = {}
        quote_source_counts: dict[str, int] = {}
        fallback_count = 0
        data_sources: dict[str, str] = {}
        data_source_errors: dict[str, str] = {}
        data_by_symbol: dict[str, pd.DataFrame] = {}
        timeline: list[tuple[pd.Timestamp, str, int]] = []
        daily_loss_by_day: dict[date, float] = {}
        trades_by_day: dict[date, int] = {}
        max_buying_power_used = 0.0

        for symbol in self.config.symbols:
            data = loader.load_historical_data(symbol)
            data_sources[symbol] = str(data.attrs.get("data_source", "unknown"))
            if data_sources[symbol] == "synthetic_sample":
                fallback_count += 1
            source_error = str(data.attrs.get("source_error", "")).strip()
            if source_error:
                data_source_errors[symbol] = source_error
            data_by_symbol[symbol] = data
            for i, row in data.iterrows():
                timeline.append((utc_timestamp(row["timestamp"]), symbol, int(i)))

        for _, symbol, i in sorted(timeline, key=lambda item: (item[0], item[1])):
            data = data_by_symbol[symbol]
            history = data.iloc[: i + 1]
            row = history.iloc[-1]
            day = row["timestamp"].date()

            position = positions.get(symbol)
            if position is not None:
                closed = self._manage_open_position(position, row)
                if closed is not None:
                    equity += closed.pnl
                    daily_loss_by_day[day] = daily_loss_by_day.get(day, 0.0) + min(closed.pnl, 0.0)
                    trade_pnls.append(closed.pnl)
                    r_multiples.append(closed.r_multiple)
                    spread_costs.append(closed.estimated_spread_cost + closed.exit_spread_cost)
                    slippages.append(closed.estimated_entry_slippage + closed.exit_slippage)
                    if closed.exit_time is not None:
                        holding_hours.append((closed.exit_time - closed.entry_time).total_seconds() / 3600.0)
                    self._journal_trade(closed)
                    del positions[symbol]

            equity_points.append({"timestamp": row["timestamp"], "equity": equity})
            if i < 1 or symbol in positions:
                continue
            if not uses_aggressive_margin(self.config):
                if trades_by_day.get(day, 0) >= self.config.max_trades_per_day:
                    continue
                if abs(daily_loss_by_day.get(day, 0.0)) >= equity * self.config.max_daily_loss:
                    continue
                if len(positions) >= self.config.max_open_positions:
                    continue

            signal = strategy.generate_signal(symbol, history, self.config, self.config.spread_bps)
            if signal is None:
                continue

            reserved_capital = sum(position.open_qty * position.entry_price for position in positions.values())
            capital_budget = buying_power_budget(self.config, equity)
            available_capital = max(capital_budget - reserved_capital, 0.0)
            quote = {
                "mid": signal.entry,
                "bid": signal.entry,
                "ask": signal.entry,
                "spread_bps": self.config.spread_bps,
                "timestamp": signal.bar_time,
                "quote_source": "bar_model",
                "quote_age_seconds": 0.0,
                "fallback_used": False,
            }
            intent = build_trade_intent(self.config, self.config.mode, signal, quote, available_capital)
            if intent is None:
                rejected_trade_count += 1
                continue

            fill = broker.submit_entry(intent)
            if fill.status != "filled" or fill.filled_qty <= 0:
                rejected_trade_count += 1
                continue

            spread_cost, entry_slippage = estimate_costs(
                intent.price_reference,
                fill.filled_qty,
                self.config.spread_bps,
                self.config.slippage_bps,
            )
            positions[symbol] = BacktestPosition(
                symbol=symbol,
                strategy=self.config.strategy,
                side=signal.side,
                entry_time=row["timestamp"],
                entry_price=fill.avg_fill_price,
                signal_price=signal.entry,
                stop_price=signal.stop,
                initial_stop_price=signal.stop,
                qty=fill.filled_qty,
                open_qty=fill.filled_qty,
                initial_risk_per_unit=abs(fill.avg_fill_price - signal.stop),
                estimated_spread_cost=spread_cost,
                estimated_entry_slippage=entry_slippage,
            )
            trades_by_day[day] = trades_by_day.get(day, 0) + 1
            bucket = session_bucket(row["timestamp"], self.config)
            session_bucket_counts[bucket] = session_bucket_counts.get(bucket, 0) + 1
            quote_source_counts["bar_model"] = quote_source_counts.get("bar_model", 0) + 1
            max_buying_power_used = max(
                max_buying_power_used,
                sum(position.open_qty * position.entry_price for position in positions.values()),
            )

        for position in list(positions.values()):
            data = data_by_symbol[position.symbol]
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

        synthetic_data_used = any(source == "synthetic_sample" for source in data_sources.values())
        performance_warning = (
            "synthetic_data_used: performance is not reliable without local CSV or Alpaca historical data"
            if synthetic_data_used
            else ""
        )

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
            "strategy": self.config.strategy,
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
            "quote_source_counts": json.dumps(quote_source_counts, sort_keys=True),
            "fallback_count": fallback_count,
            "modeled_spread_bps": self.config.spread_bps,
            "modeled_slippage_bps": self.config.slippage_bps,
            "risk_profile": self.config.risk_profile,
            "capital_deployment_fraction": self.config.capital_deployment_fraction,
            "max_buying_power_used": round(max_buying_power_used, 2),
            "synthetic_data_used": synthetic_data_used,
            "performance_warning": performance_warning,
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
            if not position.partial_taken and row["high"] >= target_1:
                self._take_partial(position, float(row["close"]))
                return None
            if row["low"] <= position.stop_price:
                return self._close_position(position, row["timestamp"], float(row["close"]), "stop_loss")
            if row["high"] >= target_2 or row["close"] < row["ema_fast"]:
                return self._close_position(position, row["timestamp"], float(row["close"]), "target_or_structure")
        else:
            if not position.partial_taken and row["low"] <= target_1:
                self._take_partial(position, float(row["close"]))
                return None
            if row["high"] >= position.stop_price:
                return self._close_position(position, row["timestamp"], float(row["close"]), "stop_loss")
            if row["low"] <= target_2 or row["close"] > row["ema_fast"]:
                return self._close_position(position, row["timestamp"], float(row["close"]), "target_or_structure")
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
        fill = modeled_exit_fill_price(position.side, exit_price, self.config)
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
                "notes": merge_notes(
                    f"entry={round(position.entry_price, 6)}",
                    format_note_pairs(
                        quote_source="bar_model",
                        fallback_used=False,
                        quote_age_seconds=0,
                        spread_bps=self.config.spread_bps,
                        modeled_slippage_bps=self.config.slippage_bps,
                        execution_model="closed_bar_market",
                    ),
                ),
            }
        )


@dataclass(frozen=True)
class OptimizationWindow:
    name: str
    lookback_days: int
    historical_end: str


@dataclass(frozen=True)
class SymbolBasket:
    name: str
    symbols: tuple[str, ...]


class NullJournal:
    def log(self, row: dict[str, Any]) -> None:
        return


class HistoricalDataCache:
    def __init__(self):
        self.frames: dict[tuple[Any, ...], pd.DataFrame] = {}

    def loader_for(self, config: Config) -> "CachedHistoricalDataLoader":
        return CachedHistoricalDataLoader(config, self)

    def load(self, config: Config, symbol: str) -> pd.DataFrame:
        key = (
            config.asset_class,
            symbol,
            config.timeframe,
            config.lookback_days,
            config.historical_end,
            config.data_dir,
            config.strict_data,
            config.ema_fast,
            config.ema_slow,
            config.vwap_window,
            config.rvol_window,
            config.breakout_lookback,
        )
        if key not in self.frames:
            self.frames[key] = DataLoader(config).load_historical_data(symbol)
        return self.frames[key]


class CachedHistoricalDataLoader:
    def __init__(self, config: Config, cache: HistoricalDataCache):
        self.config = config
        self.cache = cache

    def load_historical_data(self, symbol: str) -> pd.DataFrame:
        return self.cache.load(self.config, symbol)


def unique_symbols(symbols: list[str] | tuple[str, ...]) -> tuple[str, ...]:
    seen: set[str] = set()
    ordered: list[str] = []
    for symbol in symbols:
        normalized = canonical_symbol(symbol)
        if normalized and normalized not in seen:
            ordered.append(normalized)
            seen.add(normalized)
    return tuple(ordered)


def default_symbol_baskets(base_symbols: list[str]) -> list[SymbolBasket]:
    base = unique_symbols(base_symbols)
    baskets = [
        SymbolBasket("baseline_default", base),
        SymbolBasket("default_minus_current_losers", tuple(symbol for symbol in base if symbol not in {"NET", "PLTR", "QCOM"})),
        SymbolBasket("default_minus_long_only_drags", tuple(symbol for symbol in base if symbol not in {"PLTR", "NFLX"})),
        SymbolBasket(
            "top_contributors_10",
            tuple(symbol for symbol in ["INTC", "ARM", "NOW", "TQQQ", "ORCL", "AMZN", "BABA", "NFLX", "CRWD", "PYPL"] if symbol in base),
        ),
        SymbolBasket(
            "top_contributors_7",
            tuple(symbol for symbol in ["INTC", "ARM", "NOW", "TQQQ", "ORCL", "AMZN", "BABA"] if symbol in base),
        ),
        SymbolBasket("old_aapl_amd_msft_control", ("AAPL", "AMD", "MSFT")),
    ]
    filtered: list[SymbolBasket] = []
    seen: set[tuple[str, ...]] = set()
    for basket in baskets:
        symbols = unique_symbols(basket.symbols)
        if not symbols or symbols in seen:
            continue
        filtered.append(SymbolBasket(basket.name, symbols))
        seen.add(symbols)
    return filtered


def default_optimization_windows(anchor: datetime | None = None) -> list[OptimizationWindow]:
    run_anchor = (anchor or datetime.now(UTC)).replace(microsecond=0)
    return [
        OptimizationWindow("current_30d", 30, run_anchor.isoformat()),
        OptimizationWindow("prior_30d", 30, (run_anchor - timedelta(days=30)).isoformat()),
        OptimizationWindow("combined_60d", 60, run_anchor.isoformat()),
    ]


def apply_optimizer_risk_profile(config: Config, risk_profile: str) -> None:
    config.risk_profile = risk_profile
    if risk_profile == "aggressive_margin":
        config.apply_risk_profile_defaults()
        return
    defaults = Config()
    config.risk_per_trade = defaults.risk_per_trade
    config.capital_deployment_fraction = defaults.capital_deployment_fraction
    config.max_daily_loss = defaults.max_daily_loss


def optimization_candidate_config(
    base_config: Config,
    basket: SymbolBasket,
    timeframe: str,
    allow_short: bool,
    risk_profile: str,
    window: OptimizationWindow,
) -> Config:
    candidate = replace(base_config)
    candidate.mode = "backtest"
    candidate.symbols = list(basket.symbols)
    candidate.timeframe = timeframe
    candidate.allow_short = allow_short
    candidate.lookback_days = window.lookback_days
    candidate.historical_end = window.historical_end
    candidate.strict_data = True
    apply_optimizer_risk_profile(candidate, risk_profile)
    return candidate


def guardrail_pass(result: dict[str, Any]) -> bool:
    return (
        not bool(result.get("synthetic_data_used", False))
        and float(result.get("max_drawdown", 0.0)) >= OPTIMIZATION_MAX_DRAWDOWN
        and int(result.get("trade_count", 0)) >= OPTIMIZATION_MIN_TRADE_COUNT
    )


def optimize_for_total_return(
    config: Config,
    symbol_baskets: list[SymbolBasket] | None = None,
    timeframes: list[str] | None = None,
    windows: list[OptimizationWindow] | None = None,
    short_modes: list[bool] | None = None,
    risk_profiles: list[str] | None = None,
) -> pd.DataFrame:
    baskets = symbol_baskets or default_symbol_baskets(config.symbols)
    candidate_timeframes = timeframes or OPTIMIZATION_TIMEFRAMES
    candidate_windows = windows or default_optimization_windows()
    candidate_short_modes = short_modes if short_modes is not None else [True, False]
    candidate_risk_profiles = risk_profiles or ["conservative", "aggressive_margin"]
    data_cache = HistoricalDataCache()
    rows: list[dict[str, Any]] = []

    for window in candidate_windows:
        for basket in baskets:
            for timeframe in candidate_timeframes:
                for allow_short in candidate_short_modes:
                    for risk_profile in candidate_risk_profiles:
                        candidate = optimization_candidate_config(
                            config,
                            basket,
                            timeframe,
                            allow_short,
                            risk_profile,
                            window,
                        )
                        result = BacktestEngine(
                            candidate,
                            NullJournal(),
                            data_loader_factory=data_cache.loader_for,
                        ).run()
                        passed = guardrail_pass(result)
                        rows.append(
                            {
                                "candidate_name": basket.name,
                                "symbols": " ".join(candidate.symbols),
                                "timeframe": timeframe,
                                "allow_short": allow_short,
                                "risk_profile": risk_profile,
                                "window_name": window.name,
                                "lookback_days": window.lookback_days,
                                "historical_end": window.historical_end,
                                "total_return": result["total_return"],
                                "max_drawdown": result["max_drawdown"],
                                "win_rate": result["win_rate"],
                                "average_r": result["average_r"],
                                "trade_count": result["trade_count"],
                                "rejected_trade_count": result["rejected_trade_count"],
                                "realized_reward_to_risk": result["realized_reward_to_risk"],
                                "synthetic_data_used": result["synthetic_data_used"],
                                "guardrail_pass": passed,
                                "high_risk": risk_profile == "aggressive_margin",
                                "ending_equity": result["ending_equity"],
                                "fallback_count": result["fallback_count"],
                                "max_buying_power_used": result["max_buying_power_used"],
                            }
                        )

    frame = pd.DataFrame(rows)
    if frame.empty:
        frame.to_csv(config.optimization_output_path, index=False)
        return frame

    baseline_mask = (
        (frame["candidate_name"] == "baseline_default")
        & (frame["timeframe"] == "15Min")
        & frame["allow_short"]
        & (frame["risk_profile"] == "conservative")
    )
    baseline_by_window = dict(zip(frame.loc[baseline_mask, "window_name"], frame.loc[baseline_mask, "total_return"]))
    frame["beats_baseline"] = frame.apply(
        lambda row: float(row["total_return"]) > float(baseline_by_window.get(row["window_name"], row["total_return"])),
        axis=1,
    )
    group_columns = ["candidate_name", "symbols", "timeframe", "allow_short", "risk_profile"]
    outperforming_counts = (
        frame[frame["guardrail_pass"] & frame["beats_baseline"]]
        .groupby(group_columns)["window_name"]
        .nunique()
        .to_dict()
    )
    frame["outperforming_windows"] = [
        int(outperforming_counts.get(tuple(row[column] for column in group_columns), 0))
        for _, row in frame.iterrows()
    ]
    frame["validation_pass"] = frame["outperforming_windows"] >= 2
    frame["recommended_default"] = frame["validation_pass"] & frame["guardrail_pass"] & ~frame["high_risk"]
    frame = frame.sort_values(
        [
            "recommended_default",
            "validation_pass",
            "guardrail_pass",
            "high_risk",
            "total_return",
            "win_rate",
            "average_r",
        ],
        ascending=[False, False, False, True, False, False, False],
    ).reset_index(drop=True)
    frame.to_csv(config.optimization_output_path, index=False)
    return frame


def print_optimization_results(frame: pd.DataFrame, output_path: str) -> None:
    if frame.empty:
        print("No optimization results available.")
        print(f"Wrote optimization results: {output_path}")
        return
    print(f"Wrote optimization results: {output_path}")
    print(
        frame[
            [
                "candidate_name",
                "window_name",
                "timeframe",
                "allow_short",
                "risk_profile",
                "total_return",
                "max_drawdown",
                "win_rate",
                "trade_count",
                "rejected_trade_count",
                "guardrail_pass",
                "validation_pass",
                "recommended_default",
                "high_risk",
            ]
        ]
        .head(20)
        .to_string(index=False)
    )


def compare_strategies(config: Config) -> pd.DataFrame:
    journal_path = Path(config.journal_path)
    journal = Journal(str(journal_path.with_name(f"{journal_path.stem}_momentum{journal_path.suffix or '.csv'}")), config.run_id)
    results = [BacktestEngine(config, journal).run()]
    frame = pd.DataFrame(results)
    if not frame.empty:
        frame = frame.sort_values(["total_return", "win_rate", "average_r"], ascending=[False, False, False]).reset_index(drop=True)
        frame.to_csv(config.compare_output_path, index=False)
    return frame


def print_backtest_summary(result: dict[str, Any]) -> None:
    print(f"Strategy: {result['strategy']}")
    print(f"Risk profile: {result.get('risk_profile', 'conservative')}")
    print(f"Data sources: {result.get('data_sources', '{}')}")
    data_source_errors = result.get("data_source_errors")
    if data_source_errors and data_source_errors != "{}":
        print(f"Data source errors: {data_source_errors}")
    performance_warning = result.get("performance_warning")
    if performance_warning:
        print(f"Performance warning: {performance_warning}")
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
    print(f"Quote sources: {result.get('quote_source_counts', '{}')}")
    print(f"Fallback count: {result.get('fallback_count', 0)}")
    print(f"Modeled spread bps: {float(result.get('modeled_spread_bps', 0.0)):.2f}")
    print(f"Modeled slippage bps: {float(result.get('modeled_slippage_bps', 0.0)):.2f}")
    print(f"Max buying power used: ${float(result.get('max_buying_power_used', 0.0)):.2f}")


def print_strategy_comparison(frame: pd.DataFrame) -> None:
    if frame.empty:
        print("No backtest results available.")
        return
    print(
        frame[
            [
                "strategy",
                "risk_profile",
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
                "quote_source_counts",
                "fallback_count",
                "modeled_spread_bps",
                "modeled_slippage_bps",
                "max_buying_power_used",
                "synthetic_data_used",
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
        self.broker_adapter: BrokerAdapter = AlpacaBrokerAdapter(self.executor)
        self.trading_client: TradingClient | None = None
        self.state_path = Path(self.config.state_path)
        self.last_market_data_time: pd.Timestamp | None = None
        self.last_trade_update_time: pd.Timestamp | None = None
        self.stream_started_at: pd.Timestamp | None = None
        self.current_session_day = utc_now().date()
        self.quote_cache: dict[str, StreamQuote] = {}
        self.short_entry_skip_notices: set[tuple[date, str, str]] = set()
        self.recovery_only_logged_symbols: set[tuple[date, str]] = set()
        self.last_account_snapshot: tuple[float, float] | None = None
        self._load_local_state()

    async def run(self) -> None:
        if not ALPACA_AVAILABLE:
            raise RuntimeError("alpaca-py is required for paper/live WebSocket execution.")
        self._validate_live_config()
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
                if is_connection_limit_error(exc):
                    raise RuntimeError(
                        "Alpaca stream connection limit exceeded. Another bot, scheduled task, "
                        "or dashboard is already using the same market-data connection. Stop the "
                        "other process or wait for Alpaca to release the connection, then start "
                        "scheduled_paper again."
                    ) from exc
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

        data_task = asyncio.create_task(self._run_data_stream(data_stream))
        trading_task = asyncio.create_task(self._run_trading_stream(trading_stream))
        watcher_task = asyncio.create_task(self._watch_stream_health())

        try:
            done, _pending = await asyncio.wait(
                {data_task, trading_task, watcher_task},
                return_when=asyncio.FIRST_COMPLETED,
            )
            for task in done:
                if task.cancelled():
                    continue
                exc = task.exception()
                if exc is not None:
                    raise exc
            raise RuntimeError("Stream task exited unexpectedly.")
        finally:
            await self._shutdown_streams(
                streams=(data_stream, trading_stream),
                stream_tasks=(data_task, trading_task),
                watcher_task=watcher_task,
            )

    async def _run_data_stream(self, data_stream: Any) -> None:
        data_stream._loop = asyncio.get_running_loop()
        data_stream._should_run = True
        data_stream._running = False
        try:
            await data_stream._start_ws()
            await data_stream._send_subscribe_msg()
            data_stream._running = True
            await data_stream._consume()
        finally:
            data_stream._running = False
            await self._close_stream(data_stream)

    async def _run_trading_stream(self, trading_stream: Any) -> None:
        trading_stream._loop = asyncio.get_running_loop()
        trading_stream._should_run = True
        trading_stream._running = False
        try:
            await trading_stream._start_ws()
            trading_stream._running = True
            await trading_stream._consume()
        finally:
            trading_stream._running = False
            await self._close_stream(trading_stream)

    async def _shutdown_streams(
        self,
        streams: tuple[Any, ...],
        stream_tasks: tuple[asyncio.Task[Any], ...],
        watcher_task: asyncio.Task[Any],
    ) -> None:
        watcher_task.cancel()
        await asyncio.gather(watcher_task, return_exceptions=True)
        for stream in streams:
            await self._call_stream_method(stream, "stop_ws")
        try:
            await asyncio.wait_for(
                asyncio.gather(*stream_tasks, return_exceptions=True),
                timeout=10.0,
            )
        except asyncio.TimeoutError:
            for task in stream_tasks:
                task.cancel()
            await asyncio.gather(*stream_tasks, return_exceptions=True)
        finally:
            for stream in streams:
                await self._close_stream(stream)

    async def _close_stream(self, stream: Any) -> None:
        await self._call_stream_method(stream, "close")

    async def _call_stream_method(self, stream: Any, method_name: str) -> None:
        method = getattr(stream, method_name, None)
        if not callable(method):
            return
        result = method()
        if asyncio.iscoroutine(result):
            await result

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
        if self.broker_state.uncertain and not self._safe_reconcile():
            return
        quote = self._stream_quote_snapshot_or_fallback(symbol, signal.entry, signal.bar_time, "fallback_signal")
        intent = build_trade_intent(self.config, self.config.mode, signal, quote, self._capital_base())
        if intent is None:
            return
        if intent.client_order_id in self.state_machine.orders and (
            is_active_order_status(self.state_machine.orders[intent.client_order_id].status)
            or self.state_machine.orders[intent.client_order_id].status == "filled"
        ):
            return
        if not self._entry_risk_checks(symbol, signal.side, intent.price_reference, intent.qty, quote):
            return
        order = LiveOrder(
            client_order_id=intent.client_order_id,
            symbol=symbol,
            strategy=signal.strategy,
            side=signal.side,
            intended_qty=intent.qty,
            intended_price=intent.price_reference,
            signal_price=signal.entry,
            stop_price=signal.stop,
            entry_exit="entry",
            expected_spread_bps=float(quote["spread_bps"]),
            estimated_spread_cost=estimate_costs(intent.price_reference, intent.qty, float(quote["spread_bps"]), 0.0)[0],
            session_bucket=session_bucket(signal.bar_time, self.config),
            bar_time=signal.bar_time,
            quote_source=str(quote["quote_source"]),
            quote_age_seconds=float(quote["quote_age_seconds"]),
            fallback_used=bool(quote["fallback_used"]),
            spread_bps_at_submission=float(quote["spread_bps"]),
        )
        self.state_machine.register_intent(order)
        self._persist_local_state()
        await self._submit_order_with_retry(
            order=order,
            submitter=lambda: self._broker_adapter().submit_entry(intent),
        )

    async def flatten_for_session_close(self, reason: str = "scheduled_close") -> None:
        self.broker_state.kill_switch_active = True
        if not self._safe_reconcile():
            return
        self._cancel_open_orders()
        if not self._safe_reconcile():
            return
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
        if not self._safe_reconcile():
            return
        for order in list(self.state_machine.orders.values()):
            if not is_active_order_status(order.status):
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
            return
        history = self.history[symbol]
        row = history.iloc[-1]
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
        qty = normalize_qty(self.config.asset_class, qty)
        if qty <= 0:
            return
        if self.broker_state.uncertain and not self._safe_reconcile():
            return
        available_qty = self._available_exit_qty(symbol)
        qty = normalize_qty(self.config.asset_class, min(qty, available_qty))
        if qty <= 0:
            return
        client_order_id = deterministic_client_order_id(
            execution_mode(self.config.mode),
            position.strategy,
            symbol,
            exit_side,
            f"exit_{reason}",
            bar_time,
        )
        if client_order_id in self.state_machine.orders and (
            is_active_order_status(self.state_machine.orders[client_order_id].status)
            or self.state_machine.orders[client_order_id].status == "filled"
        ):
            return
        fallback_price = position.avg_entry_price
        if symbol in self.history and not self.history[symbol].empty:
            fallback_price = float(self.history[symbol].iloc[-1]["close"])
        quote = self._stream_quote_snapshot_or_fallback(symbol, fallback_price, bar_time, "fallback_bar")
        intended_price = float(quote["mid"] or position.avg_entry_price)
        intent = ExitIntent(
            symbol=symbol,
            side=exit_side,
            qty=qty,
            reason=reason,
            price_reference=intended_price,
            quote=quote,
            client_order_id=client_order_id,
            bar_time=bar_time,
        )
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
            quote_source=str(quote["quote_source"]),
            quote_age_seconds=float(quote["quote_age_seconds"]),
            fallback_used=bool(quote["fallback_used"]),
            spread_bps_at_submission=float(quote["spread_bps"]),
        )
        self.state_machine.register_intent(order)
        self._persist_local_state()
        await self._submit_order_with_retry(
            order=order,
            submitter=lambda: self._broker_adapter().submit_exit(intent, position),
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
            "pending_new": "pending_new",
            "accepted": "accepted",
            "accepted_for_bidding": "accepted_for_bidding",
            "partially_filled": "partially_filled",
            "fill": "filled",
            "filled": "filled",
            "done_for_day": "done_for_day",
            "canceled": "canceled",
            "cancelled": "canceled",
            "expired": "expired",
            "replaced": "replaced",
            "pending_cancel": "pending_cancel",
            "pending_replace": "pending_replace",
            "stopped": "stopped",
            "suspended": "suspended",
            "calculated": "calculated",
            "rejected": "rejected",
        }
        status = normalize_order_status(status_map.get(str(event).lower(), getattr(order, "status", "accepted")))
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
        self._persist_local_state()

    def _broker_adapter(self) -> BrokerAdapter:
        if isinstance(self.broker_adapter, AlpacaBrokerAdapter) and self.broker_adapter.executor is not self.executor:
            self.broker_adapter = AlpacaBrokerAdapter(self.executor)
        return self.broker_adapter

    def _rebuild_position_from_fills(self, order: LiveOrder) -> None:
        fill_delta = order.last_fill_delta
        if fill_delta <= 0:
            return
        if order.entry_exit == "entry":
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
                    last_fill_time=order.last_update_time,
                    available_qty=fill_delta,
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
                existing.available_qty = total_qty
                existing.initial_risk_per_unit = abs(existing.avg_entry_price - existing.stop_price) if existing.stop_price > 0 else existing.initial_risk_per_unit
                existing.last_fill_time = order.last_update_time
                existing.strategy = order.strategy
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
                if position.available_qty is not None:
                    position.available_qty = normalize_qty(self.config.asset_class, max(position.available_qty - fill_delta, 0.0))
                else:
                    position.available_qty = remaining
                position.last_fill_time = order.last_update_time
            if "partial" in order.client_order_id:
                position.partial_taken = True

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
        persisted_orders = local_state.get("orders", {})

        rebuilt_orders: list[LiveOrder] = []
        rebuilt_order_ids: set[str] = set()
        open_orders_by_symbol: dict[str, list[str]] = {}
        reserved_notional_by_symbol: dict[str, float] = {}
        reserved_exit_qty_by_symbol: dict[str, float] = {}
        for raw in open_orders_response.json():
            client_order_id = raw.get("client_order_id") or raw.get("id", "")
            symbol = raw.get("symbol", "")
            normalized_id = client_order_id.replace("_", "").lower()
            persisted_order = local_state.get("orders", {}).get(client_order_id, {})
            stop_price = float(persisted_order.get("stop_price") or 0.0)
            entry_exit = persisted_order.get("entry_exit") or ("exit" if "exit" in normalized_id else "entry")
            intended_price = float(raw.get("limit_price") or persisted_order.get("intended_price") or 0.0)
            order = LiveOrder(
                client_order_id=client_order_id,
                symbol=symbol,
                strategy=str(persisted_order.get("strategy") or self.config.strategy),
                side=raw.get("side", ""),
                intended_qty=float(raw.get("qty") or 0.0),
                intended_price=intended_price,
                signal_price=float(persisted_order.get("signal_price") or intended_price or 0.0),
                stop_price=stop_price,
                status=normalize_order_status(raw.get("status", "accepted")),
                filled_qty=float(raw.get("filled_qty") or 0.0),
                avg_fill_price=float(raw.get("filled_avg_price") or 0.0),
                broker_order_id=raw.get("id", ""),
                entry_exit=entry_exit,
                last_processed_fill_qty=float(persisted_order.get("last_processed_fill_qty") or persisted_order.get("filled_qty") or 0.0),
                expected_spread_bps=float(persisted_order.get("expected_spread_bps") or 0.0),
                estimated_spread_cost=float(persisted_order.get("estimated_spread_cost") or 0.0),
                session_bucket=str(persisted_order.get("session_bucket") or ""),
                bar_time=utc_timestamp(persisted_order.get("bar_time")) if persisted_order.get("bar_time") else None,
                quote_source=str(persisted_order.get("quote_source") or ""),
                quote_age_seconds=(
                    float(persisted_order.get("quote_age_seconds"))
                    if persisted_order.get("quote_age_seconds") not in (None, "")
                    else None
                ),
                fallback_used=parse_bool(persisted_order.get("fallback_used"), False),
                spread_bps_at_submission=float(persisted_order.get("spread_bps_at_submission") or persisted_order.get("expected_spread_bps") or 0.0),
            )
            rebuilt_orders.append(order)
            rebuilt_order_ids.add(client_order_id)
            open_orders_by_symbol.setdefault(symbol, []).append(client_order_id)
            if order.entry_exit == "exit":
                reserved_exit_qty_by_symbol[symbol] = reserved_exit_qty_by_symbol.get(symbol, 0.0) + order.remaining_qty
            else:
                reserved_notional_by_symbol[symbol] = reserved_notional_by_symbol.get(symbol, 0.0) + (
                    order.remaining_qty * max(order.intended_price, 0.0)
                )
        for client_order_id, payload in persisted_orders.items():
            if client_order_id in rebuilt_order_ids:
                continue
            order = self._order_from_state_payload(payload)
            if order is not None:
                rebuilt_orders.append(order)
                rebuilt_order_ids.add(client_order_id)
        self.state_machine.replace_open_orders(rebuilt_orders)

        rebuilt_positions: dict[str, LivePosition] = {}
        recovery_only_symbols: set[str] = set()
        daily_unrealized_pnl = 0.0
        for raw in positions_response.json():
            qty = abs(float(raw.get("qty") or 0.0))
            if qty <= 0:
                continue
            side = "buy" if float(raw.get("qty") or 0.0) > 0 else "sell"
            symbol = raw["symbol"]
            available_raw = raw.get("qty_available")
            available_qty = abs(float(available_raw if available_raw not in (None, "") else qty))
            persisted_position = local_state.get("positions", {}).get(symbol, {})
            avg_entry_price = float(raw.get("avg_entry_price") or 0.0)
            stop_price, initial_risk, strategy_name, entry_time = self._position_risk_metadata(
                symbol,
                avg_entry_price,
                persisted_position,
                rebuilt_orders,
            )
            recovery_only = stop_price <= 0 or initial_risk <= 0
            position = LivePosition(
                symbol=symbol,
                strategy=str(strategy_name),
                side=side,
                qty=qty,
                avg_entry_price=avg_entry_price,
                stop_price=stop_price,
                initial_risk_per_unit=initial_risk,
                entry_time=entry_time,
                partial_taken=bool(persisted_position.get("partial_taken", False)),
                realized_pnl=float(persisted_position.get("realized_pnl") or 0.0),
                recovery_only=recovery_only,
                available_qty=normalize_qty(self.config.asset_class, available_qty),
            )
            rebuilt_positions[raw["symbol"]] = position
            if recovery_only:
                self._log_recovery_only_position(position)
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
        self.broker_state.shorting_enabled = parse_bool(account_payload.get("shorting_enabled"), False)
        combined_loss = -(self.broker_state.daily_realized_pnl + self.broker_state.daily_unrealized_pnl)
        capital_base = self.broker_state.account_equity or self.config.starting_capital
        self.broker_state.kill_switch_active = combined_loss >= (capital_base * self.config.max_daily_loss)
        self.broker_state.uncertain = False
        self._log_account_snapshot_if_changed()
        self._persist_local_state()

    def _validate_live_config(self) -> None:
        trade_mode = execution_mode(self.config.mode)
        if trade_mode not in {"paper", "live"}:
            raise ValueError("Trading mode must be paper or live.")
        if trade_mode == "live" and os.getenv("ALLOW_LIVE", "").lower() != "true":
            raise ValueError("Live mode is blocked unless ALLOW_LIVE=true is set in the environment.")
        if not (self.config.alpaca_api_key and self.config.alpaca_secret_key):
            raise ValueError("Missing Alpaca credentials.")
        expected_url = "https://paper-api.alpaca.markets" if trade_mode == "paper" else "https://api.alpaca.markets"
        configured_url = normalize_base_url(self.config.alpaca_base_url)
        if configured_url != expected_url:
            raise ValueError(f"{trade_mode} mode requires ALPACA_BASE_URL={expected_url}. Current value: {self.config.alpaca_base_url}")

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
        last_retryable_http_error = ""
        while attempts <= self.config.max_submit_retries:
            retry_exc: Any = None
            try:
                response = submitter()
                if isinstance(response, FillEvent):
                    self._apply_fill_event(order, response)
                else:
                    self._apply_broker_order_snapshot(order, response)
                return
            except requests.HTTPError as exc:
                retry_exc = exc
                status_code = exc.response.status_code if exc.response is not None else 0
                formatted_error = format_http_error(exc)
                if 400 <= status_code < 500 and status_code != 429:
                    existing = self._lookup_broker_order(order.client_order_id)
                    if existing is not None:
                        self._apply_broker_order_snapshot(order, existing, rejection_reason=formatted_error)
                        return
                    self.state_machine.apply_update(
                        client_order_id=order.client_order_id,
                        status="rejected",
                        filled_qty=order.filled_qty,
                        avg_fill_price=order.avg_fill_price,
                        rejection_reason=formatted_error,
                        timestamp=utc_now(),
                    )
                    self._set_cooldown_for_rejection(order.symbol, formatted_error)
                    self._persist_local_state()
                    return
                last_retryable_http_error = formatted_error
                attempts += 1
            except (requests.Timeout, requests.ConnectionError):
                retry_exc = None
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
                    self._apply_broker_order_snapshot(order, existing)
                    return
                attempts += 1
            except Exception as exc:
                retry_exc = None
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
                await asyncio.sleep(self._submit_retry_delay(attempts, retry_exc))
        self.state_machine.apply_update(
            client_order_id=order.client_order_id,
            status="rejected",
            filled_qty=order.filled_qty,
            avg_fill_price=order.avg_fill_price,
            rejection_reason=(
                f"submit_retry_exhausted last_error={last_retryable_http_error}"
                if last_retryable_http_error
                else "submit_retry_exhausted"
            ),
            timestamp=utc_now(),
        )
        self._persist_local_state()

    def _submit_retry_delay(self, attempt: int, exc: Any = None) -> float:
        retry_after = None
        response = getattr(exc, "response", None)
        if response is not None:
            raw_retry_after = response.headers.get("Retry-After") or response.headers.get("retry-after")
            try:
                retry_after = float(raw_retry_after) if raw_retry_after else None
            except ValueError:
                retry_after = None
        backoff = min(self.config.reconnect_backoff_seconds * max(attempt, 1), self.config.reconnect_backoff_max_seconds)
        return max(retry_after or 0.0, backoff)

    def _set_cooldown_for_rejection(self, symbol: str, rejection_reason: str) -> None:
        reason = rejection_reason.lower()
        if any(token in reason for token in ("shorting disabled", "insufficient buying power", "cannot be shorted")):
            self._set_symbol_cooldown(symbol, self.config.cooldown_minutes_after_rejection)

    def _apply_fill_event(self, order: LiveOrder, fill: FillEvent) -> LiveOrder:
        tracked = self.state_machine.apply_update(
            client_order_id=order.client_order_id,
            status=fill.status,
            filled_qty=fill.filled_qty,
            avg_fill_price=fill.avg_fill_price,
            broker_order_id=fill.broker_order_id,
            rejection_reason=fill.rejection_reason,
            timestamp=fill.timestamp,
        )
        if fill.status == "rejected" and fill.rejection_reason:
            self._set_cooldown_for_rejection(order.symbol, fill.rejection_reason)
        self._rebuild_position_from_fills(tracked)
        self._persist_local_state()
        return tracked

    def _apply_broker_order_snapshot(self, order: LiveOrder, snapshot: dict[str, Any], rejection_reason: str = "") -> LiveOrder:
        fill = fill_event_from_snapshot(order.client_order_id, snapshot, rejection_reason)
        return self._apply_fill_event(order, fill)

    def _lookup_broker_order(self, client_order_id: str) -> dict[str, Any] | None:
        headers = {
            "APCA-API-KEY-ID": self.config.alpaca_api_key,
            "APCA-API-SECRET-KEY": self.config.alpaca_secret_key,
        }
        try:
            response = requests.get(
                f"{self.config.alpaca_base_url}/v2/orders:by_client_order_id",
                headers=headers,
                params={"client_order_id": client_order_id},
                timeout=15,
            )
            if response.status_code == 200:
                return response.json()
        except Exception:
            pass
        try:
            after = (datetime.now(UTC) - timedelta(minutes=self.config.recent_order_lookup_minutes)).isoformat()
            response = requests.get(
                f"{self.config.alpaca_base_url}/v2/orders",
                headers=headers,
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

    def _entry_risk_checks(self, symbol: str, side: str, price_reference: float, qty: float, quote: dict[str, Any]) -> bool:
        if not self._can_trade_symbol(symbol):
            return False
        if side not in {"buy", "sell"}:
            return False
        if price_reference <= 0 or qty <= 0:
            return False
        if self.config.asset_class not in {"equity", "crypto"}:
            return False
        return True

    def _skip_short_entry(self, symbol: str, reason: str, detail: str) -> bool:
        key = (self.current_session_day, symbol, reason)
        if key not in self.short_entry_skip_notices:
            self.short_entry_skip_notices.add(key)
            print(f"Short entry skipped for {symbol}: {reason} ({detail})")
        return False

    def _active_entry_layers(self, symbol: str, strategy_name: str) -> int:
        layers = 0
        position = self.broker_state.positions.get(symbol)
        if position is not None and position.strategy == strategy_name:
            layers += 1
        for order in self.state_machine.orders.values():
            if order.symbol != symbol or order.strategy != strategy_name or order.entry_exit != "entry":
                continue
            if is_active_order_status(order.status):
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
        local_available = max(position.qty - reserved, 0.0)
        broker_available = position.available_qty if position.available_qty is not None else position.qty
        return normalize_qty(self.config.asset_class, min(local_available, max(broker_available, 0.0)))

    def _capital_base(self) -> float:
        if execution_mode(self.config.mode) in {"paper", "live"}:
            equity = self.broker_state.account_equity or self.config.starting_capital
            return buying_power_budget(self.config, equity, self.broker_state.buying_power)
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
            "quote_source": "stream",
            "quote_age_seconds": quote_age_seconds(quote.timestamp),
            "fallback_used": False,
        }

    def _stream_quote_snapshot_or_fallback(
        self,
        symbol: str,
        fallback_price: float,
        fallback_time: pd.Timestamp,
        fallback_source: str = "fallback_signal",
    ) -> dict[str, Any]:
        quote = self._stream_quote_snapshot(symbol)
        if quote is not None:
            return quote
        fallback = max(float(fallback_price or 0.0), 0.0)
        return {
            "mid": fallback,
            "bid": fallback,
            "ask": fallback,
            "spread_bps": 0.0,
            "timestamp": utc_timestamp(fallback_time),
            "quote_source": fallback_source,
            "quote_age_seconds": 0.0,
            "fallback_used": True,
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
            if not is_active_order_status(order.status):
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
        return bool(symbol and symbol.strip())

    def _has_active_orders(self) -> bool:
        return any(is_active_order_status(order.status) for order in self.state_machine.orders.values())

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
            self.recovery_only_logged_symbols.clear()
            self._persist_local_state()

    def _safe_reconcile(self) -> bool:
        try:
            self.reconcile_state()
            self._persist_local_state()
            return True
        except Exception as exc:
            self.broker_state.uncertain = True
            print(f"Reconciliation failed: {exc}")
            return False

    def _read_state_file(self) -> dict[str, Any]:
        if not self.state_path.exists():
            return {}
        try:
            return json.loads(self.state_path.read_text())
        except Exception:
            return {}

    def _order_from_state_payload(self, payload: dict[str, Any]) -> LiveOrder | None:
        try:
            filled_qty = float(payload.get("filled_qty") or 0.0)
            last_processed = float(payload.get("last_processed_fill_qty", filled_qty) or 0.0)
            return LiveOrder(
                client_order_id=payload["client_order_id"],
                symbol=payload["symbol"],
                strategy=payload["strategy"],
                side=payload["side"],
                intended_qty=float(payload["intended_qty"]),
                intended_price=float(payload["intended_price"]),
                signal_price=float(payload["signal_price"]),
                stop_price=float(payload.get("stop_price") or 0.0),
                status=normalize_order_status(payload.get("status") or "new"),
                filled_qty=filled_qty,
                avg_fill_price=float(payload.get("avg_fill_price") or 0.0),
                last_update_time=utc_timestamp(payload.get("last_update_time")) if payload.get("last_update_time") else utc_now(),
                rejection_reason=str(payload.get("rejection_reason") or ""),
                broker_order_id=str(payload.get("broker_order_id") or ""),
                entry_exit=str(payload.get("entry_exit") or "entry"),
                expected_spread_bps=float(payload.get("expected_spread_bps") or 0.0),
                estimated_spread_cost=float(payload.get("estimated_spread_cost") or 0.0),
                session_bucket=str(payload.get("session_bucket") or ""),
                bar_time=utc_timestamp(payload.get("bar_time")) if payload.get("bar_time") else None,
                last_processed_fill_qty=last_processed,
                quote_source=str(payload.get("quote_source") or ""),
                quote_age_seconds=float(payload.get("quote_age_seconds")) if payload.get("quote_age_seconds") not in (None, "") else None,
                fallback_used=parse_bool(payload.get("fallback_used"), False),
                spread_bps_at_submission=float(payload.get("spread_bps_at_submission") or payload.get("expected_spread_bps") or 0.0),
            )
        except Exception:
            return None

    def _position_risk_metadata(
        self,
        symbol: str,
        avg_entry_price: float,
        persisted_position: dict[str, Any],
        known_orders: list[LiveOrder],
    ) -> tuple[float, float, str, pd.Timestamp]:
        stop_price = float(persisted_position.get("stop_price") or 0.0)
        initial_risk = float(persisted_position.get("initial_risk_per_unit") or 0.0)
        strategy_name = str(persisted_position.get("strategy") or self.config.strategy)
        entry_time = utc_timestamp(persisted_position.get("entry_time")) if persisted_position.get("entry_time") else utc_now()
        if stop_price > 0 and initial_risk <= 0 and avg_entry_price > 0:
            initial_risk = abs(avg_entry_price - stop_price)
        if stop_price > 0 and initial_risk > 0:
            return stop_price, initial_risk, strategy_name, entry_time

        candidates = [
            order
            for order in known_orders
            if order.symbol == symbol and order.entry_exit == "entry" and order.stop_price > 0
        ]
        candidates.sort(key=lambda order: order.last_update_time, reverse=True)
        for order in candidates:
            entry_reference = order.avg_fill_price or order.intended_price or order.signal_price or avg_entry_price
            derived_risk = abs(entry_reference - order.stop_price)
            if derived_risk > 0:
                return order.stop_price, derived_risk, order.strategy, order.last_update_time
        return stop_price, initial_risk, strategy_name, entry_time

    def _log_recovery_only_position(self, position: LivePosition) -> None:
        key = (self.current_session_day, position.symbol)
        if key in self.recovery_only_logged_symbols:
            return
        self.recovery_only_logged_symbols.add(key)
        self.journal.log(
            {
                "timestamp": utc_now().isoformat(),
                "symbol": position.symbol,
                "strategy": position.strategy,
                "side": position.side,
                "mode": self.config.mode,
                "event": "recovery_only",
                "entry_exit": "state",
                "stop": round(position.stop_price, 6) if position.stop_price else "",
                "size": round(position.qty, 6),
                "signal_price": "",
                "intended_price": "",
                "fill_price": round(position.avg_entry_price, 6) if position.avg_entry_price else "",
                "slippage": "",
                "spread_cost": "",
                "execution_cost": "",
                "pnl": "",
                "r_multiple": "",
                "session_bucket": session_bucket(utc_now(), self.config),
                "order_status": "",
                "filled_qty": "",
                "remaining_qty": "",
                "rejection_reason": "",
                "notes": format_note_pairs(
                    position_state="recovery_only",
                    reason="missing_stop_or_risk_metadata",
                    stop_price=position.stop_price,
                    initial_risk_per_unit=position.initial_risk_per_unit,
                ),
            }
        )

    def _log_account_snapshot_if_changed(self) -> None:
        snapshot = (
            round(self.broker_state.daily_realized_pnl, 6),
            round(self.broker_state.daily_unrealized_pnl, 6),
        )
        if snapshot == self.last_account_snapshot:
            return
        self.last_account_snapshot = snapshot
        self.journal.log(
            {
                "timestamp": utc_now().isoformat(),
                "symbol": "",
                "strategy": self.config.strategy,
                "side": "",
                "mode": self.config.mode,
                "event": "account_snapshot",
                "entry_exit": "account",
                "stop": "",
                "size": "",
                "signal_price": "",
                "intended_price": "",
                "fill_price": "",
                "slippage": "",
                "spread_cost": "",
                "execution_cost": "",
                "pnl": "",
                "r_multiple": "",
                "session_bucket": session_bucket(utc_now(), self.config),
                "client_order_id": "",
                "order_status": "",
                "filled_qty": "",
                "remaining_qty": "",
                "rejection_reason": "",
                "notes": format_note_pairs(
                    daily_realized_pnl=self.broker_state.daily_realized_pnl,
                    daily_unrealized_pnl=self.broker_state.daily_unrealized_pnl,
                    account_equity=self.broker_state.account_equity,
                    buying_power=self.broker_state.buying_power,
                ),
            }
        )

    def _load_local_state(self) -> None:
        state = self._read_state_file()
        loaded_orders: list[LiveOrder] = []
        for payload in state.get("orders", {}).values():
            order = self._order_from_state_payload(payload)
            if order is not None:
                loaded_orders.append(order)
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
                "last_processed_fill_qty": order.last_processed_fill_qty,
                "avg_fill_price": order.avg_fill_price,
                "last_update_time": order.last_update_time.isoformat(),
                "rejection_reason": order.rejection_reason,
                "broker_order_id": order.broker_order_id,
                "entry_exit": order.entry_exit,
                "expected_spread_bps": order.expected_spread_bps,
                "estimated_spread_cost": order.estimated_spread_cost,
                "session_bucket": order.session_bucket,
                "bar_time": order.bar_time.isoformat() if order.bar_time is not None else "",
                "quote_source": order.quote_source,
                "quote_age_seconds": order.quote_age_seconds,
                "fallback_used": order.fallback_used,
                "spread_bps_at_submission": order.spread_bps_at_submission,
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
                "partial_taken": position.partial_taken,
                "realized_pnl": position.realized_pnl,
                "recovery_only": position.recovery_only,
                "available_qty": position.available_qty,
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


class SingleInstanceLock:
    def __init__(self, path: Path, mode: str):
        self.path = path
        self.mode = mode
        self.handle: Any | None = None

    def __enter__(self) -> "SingleInstanceLock":
        self.acquire()
        return self

    def __exit__(self, exc_type: Any, exc: Any, traceback: Any) -> None:
        self.release()

    def acquire(self) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        handle = self.path.open("a+")
        try:
            if os.name == "nt":
                import msvcrt

                handle.seek(0)
                msvcrt.locking(handle.fileno(), msvcrt.LK_NBLCK, 1)
            else:
                import fcntl

                fcntl.lockf(handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except OSError as exc:
            handle.close()
            raise RuntimeError(
                f"{self.mode} is already running. Stop the existing process before starting another one. "
                f"Lock file: {self.path}"
            ) from exc
        handle.seek(0)
        handle.truncate()
        handle.write(f"pid={os.getpid()}\nmode={self.mode}\nstarted_at={utc_now().isoformat()}\n")
        handle.flush()
        self.handle = handle

    def release(self) -> None:
        if self.handle is None:
            return
        try:
            if os.name == "nt":
                import msvcrt

                self.handle.seek(0)
                msvcrt.locking(self.handle.fileno(), msvcrt.LK_UNLCK, 1)
            else:
                import fcntl

                fcntl.lockf(self.handle.fileno(), fcntl.LOCK_UN)
        except OSError:
            pass
        finally:
            self.handle.close()
            self.handle = None
            try:
                self.path.unlink()
            except OSError:
                pass


class ScheduledSessionRunner:
    def __init__(self, config: Config, journal: Journal):
        self.config = config
        self.journal = journal

    async def run(self) -> None:
        with SingleInstanceLock(self._lock_path(), self.config.mode):
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

    def _lock_path(self) -> Path:
        state_path = Path(self.config.state_path)
        return state_path.with_name(f"{state_path.stem}.{self.config.mode}.lock")

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

    def run(self, compare: bool = False) -> None:
        if compare:
            frame = compare_strategies(self.config)
            print_strategy_comparison(frame)
            return
        if self.config.mode == "optimize":
            frame = optimize_for_total_return(self.config)
            print_optimization_results(frame, self.config.optimization_output_path)
            return
        if execution_mode(self.config.mode) == "backtest":
            journal = Journal(self.config.journal_path, self.config.run_id)
            result = BacktestEngine(self.config, journal).run()
            print_backtest_summary(result)
            return
        journal = Journal(self.config.journal_path, self.config.run_id)
        if is_scheduled_mode(self.config.mode):
            self._run_async(ScheduledSessionRunner(self.config, journal).run())
            return
        self._run_async(StreamExecutionEngine(self.config, journal).run())


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Lightweight trading bot for small accounts.")
    parser.add_argument("--mode", choices=["backtest", "optimize", "paper", "live", "scheduled_paper", "scheduled_live"], default="backtest")
    parser.add_argument("--asset-class", choices=["equity", "crypto"], default=None)
    parser.add_argument("--compare", action="store_true", help="Run a momentum backtest summary and write strategy_comparison.csv.")
    parser.add_argument("--symbols", nargs="+", help="Override watchlist symbols.")
    parser.add_argument("--capital", type=float, help="Override starting capital.")
    parser.add_argument("--timeframe", default=None, help="Override timeframe such as 5Min or 15Min.")
    parser.add_argument("--lookback-days", type=int, default=None, help="Override historical lookback days.")
    parser.add_argument("--historical-end", default=None, help="Override historical end timestamp for backtests.")
    parser.add_argument("--risk-profile", choices=sorted(RISK_PROFILES), default=None)
    parser.add_argument("--capital-deployment-fraction", type=float, default=None)
    parser.add_argument("--max-daily-loss", type=float, default=None)
    parser.add_argument("--journal-path", default=None)
    parser.add_argument("--state-path", default=None)
    parser.add_argument("--optimization-output-path", default=None)
    parser.add_argument("--strict-data", action="store_true", help="Refuse synthetic sample data in backtests.")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    config = Config.from_env()
    config.mode = args.mode
    if args.asset_class:
        config.asset_class = args.asset_class
    if args.risk_profile:
        config.risk_profile = args.risk_profile
        config.apply_risk_profile_defaults()
    if args.symbols:
        config.symbols = args.symbols
    if args.capital:
        config.starting_capital = args.capital
    if args.timeframe:
        config.timeframe = args.timeframe
    if args.lookback_days is not None:
        config.lookback_days = args.lookback_days
    if args.historical_end:
        config.historical_end = args.historical_end
    if args.capital_deployment_fraction is not None:
        config.capital_deployment_fraction = args.capital_deployment_fraction
    if args.max_daily_loss is not None:
        config.max_daily_loss = args.max_daily_loss
    if args.journal_path:
        config.journal_path = args.journal_path
    if args.state_path:
        config.state_path = args.state_path
    if args.optimization_output_path:
        config.optimization_output_path = args.optimization_output_path
    if args.strict_data:
        config.strict_data = True
    TradingBot(config).run(compare=args.compare)


if __name__ == "__main__":
    main()
