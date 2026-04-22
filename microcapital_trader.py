#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import math
import os
from dataclasses import dataclass, field, replace
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any, Iterable

import pandas as pd
import requests
from dotenv import load_dotenv


@dataclass
class Config:
    mode: str = "backtest"
    strategy: str = "momentum"
    compare_strategies: list[str] = field(default_factory=lambda: ["momentum", "mean_reversion"])
    symbols: list[str] = field(default_factory=lambda: ["AAPL", "MSFT", "AMD"])
    timeframe: str = "15Min"
    lookback_days: int = 30
    starting_capital: float = 1_000.0
    risk_per_trade: float = 0.005
    max_daily_loss: float = 0.02
    max_trades_per_day: int = 4
    slippage_bps: float = 2.0
    fee_per_trade: float = 0.0
    allow_short: bool = True
    breakout_lookback: int = 20
    ema_fast: int = 9
    ema_slow: int = 20
    vwap_window: int = 30
    rvol_window: int = 20
    rvol_threshold: float = 1.5
    candle_body_threshold: float = 0.6
    mean_reversion_window: int = 20
    mean_reversion_zscore: float = 1.2
    stop_buffer_pct: float = 0.0025
    partial_take_profit_r: float = 1.0
    final_take_profit_r: float = 2.0
    journal_path: str = "trade_journal.csv"
    data_dir: str = "data"
    alpaca_api_key: str = ""
    alpaca_secret_key: str = ""
    alpaca_base_url: str = "https://paper-api.alpaca.markets"
    alpaca_data_url: str = "https://data.alpaca.markets"

    @classmethod
    def from_env(cls) -> "Config":
        load_dotenv()
        return cls(
            alpaca_api_key=os.getenv("ALPACA_API_KEY", ""),
            alpaca_secret_key=os.getenv("ALPACA_SECRET_KEY", ""),
            alpaca_base_url=os.getenv("ALPACA_BASE_URL", "https://paper-api.alpaca.markets"),
        )


def annualization_factor(timeframe: str) -> float:
    mapping = {
        "1Min": 252 * 390,
        "5Min": 252 * 78,
        "15Min": 252 * 26,
        "30Min": 252 * 13,
        "1Hour": 252 * 6.5,
        "1Day": 252,
    }
    return mapping.get(timeframe, 252 * 26)


def ema(series: pd.Series, span: int) -> pd.Series:
    return series.ewm(span=span, adjust=False).mean()


def rolling_vwap(df: pd.DataFrame, window: int) -> pd.Series:
    typical_price = (df["high"] + df["low"] + df["close"]) / 3.0
    price_volume = typical_price * df["volume"]
    return price_volume.rolling(window).sum() / df["volume"].rolling(window).sum().replace(0, pd.NA)


def relative_volume(volume: pd.Series, window: int) -> pd.Series:
    return volume / volume.rolling(window).mean().replace(0, pd.NA)


def strong_close_fraction(df: pd.DataFrame) -> pd.Series:
    candle_range = (df["high"] - df["low"]).replace(0, pd.NA)
    return (df["close"] - df["low"]) / candle_range


def weak_close_fraction(df: pd.DataFrame) -> pd.Series:
    candle_range = (df["high"] - df["low"]).replace(0, pd.NA)
    return (df["high"] - df["close"]) / candle_range


def max_drawdown(equity_curve: pd.Series) -> float:
    running_peak = equity_curve.cummax()
    drawdown = (equity_curve / running_peak) - 1.0
    return float(drawdown.min()) if not drawdown.empty else 0.0


def position_size_from_risk(equity: float, entry: float, stop: float, risk_fraction: float) -> float:
    if entry <= 0 or equity <= 0:
        return 0.0
    risk_amount = equity * risk_fraction
    risk_per_share = abs(entry - stop)
    if risk_amount <= 0 or risk_per_share <= 0:
        return 0.0
    shares_by_risk = risk_amount / risk_per_share
    shares_by_cash = equity / entry
    qty = min(shares_by_risk, shares_by_cash)
    return round(max(qty, 0.0), 4)


class Journal:
    def __init__(self, path: str):
        self.path = Path(path)
        self.fields = [
            "timestamp",
            "symbol",
            "strategy",
            "side",
            "entry",
            "stop",
            "exit",
            "size",
            "pnl",
            "r_multiple",
        ]
        if not self.path.exists():
            with self.path.open("w", newline="") as handle:
                writer = csv.DictWriter(handle, fieldnames=self.fields)
                writer.writeheader()

    def log_trade(self, row: dict[str, Any]) -> None:
        with self.path.open("a", newline="") as handle:
            writer = csv.DictWriter(handle, fieldnames=self.fields)
            writer.writerow(row)


class DataLoader:
    def __init__(self, config: Config):
        self.config = config

    def load_historical_data(self, symbol: str) -> pd.DataFrame:
        csv_path = Path(self.config.data_dir) / f"{symbol}_{self.config.timeframe}.csv"
        if csv_path.exists():
            df = pd.read_csv(csv_path, parse_dates=["timestamp"])
            return self._prepare(df, symbol)
        if self.config.alpaca_api_key and self.config.alpaca_secret_key:
            fetched = self._fetch_alpaca_bars(symbol)
            if fetched is not None and not fetched.empty:
                return self._prepare(fetched, symbol)
        return self._prepare(self._generate_sample_data(symbol), symbol)

    def latest_bar(self, symbol: str) -> pd.DataFrame:
        if not (self.config.alpaca_api_key and self.config.alpaca_secret_key):
            raise ValueError("Alpaca credentials are required for paper/live data.")
        params = {"symbols": symbol, "timeframe": self.config.timeframe, "limit": 2}
        response = requests.get(
            f"{self.config.alpaca_data_url}/v2/stocks/bars/latest",
            headers=self._alpaca_headers(),
            params=params,
            timeout=15,
        )
        response.raise_for_status()
        payload = response.json().get("bars", {})
        bar = payload.get(symbol)
        if not bar:
            raise ValueError(f"No latest bar returned for {symbol}.")
        df = pd.DataFrame(
            [
                {
                    "timestamp": pd.to_datetime(bar["t"], utc=True),
                    "open": bar["o"],
                    "high": bar["h"],
                    "low": bar["l"],
                    "close": bar["c"],
                    "volume": bar["v"],
                    "symbol": symbol,
                }
            ]
        )
        return self._prepare(df, symbol)

    def _alpaca_headers(self) -> dict[str, str]:
        return {
            "APCA-API-KEY-ID": self.config.alpaca_api_key,
            "APCA-API-SECRET-KEY": self.config.alpaca_secret_key,
        }

    def _fetch_alpaca_bars(self, symbol: str) -> pd.DataFrame | None:
        end = datetime.now(UTC)
        start = end - timedelta(days=self.config.lookback_days)
        params = {
            "symbols": symbol,
            "timeframe": self.config.timeframe,
            "start": start.isoformat(),
            "end": end.isoformat(),
            "limit": 10_000,
            "adjustment": "raw",
            "feed": "iex",
        }
        response = requests.get(
            f"{self.config.alpaca_data_url}/v2/stocks/bars",
            headers=self._alpaca_headers(),
            params=params,
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
                    "symbol": symbol,
                }
            )
        return pd.DataFrame(rows)

    def _generate_sample_data(self, symbol: str) -> pd.DataFrame:
        periods = max(self.config.lookback_days * 26, 200)
        end = datetime.now(UTC).replace(minute=0, second=0, microsecond=0)
        index = pd.date_range(end=end, periods=periods, freq="15min", tz="UTC")
        seed = sum(ord(char) for char in symbol)
        rows: list[dict[str, Any]] = []
        close = 50.0 + (seed % 30)
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
                    "symbol": symbol,
                }
            )
        return pd.DataFrame(rows)

    def _prepare(self, df: pd.DataFrame, symbol: str) -> pd.DataFrame:
        clean = df.copy()
        clean["timestamp"] = pd.to_datetime(clean["timestamp"], utc=True)
        clean = clean.sort_values("timestamp").drop_duplicates("timestamp")
        clean["symbol"] = symbol
        return add_indicators(clean, self.config)


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
    return data


class Strategy:
    name = "base"

    def generate_signal(self, row: pd.Series, history: pd.DataFrame, config: Config) -> dict[str, Any] | None:
        raise NotImplementedError


class VWAPMomentumBreakoutStrategy(Strategy):
    name = "momentum"

    def generate_signal(self, row: pd.Series, history: pd.DataFrame, config: Config) -> dict[str, Any] | None:
        if pd.isna(row["vwap"]) or pd.isna(row["recent_high"]) or pd.isna(row["recent_low"]) or pd.isna(row["rvol"]):
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
            if stop < row["close"]:
                return {"side": "buy", "entry": row["close"], "stop": stop, "reason": "vwap_breakout_long"}

        short_setup = (
            config.allow_short
            and row["close"] < row["vwap"]
            and row["ema_fast"] < row["ema_slow"]
            and row["close"] < row["recent_low"]
            and row["rvol"] > config.rvol_threshold
            and row["weak_close"] >= config.candle_body_threshold
        )
        if short_setup:
            stop = max(row["recent_low"], row["high"]) * (1.0 + config.stop_buffer_pct)
            if stop > row["close"]:
                return {"side": "sell", "entry": row["close"], "stop": stop, "reason": "vwap_breakout_short"}
        return None


class MeanReversionStrategy(Strategy):
    name = "mean_reversion"

    def generate_signal(self, row: pd.Series, history: pd.DataFrame, config: Config) -> dict[str, Any] | None:
        if pd.isna(row["zscore"]) or pd.isna(row["mean"]):
            return None

        if row["zscore"] <= -config.mean_reversion_zscore and row["close"] < row["vwap"]:
            stop = row["low"] * (1.0 - config.stop_buffer_pct)
            if stop < row["close"]:
                return {"side": "buy", "entry": row["close"], "stop": stop, "reason": "mean_reversion_long"}

        if config.allow_short and row["zscore"] >= config.mean_reversion_zscore and row["close"] > row["vwap"]:
            stop = row["high"] * (1.0 + config.stop_buffer_pct)
            if stop > row["close"]:
                return {"side": "sell", "entry": row["close"], "stop": stop, "reason": "mean_reversion_short"}
        return None


STRATEGIES: dict[str, Strategy] = {
    VWAPMomentumBreakoutStrategy.name: VWAPMomentumBreakoutStrategy(),
    MeanReversionStrategy.name: MeanReversionStrategy(),
}


class ExecutionHandler:
    def __init__(self, config: Config):
        self.config = config

    def submit_fractional_order(self, symbol: str, side: str, qty: float, order_type: str = "market") -> dict[str, Any]:
        if self.config.mode == "backtest":
            raise ValueError("ExecutionHandler should not be used in backtest mode.")
        if not (self.config.alpaca_api_key and self.config.alpaca_secret_key):
            raise ValueError("Alpaca credentials are required for paper/live execution.")

        payload = {
            "symbol": symbol,
            "side": side,
            "type": order_type,
            "time_in_force": "day",
            "qty": round(qty, 4),
        }

        response = requests.post(
            f"{self.config.alpaca_base_url}/v2/orders",
            headers={
                "APCA-API-KEY-ID": self.config.alpaca_api_key,
                "APCA-API-SECRET-KEY": self.config.alpaca_secret_key,
                "Content-Type": "application/json",
            },
            json={k: v for k, v in payload.items() if v is not None},
            timeout=15,
        )
        response.raise_for_status()
        return response.json()


@dataclass
class Position:
    symbol: str
    strategy: str
    side: str
    entry_time: pd.Timestamp
    entry_price: float
    stop_price: float
    initial_stop_price: float
    qty: float
    open_qty: float
    initial_risk_per_share: float
    partial_taken: bool = False
    exit_time: pd.Timestamp | None = None
    exit_price: float | None = None
    pnl: float = 0.0
    r_multiple: float = 0.0


class BacktestEngine:
    def __init__(self, config: Config, journal: Journal):
        self.config = config
        self.journal = journal

    def run(self, strategy_name: str) -> dict[str, Any]:
        strategy = STRATEGIES[strategy_name]
        equity = self.config.starting_capital
        daily_loss = 0.0
        trades_today = 0
        current_day: datetime.date | None = None
        position: Position | None = None
        trade_pnls: list[float] = []
        r_multiples: list[float] = []
        equity_points: list[dict[str, Any]] = []

        for symbol in self.config.symbols:
            data = DataLoader(self.config).load_historical_data(symbol)
            position = None
            current_day = None
            daily_loss = 0.0
            trades_today = 0

            for i in range(1, len(data)):
                row = data.iloc[i]
                history = data.iloc[: i + 1]
                day = row["timestamp"].date()
                if current_day != day:
                    current_day = day
                    daily_loss = 0.0
                    trades_today = 0

                if position and position.symbol == symbol:
                    closed = self._manage_open_position(position, row)
                    if closed is not None:
                        equity += closed.pnl
                        daily_loss += min(closed.pnl, 0.0)
                        trade_pnls.append(closed.pnl)
                        r_multiples.append(closed.r_multiple)
                        self._journal_trade(closed)
                        position = None

                if position is not None:
                    equity_points.append({"timestamp": row["timestamp"], "equity": equity})
                    continue

                if trades_today >= self.config.max_trades_per_day:
                    equity_points.append({"timestamp": row["timestamp"], "equity": equity})
                    continue

                if abs(daily_loss) >= equity * self.config.max_daily_loss:
                    equity_points.append({"timestamp": row["timestamp"], "equity": equity})
                    continue

                signal = strategy.generate_signal(row, history, self.config)
                if signal is None:
                    equity_points.append({"timestamp": row["timestamp"], "equity": equity})
                    continue

                qty = position_size_from_risk(equity, signal["entry"], signal["stop"], self.config.risk_per_trade)
                if qty <= 0:
                    equity_points.append({"timestamp": row["timestamp"], "equity": equity})
                    continue

                slippage = signal["entry"] * (self.config.slippage_bps / 10_000.0)
                entry_price = signal["entry"] + slippage if signal["side"] == "buy" else signal["entry"] - slippage
                position = Position(
                    symbol=symbol,
                    strategy=strategy_name,
                    side=signal["side"],
                    entry_time=row["timestamp"],
                    entry_price=entry_price,
                    stop_price=signal["stop"],
                    initial_stop_price=signal["stop"],
                    qty=qty,
                    open_qty=qty,
                    initial_risk_per_share=abs(entry_price - signal["stop"]),
                )
                trades_today += 1
                equity_points.append({"timestamp": row["timestamp"], "equity": equity})

            if position is not None:
                last_row = data.iloc[-1]
                closed = self._close_position(position, last_row["timestamp"], last_row["close"], "end_of_data")
                equity += closed.pnl
                trade_pnls.append(closed.pnl)
                r_multiples.append(closed.r_multiple)
                self._journal_trade(closed)

        equity_curve = pd.DataFrame(equity_points)
        if equity_curve.empty:
            equity_series = pd.Series([self.config.starting_capital], dtype="float64")
        else:
            equity_series = equity_curve["equity"]

        total_return = (equity / self.config.starting_capital) - 1.0
        wins = sum(1 for pnl in trade_pnls if pnl > 0)
        win_rate = wins / len(trade_pnls) if trade_pnls else 0.0
        return {
            "strategy": strategy_name,
            "ending_equity": round(equity, 2),
            "total_return": round(total_return * 100, 2),
            "win_rate": round(win_rate * 100, 2),
            "average_r": round(sum(r_multiples) / len(r_multiples), 2) if r_multiples else 0.0,
            "max_drawdown": round(max_drawdown(equity_series) * 100, 2),
            "trades": len(trade_pnls),
        }

    def _manage_open_position(self, position: Position, row: pd.Series) -> Position | None:
        target_1 = (
            position.entry_price + position.initial_risk_per_share * self.config.partial_take_profit_r
            if position.side == "buy"
            else position.entry_price - position.initial_risk_per_share * self.config.partial_take_profit_r
        )
        target_2 = (
            position.entry_price + position.initial_risk_per_share * self.config.final_take_profit_r
            if position.side == "buy"
            else position.entry_price - position.initial_risk_per_share * self.config.final_take_profit_r
        )

        if position.side == "buy":
            if row["low"] <= position.stop_price:
                return self._close_position(position, row["timestamp"], position.stop_price, "stop_loss")
            if not position.partial_taken and row["high"] >= target_1:
                self._take_partial(position, target_1)
            if row["high"] >= target_2 or row["close"] < row["ema_fast"]:
                return self._close_position(position, row["timestamp"], min(target_2, row["close"]), "target_or_structure")
        else:
            if row["high"] >= position.stop_price:
                return self._close_position(position, row["timestamp"], position.stop_price, "stop_loss")
            if not position.partial_taken and row["low"] <= target_1:
                self._take_partial(position, target_1)
            if row["low"] <= target_2 or row["close"] > row["ema_fast"]:
                return self._close_position(position, row["timestamp"], max(target_2, row["close"]), "target_or_structure")
        return None

    def _take_partial(self, position: Position, target_price: float) -> None:
        partial_qty = round(position.open_qty * 0.5, 4)
        if partial_qty <= 0:
            return
        direction = 1 if position.side == "buy" else -1
        position.pnl += (target_price - position.entry_price) * partial_qty * direction
        position.open_qty = round(position.open_qty - partial_qty, 4)
        position.partial_taken = True
        if position.side == "buy":
            position.stop_price = max(position.stop_price, position.entry_price)
        else:
            position.stop_price = min(position.stop_price, position.entry_price)

    def _close_position(self, position: Position, exit_time: pd.Timestamp, exit_price: float, reason: str) -> Position:
        slippage = exit_price * (self.config.slippage_bps / 10_000.0)
        fill = exit_price - slippage if position.side == "buy" else exit_price + slippage
        direction = 1 if position.side == "buy" else -1
        gross_pnl = (fill - position.entry_price) * position.open_qty * direction
        pnl = position.pnl + gross_pnl - self.config.fee_per_trade
        r_multiple = pnl / (position.initial_risk_per_share * position.qty) if position.initial_risk_per_share > 0 else 0.0
        position.exit_time = exit_time
        position.exit_price = fill
        position.pnl = round(pnl, 2)
        position.r_multiple = round(r_multiple, 2)
        return position

    def _journal_trade(self, position: Position) -> None:
        self.journal.log_trade(
            {
                "timestamp": position.exit_time.isoformat() if position.exit_time is not None else "",
                "symbol": position.symbol,
                "strategy": position.strategy,
                "side": position.side,
                "entry": round(position.entry_price, 4),
                "stop": round(position.initial_stop_price, 4),
                "exit": round(position.exit_price or 0.0, 4),
                "size": round(position.qty, 4),
                "pnl": position.pnl,
                "r_multiple": position.r_multiple,
            }
        )


def compare_strategies(config: Config, strategy_names: Iterable[str]) -> pd.DataFrame:
    results = []
    for strategy_name in strategy_names:
        isolated = replace(config, strategy=strategy_name)
        journal = Journal(f"{Path(config.journal_path).stem}_{strategy_name}.csv")
        engine = BacktestEngine(isolated, journal)
        results.append(engine.run(strategy_name))
    frame = pd.DataFrame(results)
    sort_columns = ["total_return", "win_rate", "average_r"]
    if not frame.empty:
        frame = frame.sort_values(sort_columns, ascending=[False, False, False]).reset_index(drop=True)
    return frame


def print_backtest_summary(result: dict[str, Any]) -> None:
    print(f"Strategy: {result['strategy']}")
    print(f"Ending equity: ${result['ending_equity']:.2f}")
    print(f"Total return: {result['total_return']:.2f}%")
    print(f"Win rate: {result['win_rate']:.2f}%")
    print(f"Average R: {result['average_r']:.2f}")
    print(f"Max drawdown: {result['max_drawdown']:.2f}%")
    print(f"Trades: {result['trades']}")


def print_strategy_comparison(frame: pd.DataFrame) -> None:
    if frame.empty:
        print("No backtest results available.")
        return
    display = frame[["strategy", "total_return", "win_rate", "average_r", "max_drawdown", "trades"]]
    print(display.to_string(index=False))


def build_signal_snapshot(config: Config, strategy_name: str, symbol: str) -> dict[str, Any] | None:
    data = DataLoader(config).load_historical_data(symbol)
    if len(data) < 2:
        return None
    strategy = STRATEGIES[strategy_name]
    row = data.iloc[-1]
    signal = strategy.generate_signal(row, data, config)
    if signal is None:
        return None
    qty = position_size_from_risk(config.starting_capital, signal["entry"], signal["stop"], config.risk_per_trade)
    return {
        "symbol": symbol,
        "strategy": strategy_name,
        "side": signal["side"],
        "entry": round(signal["entry"], 2),
        "stop": round(signal["stop"], 2),
        "qty": qty,
    }


def run_trading_mode(config: Config) -> None:
    if config.mode not in {"paper", "live"}:
        raise ValueError("Trading mode must be paper or live.")
    if config.mode == "live" and os.getenv("ALLOW_LIVE", "").lower() != "true":
        raise ValueError("Live mode is blocked unless ALLOW_LIVE=true is set in the environment.")
    execution = ExecutionHandler(config)
    for symbol in config.symbols:
        snapshot = build_signal_snapshot(config, config.strategy, symbol)
        if snapshot is None:
            print(f"{symbol}: no signal")
            continue
        if snapshot["qty"] <= 0:
            print(f"{symbol}: signal found but size is zero")
            continue
        print(f"{config.mode.upper()} mode submitting {snapshot}")
        result = execution.submit_fractional_order(symbol, snapshot["side"], snapshot["qty"])
        print(f"Order submitted: {result.get('id', 'unknown_id')} {symbol}")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Lightweight trading bot for small accounts.")
    parser.add_argument("--mode", choices=["backtest", "paper", "live"], default="backtest")
    parser.add_argument("--strategy", choices=sorted(STRATEGIES.keys()), default="momentum")
    parser.add_argument("--compare", action="store_true", help="Run comparison across configured strategies.")
    parser.add_argument("--symbols", nargs="+", help="Override watchlist symbols.")
    parser.add_argument("--capital", type=float, help="Override starting capital.")
    parser.add_argument("--timeframe", default=None, help="Override timeframe such as 15Min or 1Hour.")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    config = Config.from_env()
    if args.symbols:
        config.symbols = args.symbols
    if args.capital:
        config.starting_capital = args.capital
    if args.timeframe:
        config.timeframe = args.timeframe
    config.mode = args.mode
    config.strategy = args.strategy

    if config.strategy not in STRATEGIES:
        raise ValueError(f"Unknown strategy: {config.strategy}")

    if args.compare:
        frame = compare_strategies(config, config.compare_strategies)
        print_strategy_comparison(frame)
        return

    if config.mode == "backtest":
        journal = Journal(config.journal_path)
        result = BacktestEngine(config, journal).run(config.strategy)
        print_backtest_summary(result)
        return

    run_trading_mode(config)


if __name__ == "__main__":
    main()
