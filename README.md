# microcapital-trader

Lightweight momentum trading bot for small accounts, built around a single Python script: [microcapital_trader.py](/Users/jurekolar/Code/microcapital-trader/microcapital_trader.py).

## Current State

The current version supports:

- Momentum backtesting
- One-strategy comparison output for momentum
- Alpaca paper trading
- Alpaca live trading
- Scheduled paper trading sessions
- Scheduled live trading sessions
- Fractional position sizing for small accounts
- CSV trade and order journaling

Implemented strategy:

- `momentum`: VWAP momentum breakout with EMA, relative-volume, and candle-close filters

The script has been smoke-tested locally with:

- `python -m py_compile`
- backtest mode
- compare mode

## Project Files

- [microcapital_trader.py](/Users/jurekolar/Code/microcapital-trader/microcapital_trader.py): main bot script
- [requirements.txt](/Users/jurekolar/Code/microcapital-trader/requirements.txt): runtime dependencies
- `trade_journal.csv`: created automatically when trades or order events are logged
- `trade_journal_momentum.csv`: created automatically during compare runs
- `strategy_comparison.csv`: created automatically by compare mode
- `live_state.json`: local live/paper order and position state
- `data/<SYMBOL>_<TIMEFRAME>.csv`: optional local historical data input

Backtest data source priority:

1. local CSV in `data/`
2. Alpaca historical bars for Alpaca-supported symbols
3. synthetic sample data as a fallback

Backtest mode prints the resolved source per symbol and warns explicitly when it falls back to synthetic data.

## Install

```bash
python3 -m venv .venv
.venv/bin/pip install -r requirements.txt
```

Dependencies:

- `pandas`
- `requests`
- `python-dotenv`
- `alpaca-py`

## Environment Variables

Create a `.env` file in the repo root:

```env
ALPACA_API_KEY=your_key
ALPACA_SECRET_KEY=your_secret
ALPACA_BASE_URL=https://paper-api.alpaca.markets
```

Notes:

- For paper trading, use `https://paper-api.alpaca.markets`
- For live trading, use `https://api.alpaca.markets`
- Live mode is additionally gated by `ALLOW_LIVE=true`
- Optional overrides include `MODE`, `SYMBOLS`, `ASSET_CLASS`, `ALLOW_SHORT`, `RISK_PROFILE`, `RISK_PER_TRADE`, `CAPITAL_DEPLOYMENT_FRACTION`, `MAX_DAILY_LOSS`, `SLIPPAGE_BPS`, `SPREAD_BPS`, `TIMEFRAME`, `JOURNAL_PATH`, `STATE_PATH`, `RUN_ID`, and `STRICT_DATA`

## Default Configuration

The bot uses a single `Config` dataclass inside [microcapital_trader.py](/Users/jurekolar/Code/microcapital-trader/microcapital_trader.py).

Current defaults:

- Starting capital: `$1,000`
- Risk per trade: `5.0%`
- Max daily loss: `10%`
- Max trades per day: `4`
- Symbols: `TQQQ MSFT ORCL NET PYPL CAT NFLX INTC PLTR AMZN NOW BABA ARM CRWD QCOM`
- Timeframe: `15Min`
- Strategy: `momentum`
- Breakout lookback: `20`
- EMA fast/slow: `9 / 20`
- VWAP window: `30`
- RVOL threshold: `1.5`
- Partial take profit: `1R`
- Final take profit: `2R`
- Max position notional: `$1,000`
- Max symbol exposure: `$1,000`
- Max gross exposure: `$3,000`

To change defaults, edit the `Config` dataclass directly.

Risk profiles:

- `conservative`: current default sizing posture, using equity as the sizing budget.
- `aggressive_margin`: sets `risk_per_trade=1.0`, `capital_deployment_fraction=1.0`, and `max_daily_loss=1.0`; paper/live sizing uses Alpaca `buying_power`, while backtests use the configured gross exposure budget.

## Strategy Logic

### `momentum`

Long setup:

- price above VWAP
- EMA 9 above EMA 20
- close above recent high
- relative volume above `1.5`
- strong candle close

Short setup:

- price below VWAP
- EMA 9 below EMA 20
- close below recent low
- relative volume above `1.5`
- weak candle close
- disabled for crypto

Trade management:

- stop beyond breakout structure
- partial profit at `1R`
- remaining position exits at `2R` or EMA structure break

## Data Flow

```text
historical data / Alpaca bars
-> indicators
-> momentum signal
-> position sizing and risk checks
-> backtest engine or Alpaca execution
-> CSV journal and local live state
```

## Usage

### 1. Run a backtest

```bash
.venv/bin/python microcapital_trader.py --mode backtest
```

Override capital and symbols:

```bash
.venv/bin/python microcapital_trader.py --mode backtest --capital 1500 --symbols AAPL NVDA
```

Override timeframe:

```bash
.venv/bin/python microcapital_trader.py --mode backtest --timeframe 1Hour
```

### 2. Write a momentum comparison report

```bash
.venv/bin/python microcapital_trader.py --mode backtest --compare
```

Current comparison metrics:

- total return
- win rate
- average R
- max drawdown
- number of trades
- average slippage
- average spread cost
- average holding time
- rejected trade count
- realized reward:risk
- data sources
- trades by session bucket
- quote source counts
- max buying power used
- synthetic data warning when fallback data was used

### 3. Paper trade with Alpaca

Set `.env` to paper API credentials and paper base URL, then run:

```bash
.venv/bin/python microcapital_trader.py --mode paper
```

Example:

```bash
.venv/bin/python microcapital_trader.py --mode paper --symbols AAPL MSFT
```

### 4. Live trade with Alpaca

Live mode is intentionally separated and blocked unless `ALLOW_LIVE=true` is set.

```bash
ALLOW_LIVE=true .venv/bin/python microcapital_trader.py --mode live
```

Example with explicit live base URL in `.env`:

```env
ALPACA_BASE_URL=https://api.alpaca.markets
```

```bash
ALLOW_LIVE=true .venv/bin/python microcapital_trader.py --mode live --symbols AAPL
```

### 5. Scheduled paper session

The scheduled modes wait until the configured pre-open window, start the stream engine automatically, flatten positions shortly before the close, and exit after the close.

```bash
.venv/bin/python microcapital_trader.py --mode scheduled_paper
```

### 6. Scheduled live session

```bash
ALLOW_LIVE=true .venv/bin/python microcapital_trader.py --mode scheduled_live
```

Scheduler defaults in `Config`:

- start `30` minutes before the market open
- flatten `1` minute before the market close
- shut down `5` minutes after the market close

## Optional Local Data Files

If you place CSV files in `data/`, the loader will use them before falling back to Alpaca or synthetic sample data.

Expected file name pattern:

```text
data/AAPL_15Min.csv
data/MSFT_1Hour.csv
```

Expected columns:

```text
timestamp,open,high,low,close,volume
```

## Output and Journaling

Trades and order events are logged to CSV with these fields:

```text
timestamp,symbol,strategy,side,mode,event,entry_exit,stop,size,signal_price,intended_price,fill_price,slippage,spread_cost,execution_cost,pnl,r_multiple,session_bucket,client_order_id,order_status,filled_qty,remaining_qty,rejection_reason,notes
```

Backtest mode prints a summary like:

```text
Strategy: momentum
Data sources: {"AAPL": "synthetic_sample"}
Ending equity: $1008.42
Total return: 0.84%
Win rate: 50.00%
Average R: 0.12
Max drawdown: -1.34%
Trade count: 10
```

Compare mode prints the same momentum metrics and writes one row to `strategy_comparison.csv`.

## CLI Reference

```bash
.venv/bin/python microcapital_trader.py --help
```

Available flags:

- `--mode {backtest,paper,live,scheduled_paper,scheduled_live}`
- `--asset-class {equity,crypto}`
- `--compare`
- `--symbols SYMBOL [SYMBOL ...]`
- `--capital FLOAT`
- `--timeframe VALUE`
- `--risk-profile {aggressive_margin,conservative}`
- `--capital-deployment-fraction FLOAT`
- `--max-daily-loss FLOAT`
- `--journal-path PATH`
- `--state-path PATH`
- `--strict-data`
- `--show-plan`

## Safety Notes

- `paper` and `live` require Alpaca credentials
- `scheduled_paper` and `scheduled_live` also require Alpaca credentials
- `live` requires `ALLOW_LIVE=true`
- `scheduled_live` also requires `ALLOW_LIVE=true`
- paper mode requires `ALPACA_BASE_URL=https://paper-api.alpaca.markets`
- live mode requires `ALPACA_BASE_URL=https://api.alpaca.markets`
- conservative position sizing is capped by available account equity
- `aggressive_margin` can use Alpaca buying power and can lose the full deployed capital
- the current execution path submits market orders only
- short logic exists in the backtest and signal engine and still requires `ALLOW_SHORT=true` to generate short signals
- paper/live only block hard technical failures locally; Alpaca remains the source of truth for buying power, shorting, and final order acceptance
- broker-side order rejections include Alpaca HTTP status, request ID, and response body in `rejection_reason`

## Limitations

- single-script implementation by design
- no portfolio optimizer or advanced framework
- scheduling depends on Alpaca market calendar access and falls back to weekday assumptions if the calendar endpoint is unavailable
- offline backtests may use synthetic sample data if no local CSV or Alpaca credentials are available
- use `--strict-data` for performance claims so missing market data fails instead of falling back to synthetic sample data
