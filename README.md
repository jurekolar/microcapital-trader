# microcapital-trader

Lightweight trading bot for small accounts, built around a single Python script: [microcapital_trader.py](/Users/jurekolar/Code/microcapital-trader/microcapital_trader.py).

## Current State

The current version supports:

- Backtesting
- Strategy comparison across multiple strategies
- Alpaca paper trading
- Alpaca live trading
- Scheduled paper trading sessions
- Scheduled live trading sessions
- Fractional position sizing for small accounts
- CSV trade journaling

Implemented strategies:

- `momentum` (default): VWAP momentum breakout
- `mean_reversion`: simple z-score reversion strategy
- `premarket_regression`: trades SPY from a 2-hour premarket regression signal into the close

The script has been smoke-tested locally with:

- `python -m py_compile`
- backtest mode
- compare mode

## Project Files

- [microcapital_trader.py](/Users/jurekolar/Code/microcapital-trader/microcapital_trader.py): main bot script
- [requirements.txt](/Users/jurekolar/Code/microcapital-trader/requirements.txt): runtime dependencies
- `trade_journal.csv`: created automatically when trades are logged
- `trade_journal_momentum.csv` / `trade_journal_mean_reversion.csv`: created automatically during compare runs
- `data/<SYMBOL>_<TIMEFRAME>.csv`: optional local historical data input

Backtest data source priority:

1. local CSV in `data/`
2. Alpaca historical bars
3. synthetic sample data as a fallback

Backtest mode now prints the resolved source per symbol and warns explicitly when it falls back to synthetic data.

## Install

```bash
python3 -m venv .venv
.venv/bin/pip install -r requirements.txt
```

Dependencies:

- `pandas`
- `requests`
- `python-dotenv`

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

## Default Configuration

The bot uses a single `Config` dataclass inside [microcapital_trader.py](/Users/jurekolar/Code/microcapital-trader/microcapital_trader.py).

Current defaults:

- Starting capital: `$1,000`
- Risk per trade: `0.5%`
- Max daily loss: `2%`
- Max trades per day: `4`
- Symbols: `AAPL MSFT AMD`
- Timeframe: `15Min`
- Default strategy: `momentum`
- Breakout lookback: `20`
- EMA fast/slow: `9 / 20`
- RVOL threshold: `1.5`
- Mean reversion z-score: `1.2`
- Bollinger mean reversion window: `20`
- Bollinger mean reversion std-dev: `2.0`
- Bollinger mean reversion stop loss: `1.5%`
- Bollinger mean reversion fixed order size: `100`
- Partial take profit: `1R`
- Final take profit: `2R`

To change defaults, edit the `Config` dataclass directly.

## Strategy Logic

### `momentum`

Long setup:

- price above VWAP
- EMA 9 above EMA 20
- close above recent high
- relative volume above `1.5`
- strong candle close

Short setup:

- reverse of the long logic

Trade management:

- stop below breakout structure
- partial profit at `1R`
- remaining position exits at `2R` or EMA structure break

### `mean_reversion`

Long setup:

- z-score below negative threshold
- price below VWAP

Short setup:

- z-score above positive threshold
- price above VWAP

Trade management uses the same risk model and exit engine.

### `premarket_regression`

Long setup:

- uses `SPY`
- computes the linear regression slope across the last 2 hours of premarket closes
- buys on the first regular-session bar close when the slope is positive

Short setup:

- sells on the first regular-session bar close when the slope is negative

Trade management:

- allocates 100% of configured capital by default
- exits near the regular market close

### `bb_mean_reversion_long`

Long setup:

- Bollinger Bands use a `20`-bar SMA and `2.0` standard deviations
- enters long when the close crosses under the lower band
- long only

Trade management:

- fixed stop loss at `1.5%` below entry
- exits at the Bollinger middle band
- targets a fixed order size of `100`, capped by available capital in this bot
- this implementation does not support Pine-style `pyramiding=3`; the bot keeps one open position per symbol

## Data Flow

```text
historical data / Alpaca bars
-> indicators
-> strategy signal
-> position sizing and risk checks
-> backtest engine or Alpaca execution
-> CSV journal
```

## Usage

### 1. Run a backtest

Default strategy:

```bash
.venv/bin/python microcapital_trader.py --mode backtest --strategy momentum
```

Alternative strategy:

```bash
.venv/bin/python microcapital_trader.py --mode backtest --strategy mean_reversion
```

```bash
.venv/bin/python microcapital_trader.py --mode backtest --strategy bb_mean_reversion_long
```

Override capital and symbols:

```bash
.venv/bin/python microcapital_trader.py --mode backtest --strategy momentum --capital 1500 --symbols AAPL NVDA
```

Override timeframe:

```bash
.venv/bin/python microcapital_trader.py --mode backtest --strategy momentum --timeframe 1Hour
```

### 2. Compare strategies

```bash
.venv/bin/python microcapital_trader.py --mode backtest --compare
```

Current comparison metrics:

- total return
- win rate
- average R
- max drawdown
- number of trades

### 3. Paper trade with Alpaca

Set `.env` to paper API credentials and paper base URL, then run:

```bash
.venv/bin/python microcapital_trader.py --mode paper --strategy momentum
```

Example:

```bash
.venv/bin/python microcapital_trader.py --mode paper --strategy momentum --symbols AAPL MSFT
```

### 4. Live trade with Alpaca

Live mode is intentionally separated and blocked unless `ALLOW_LIVE=true` is set.

```bash
ALLOW_LIVE=true .venv/bin/python microcapital_trader.py --mode live --strategy momentum
```

Example with explicit live base URL in `.env`:

```env
ALPACA_BASE_URL=https://api.alpaca.markets
```

```bash
ALLOW_LIVE=true .venv/bin/python microcapital_trader.py --mode live --strategy momentum --symbols AAPL
```

### 5. Scheduled paper session

The scheduled modes wait until the configured pre-open window, start the stream engine automatically, flatten positions shortly before the close, and exit after the close.

```bash
.venv/bin/python microcapital_trader.py --mode scheduled_paper --strategy premarket_regression
```

### 6. Scheduled live session

```bash
ALLOW_LIVE=true .venv/bin/python microcapital_trader.py --mode scheduled_live --strategy premarket_regression
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

Trades are logged to CSV with these fields:

- `timestamp`
- `symbol`
- `strategy`
- `side`
- `entry`
- `stop`
- `exit`
- `size`
- `pnl`
- `r_multiple`

Backtest mode prints a simple summary like:

```text
Strategy: momentum
Ending equity: $1641.60
Total return: 64.16%
Win rate: 90.11%
Average R: 1.08
Max drawdown: -0.52%
Trades: 91
```

Compare mode prints a table like:

```text
      strategy  total_return  win_rate  average_r  max_drawdown  trades
      momentum         64.16     90.11       1.08         -0.52      91
mean_reversion        -30.81      9.26      -0.66        -30.81     108
```

## CLI Reference

```bash
.venv/bin/python microcapital_trader.py --help
```

Available flags:

- `--mode {backtest,paper,live,scheduled_paper,scheduled_live}`
- `--strategy {momentum,mean_reversion,premarket_regression}`
- `--compare`
- `--symbols SYMBOL [SYMBOL ...]`
- `--capital FLOAT`
- `--timeframe VALUE`

## Safety Notes

- `paper` and `live` require Alpaca credentials
- `scheduled_paper` and `scheduled_live` also require Alpaca credentials
- `live` requires `ALLOW_LIVE=true`
- `scheduled_live` also requires `ALLOW_LIVE=true`
- position sizing is capped by available account equity
- the current execution path submits market orders only
- short logic exists in the backtest and signal engine, but live tradability depends on your Alpaca account permissions

## Limitations

- single-script implementation by design
- no portfolio optimizer or advanced framework
- scheduling depends on Alpaca market calendar access and falls back to weekday assumptions if the calendar endpoint is unavailable
- no persistent state beyond CSV journals
- offline backtests may use synthetic sample data if no local CSV or Alpaca credentials are available
