@echo off
cd /d C:\Users\jurek.MARUSA-PC\code\microcapital-trader
.\.venv\Scripts\python.exe .\microcapital_trader.py --mode scheduled_paper >> paper_trading.log 2>&1
