# block-trade

A pipeline for fetching real minute-level stock price data and generating synthetic trade-level data from it.

## Overview

The project fetches 1-minute OHLCV bars for the top 10 most-traded S&P 500 stocks over the past 28 days, then simulates realistic intraday trades at millisecond precision from those bars. All timestamps are in US Eastern time.

## Scripts

### `fetch_tickers.py`

Identifies the top S&P 500 stocks by average daily volume.

- Scrapes the S&P 500 constituent list from Wikipedia and caches it in `sp500.csv`.
- Ranks up to 200 tickers by 5-day average volume and selects the top 10.
- Fetches the full company name for each selected ticker via yfinance.
- Caches the final list in `top_tickers.csv`. Delete this file to force a refresh.

### `fetch_data.py`

Downloads 1-minute OHLCV price data for the selected tickers.

- Reads the ticker list from `fetch_tickers.py`.
- Fetches data for the past 28 days in 7-day chunks to stay within Yahoo Finance's 30-day rolling limit for 1m interval data.
- Converts all timestamps to US Eastern time with no timezone suffix.
- Saves one CSV per ticker to `data/prices/`.

### `simulate_trades.py`

Generates synthetic trade records from the fetched price data.

- Reads each CSV from `data/prices/`.
- For every 1-minute bar, generates 5 trades with:
  - Timestamps at millisecond precision spread across the full 60-second window.
  - Prices biased toward the high on bullish bars and toward the low on bearish bars.
  - Trade sizes drawn from an exponential distribution scaled to the bar's volume.
- Saves one CSV per ticker to `data/trades/` with columns: `timestamp`, `price`, `size`.

## Data Layout

```
data/
  prices/   # raw 1m OHLCV bars from Yahoo Finance
  trades/   # simulated trade records derived from prices
```

## Usage

```bash
python fetch_tickers.py   # optional, runs automatically via fetch_data.py
python fetch_data.py
python simulate_trades.py
```

## Cache Files

| File | Contents |
|---|---|
| `sp500.csv` | Full S&P 500 constituent list |
| `top_tickers.csv` | Top 10 tickers with company names |

Delete either file to force a fresh fetch on the next run.
