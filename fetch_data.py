import os
import time
import warnings
import logging

import pandas as pd
import requests
import yfinance as yf
from tqdm import tqdm

from fetch_tickers import get_top_stocks

warnings.filterwarnings("ignore")
logging.getLogger("yfinance").setLevel(logging.CRITICAL)

SAVE_DIR = "data/prices/"


def _fetch_single_ticker_data(ticker, days=28, chunk_size=7):
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
    }
    now = int(time.time())
    start = now - days * 86400
    chunk_sec = chunk_size * 86400

    dfs = []
    curr_start = start
    while curr_start < now:
        curr_end = min(curr_start + chunk_sec, now)
        url = f"https://query1.finance.yahoo.com/v8/finance/chart/{ticker}?period1={curr_start}&period2={curr_end}&interval=1m"
        try:
            resp = requests.get(url, headers=headers, timeout=10)
            if resp.status_code == 200:
                data = resp.json()
                result = data.get("chart", {}).get("result")
                if result and "timestamp" in result[0]:
                    timestamps = result[0]["timestamp"]
                    indicators = result[0].get("indicators", {})
                    quote = indicators.get("quote", [{}])[0]
                    chunk_df = pd.DataFrame({
                        "Datetime": pd.to_datetime(timestamps, unit="s", utc=True),
                        "Open": quote.get("open"),
                        "High": quote.get("high"),
                        "Low": quote.get("low"),
                        "Close": quote.get("close"),
                        "Volume": quote.get("volume"),
                    })
                    dfs.append(chunk_df)
        except Exception:
            pass
        curr_start = curr_end
        time.sleep(0.1)

    if dfs:
        df = pd.concat(dfs, ignore_index=True)
        df.dropna(subset=["Open", "High", "Low", "Close"], inplace=True)
        df.drop_duplicates(subset=["Datetime"], keep="first", inplace=True)
        df.sort_values("Datetime", inplace=True)
        df["Datetime"] = df["Datetime"].dt.tz_convert("America/New_York").dt.tz_localize(None)
        df.reset_index(drop=True, inplace=True)
        return df

    # Fallback to yfinance if direct API call yielded no data
    end_ts = pd.Timestamp.now(tz="America/New_York").normalize()
    start_ts = end_ts - pd.Timedelta(days=days)
    chunks = []
    chunk_start = start_ts
    while chunk_start < end_ts:
        chunk_end = min(chunk_start + pd.Timedelta(days=chunk_size), end_ts)
        try:
            cdf = yf.download(
                ticker,
                start=chunk_start.strftime("%Y-%m-%d"),
                end=chunk_end.strftime("%Y-%m-%d"),
                interval="1m",
                auto_adjust=True,
                progress=False,
            )
            if not cdf.empty:
                chunks.append(cdf)
        except Exception:
            pass
        chunk_start = chunk_end

    if chunks:
        combined = pd.concat(chunks)
        combined = combined[~combined.index.duplicated(keep="first")].sort_index()
        combined.index = combined.index.tz_convert("America/New_York").tz_localize(None)
        combined.reset_index(inplace=True)
        return combined

    return pd.DataFrame()


def fetch_minute_data(tickers, days=28, chunk_size=7):
    """
    Downloads 1-minute OHLCV data for each ticker over the past `days` days.

    Yahoo limits 1m interval requests to a 30-day rolling window, so data
    is fetched in `chunk_size`-day windows and concatenated. Timestamps are
    converted to US Eastern time with no timezone suffix.

    Returns a dict of {ticker: DataFrame}.
    """
    data_dict = {}
    for ticker in tqdm(tickers):
        try:
            df = _fetch_single_ticker_data(ticker, days=days, chunk_size=chunk_size)
            if not df.empty:
                data_dict[ticker] = df
        except Exception as e:
            print(f"[!] {ticker}: {e}")

    return data_dict


if __name__ == "__main__":
    top_stocks = get_top_stocks()

    print("\nTop Stocks:\n")
    for i, (symbol, name) in enumerate(top_stocks.items(), 1):
        print(f"  {i}. {symbol} - {name}")
    print()

    minute_data = fetch_minute_data(list(top_stocks.keys()), days=28)

    os.makedirs(SAVE_DIR, exist_ok=True)
    for ticker, df in minute_data.items():
        df.to_csv(os.path.join(SAVE_DIR, f"{ticker}.csv"), index=False)

    print("Data saved successfully.")

