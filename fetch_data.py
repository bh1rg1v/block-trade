import os
import warnings
import logging

import pandas as pd
import yfinance as yf
from tqdm import tqdm

from fetch_tickers import get_top_stocks

warnings.filterwarnings("ignore")
logging.getLogger("yfinance").setLevel(logging.CRITICAL)

SAVE_DIR = "data/prices/"


def fetch_minute_data(tickers, days=28, chunk_size=7):
    """
    Downloads 1-minute OHLCV data for each ticker over the past `days` days.

    yfinance limits 1m interval requests to a 30-day rolling window, so data
    is fetched in `chunk_size`-day windows and concatenated. Timestamps are
    converted to US Eastern time with no timezone suffix. Chunks that fall
    outside Yahoo's allowed range are silently skipped.

    Returns a dict of {ticker: DataFrame}.
    """
    data_dict = {}
    end = pd.Timestamp.now(tz="America/New_York").normalize()
    start = end - pd.Timedelta(days=days)

    for ticker in tqdm(tickers):
        chunks = []
        try:
            chunk_start = start
            while chunk_start < end:
                chunk_end = min(chunk_start + pd.Timedelta(days=chunk_size), end)
                try:
                    df = yf.download(
                        ticker,
                        start=chunk_start.strftime("%Y-%m-%d"),
                        end=chunk_end.strftime("%Y-%m-%d"),
                        interval="1m",
                        auto_adjust=True,
                        progress=False,
                    )
                    if not df.empty:
                        chunks.append(df)
                except Exception:
                    pass
                chunk_start = chunk_end

            if chunks:
                combined = pd.concat(chunks)
                combined = combined[~combined.index.duplicated(keep="first")].sort_index()
                combined.index = combined.index.tz_convert("America/New_York").tz_localize(None)
                combined.reset_index(inplace=True)
                data_dict[ticker] = combined
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
