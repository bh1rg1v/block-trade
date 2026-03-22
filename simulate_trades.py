import os

import numpy as np
import pandas as pd

STOCK_DIR = "data/prices/"
TRADE_DIR = "data/trades/"
TRADES_PER_MINUTE = 5


def simulate_trades(df, trades_per_minute=5):
    """
    Generates synthetic trades from 1-minute OHLCV bars.

    For each bar, `trades_per_minute` trades are created with:
    - Timestamps spread across the full minute at millisecond precision.
    - Prices biased toward the high on bullish bars and toward the low on
      bearish bars, using the midpoint of open/close as the boundary.
    - Sizes drawn from an exponential distribution scaled to the bar's volume.

    Rows with missing OHLC values are skipped.
    """
    trades = []
    df = df.sort_values("Datetime").reset_index(drop=True)

    for _, row in df.iterrows():
        open_p = row["Open"]
        high = row["High"]
        low = row["Low"]
        close = row["Close"]
        volume = row["Volume"] if not pd.isna(row["Volume"]) else 1

        if pd.isna(open_p) or pd.isna(high) or pd.isna(low) or pd.isna(close):
            continue

        for _ in range(trades_per_minute):
            offset_ms = np.random.randint(0, 60 * 1000)
            trade_time = row["Datetime"] + pd.Timedelta(milliseconds=offset_ms)

            if trade_time.tzinfo is not None:
                trade_time = trade_time.tz_convert("America/New_York").tz_localize(None)

            mid = (open_p + close) / 2
            price = (
                np.random.uniform(mid, high)
                if close >= open_p
                else np.random.uniform(low, mid)
            )
            size = max(1, int(np.random.exponential(scale=volume / trades_per_minute)))

            trades.append({
                "timestamp": trade_time,
                "price": round(price, 2),
                "size": size,
            })

    return pd.DataFrame(trades)


def process_all_stocks(stock_dir, trade_dir):
    """
    Reads each price CSV from `stock_dir`, simulates trades, and writes the
    result to `trade_dir` under the same ticker name.

    Datetime columns are normalized to US Eastern time with no timezone suffix.
    OHLCV columns are coerced to numeric to handle any formatting inconsistencies.
    """
    os.makedirs(trade_dir, exist_ok=True)

    for file in os.listdir(stock_dir):
        if not file.endswith(".csv"):
            continue

        ticker = file.replace(".csv", "")
        file_path = os.path.join(stock_dir, file)

        try:
            df = pd.read_csv(file_path)

            for col in ["Open", "High", "Low", "Close", "Volume"]:
                if col in df.columns:
                    df[col] = pd.to_numeric(df[col], errors="coerce")

            if "Datetime" in df.columns:
                df["Datetime"] = (
                    pd.to_datetime(df["Datetime"], utc=True)
                    .dt.tz_convert("America/New_York")
                    .dt.tz_localize(None)
                )
            elif "Date" in df.columns:
                df.rename(columns={"Date": "Datetime"}, inplace=True)
                df["Datetime"] = (
                    pd.to_datetime(df["Datetime"], utc=True)
                    .dt.tz_convert("America/New_York")
                    .dt.tz_localize(None)
                )
            else:
                print(f"[!] No datetime column found for {ticker}, skipping.")
                continue

            trades_df = simulate_trades(df, TRADES_PER_MINUTE)
            trades_df.to_csv(os.path.join(trade_dir, f"{ticker}.csv"), index=False)
            print(f"[+] {ticker}")

        except Exception as e:
            print(f"[-] {ticker}: {e}")


if __name__ == "__main__":
    process_all_stocks(STOCK_DIR, TRADE_DIR)
    print("\nTrade simulation complete.")
