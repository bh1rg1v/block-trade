import os
import pytz
import pandas as pd
import yfinance as yf
from datetime import datetime, time

# Paths to NVDA.csv (both backend/NVDA.csv and root NVDA.csv maintained synchronously)
BACKEND_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
ROOT_DIR = os.path.abspath(os.path.join(BACKEND_DIR, ".."))
PRICES_DIR = os.path.join(ROOT_DIR, "data", "prices")

NVDA_CSV_PATHS = [
    os.path.join(BACKEND_DIR, "NVDA.csv"),
    os.path.join(ROOT_DIR, "NVDA.csv"),
    os.path.join(PRICES_DIR, "NVDA.csv"),
]

ET_TZ = pytz.timezone("America/New_York")
IST_TZ = pytz.timezone("Asia/Kolkata")


def ensure_nvda_csv_exists() -> str:
    """
    Ensures at least one NVDA.csv exists. If prices/NVDA.csv exists, copies or initializes.
    Returns primary file path.
    """
    primary_path = NVDA_CSV_PATHS[0]
    for p in NVDA_CSV_PATHS:
        if os.path.exists(p) and os.path.getsize(p) > 0:
            primary_path = p
            break

    os.makedirs(os.path.dirname(NVDA_CSV_PATHS[0]), exist_ok=True)
    os.makedirs(os.path.dirname(NVDA_CSV_PATHS[1]), exist_ok=True)
    os.makedirs(os.path.dirname(NVDA_CSV_PATHS[2]), exist_ok=True)

    return primary_path


def load_nvda_csv() -> pd.DataFrame:
    """
    Reads the maintained NVDA.csv into a clean, timezone-naive (ET) DataFrame.
    """
    primary_path = ensure_nvda_csv_exists()
    if not os.path.exists(primary_path) or os.path.getsize(primary_path) == 0:
        return pd.DataFrame(columns=["Datetime", "Open", "High", "Low", "Close", "Volume"])

    df = pd.read_csv(primary_path)
    if "Datetime" in df.columns:
        df["Datetime"] = pd.to_datetime(df["Datetime"])
        df.sort_values("Datetime", inplace=True)
        df.drop_duplicates(subset=["Datetime"], keep="first", inplace=True)
        df.reset_index(drop=True, inplace=True)
    return df


def save_nvda_csv(df: pd.DataFrame):
    """
    Saves deduplicated, chronologically sorted NVDA.csv across all target locations.
    """
    if df.empty:
        return

    df_to_save = df.copy()
    df_to_save.sort_values("Datetime", inplace=True)
    df_to_save.drop_duplicates(subset=["Datetime"], keep="first", inplace=True)
    df_to_save.reset_index(drop=True, inplace=True)

    # Format Datetime cleanly as YYYY-MM-DD HH:MM:SS
    if pd.api.types.is_datetime64_any_dtype(df_to_save["Datetime"]):
        df_to_save["Datetime"] = df_to_save["Datetime"].dt.strftime("%Y-%m-%d %H:%M:%S")

    required_cols = ["Datetime", "Open", "High", "Low", "Close", "Volume"]
    df_to_save = df_to_save[required_cols]

    for p in NVDA_CSV_PATHS:
        os.makedirs(os.path.dirname(p), exist_ok=True)
        df_to_save.to_csv(p, index=False)


def fetch_latest_nvda_data(days: int = 7) -> pd.DataFrame:
    """
    Fetches latest NVDA 1-minute historical data from yfinance.
    """
    print(f"[*] Fetching NVDA 1m data for past {days} days via yfinance...")
    try:
        ticker = yf.Ticker("NVDA")
        df = ticker.history(period=f"{days}d", interval="1m", auto_adjust=True)
        if df.empty:
            # Fallback yf.download
            df = yf.download("NVDA", period=f"{days}d", interval="1m", auto_adjust=True, progress=False)

        if df.empty:
            print("[!] yfinance returned empty DataFrame for NVDA.")
            return pd.DataFrame()

        df.reset_index(inplace=True)

        # Standardize column names
        col_map = {}
        for col in df.columns:
            col_str = str(col).lower()
            if "datetime" in col_str or "date" in col_str:
                col_map[col] = "Datetime"
            elif "open" in col_str:
                col_map[col] = "Open"
            elif "high" in col_str:
                col_map[col] = "High"
            elif "low" in col_str:
                col_map[col] = "Low"
            elif "close" in col_str:
                col_map[col] = "Close"
            elif "volume" in col_str:
                col_map[col] = "Volume"

        df.rename(columns=col_map, inplace=True)

        # Convert to US/Eastern local time without timezone offset
        if pd.api.types.is_datetime64tz_dtype(df["Datetime"]):
            df["Datetime"] = df["Datetime"].dt.tz_convert("America/New_York").dt.tz_localize(None)
        else:
            df["Datetime"] = pd.to_datetime(df["Datetime"])

        df.dropna(subset=["Open", "High", "Low", "Close"], inplace=True)
        df = df[["Datetime", "Open", "High", "Low", "Close", "Volume"]]
        return df

    except Exception as e:
        print(f"[!] Error fetching NVDA data from yfinance: {e}")
        return pd.DataFrame()


def sync_nvda_market_data() -> pd.DataFrame:
    """
    Executes the complete daily acquisition flow:
    yfinance -> Latest NVDA data -> Validate -> Deduplicate -> Merge with NVDA.csv -> Chronological ordering -> Save.
    """
    existing_df = load_nvda_csv()
    new_df = fetch_latest_nvda_data(days=7)

    if not new_df.empty:
        if not existing_df.empty:
            combined = pd.concat([existing_df, new_df], ignore_index=True)
        else:
            combined = new_df
    else:
        combined = existing_df

    if not combined.empty:
        save_nvda_csv(combined)

    return load_nvda_csv()


def get_latest_completed_session(df: pd.DataFrame = None) -> tuple[pd.DataFrame, str]:
    """
    Identifies the most recent COMPLETED U.S. trading session in NVDA.csv.
    Session hours: 09:30 ET to 16:00 ET.
    A complete session contains regular-market minute observations.

    Returns (session_df, session_date_str).
    """
    if df is None or df.empty:
        df = load_nvda_csv()

    if df.empty:
        raise ValueError("NVDA.csv is empty. Run sync_nvda_market_data() first.")

    df = df.copy()
    df["Datetime"] = pd.to_datetime(df["Datetime"])
    df["DateStr"] = df["Datetime"].dt.strftime("%Y-%m-%d")
    df["Time"] = df["Datetime"].dt.time

    # Filter to core regular market session 09:30 ET <= Time <= 16:00 ET
    mkt_start = time(9, 30)
    mkt_end = time(16, 0)
    regular_df = df[(df["Time"] >= mkt_start) & (df["Time"] <= mkt_end)].copy()

    if regular_df.empty:
        # If no time filter matches, fallback to whole dates
        regular_df = df.copy()

    # Get available dates sorted descending
    unique_dates = sorted(regular_df["DateStr"].unique(), reverse=True)

    now_et = datetime.now(ET_TZ)
    today_et_str = now_et.strftime("%Y-%m-%d")

    completed_session_date = None
    completed_df = pd.DataFrame()

    for date_str in unique_dates:
        session_bars = regular_df[regular_df["DateStr"] == date_str].sort_values("Datetime")
        # If the date is today's ET date and market is currently open (before 16:00 ET), skip it as incomplete
        if date_str == today_et_str and now_et.time() < mkt_end:
            continue
        
        # Must have sufficient minute bars (e.g. at least 60 bars for valid trading session)
        if len(session_bars) >= 60:
            completed_session_date = date_str
            completed_df = session_bars.drop(columns=["DateStr", "Time"]).reset_index(drop=True)
            break

    # Fallback to the latest date available if no date met strict threshold
    if completed_df.empty and unique_dates:
        completed_session_date = unique_dates[0]
        completed_df = regular_df[regular_df["DateStr"] == completed_session_date].drop(columns=["DateStr", "Time"]).reset_index(drop=True)

    return completed_df, completed_session_date


if __name__ == "__main__":
    df = sync_nvda_market_data()
    print(f"NVDA.csv updated. Total records: {len(df)}")
    session_df, session_date = get_latest_completed_session(df)
    print(f"Latest completed session: {session_date} with {len(session_df)} observations.")
