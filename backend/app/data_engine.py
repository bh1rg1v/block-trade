import os
import glob
import pandas as pd

PRICES_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", "data", "prices"))
TOP_TICKERS_CACHE = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", "top_tickers.csv"))
PORTFOLIO_CSV = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", "data", "user_portfolio.csv"))
ORDERS_CSV = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", "data", "user_orders.csv"))
INITIAL_CASH = 100000.0


def get_available_tickers():
    """
    Returns list of available tickers and company names.
    Reads from top_tickers.csv if present, else scans data/prices/.
    """
    if os.path.exists(TOP_TICKERS_CACHE):
        df = pd.read_csv(TOP_TICKERS_CACHE)
        return df.to_dict(orient="records")

    csv_files = glob.glob(os.path.join(PRICES_DIR, "*.csv"))
    tickers = [os.path.basename(f).replace(".csv", "") for f in csv_files]
    return [{"Symbol": t, "Name": t} for t in tickers]


def load_price_data(ticker: str):
    """
    Loads price CSV for a ticker.
    """
    file_path = os.path.join(PRICES_DIR, f"{ticker}.csv")
    if not os.path.exists(file_path):
        raise FileNotFoundError(f"Price data for {ticker} not found.")

    df = pd.read_csv(file_path)
    df["Datetime"] = pd.to_datetime(df["Datetime"])
    df.sort_values("Datetime", inplace=True)
    df.reset_index(drop=True, inplace=True)
    return df


def get_most_recent_day_data(ticker: str):
    """
    Filters the price dataset to the most recent trading day available.
    """
    df = load_price_data(ticker)
    if df.empty:
        return df, ""

    # Determine latest date string YYYY-MM-DD
    df["Date"] = df["Datetime"].dt.strftime("%Y-%m-%d")
    latest_date = df["Date"].max()
    recent_df = df[df["Date"] == latest_date].copy()
    recent_df.drop(columns=["Date"], inplace=True)
    recent_df.reset_index(drop=True, inplace=True)
    return recent_df, latest_date


def initialize_csv_storage():
    """
    Ensures user_portfolio.csv and user_orders.csv exist with initial default state.
    """
    os.makedirs(os.path.dirname(PORTFOLIO_CSV), exist_ok=True)

    if not os.path.exists(PORTFOLIO_CSV):
        # Format: item_type, key, value1, value2
        # Row 1: CASH, CASH, 100000.0, 0
        # Row 2+: POSITION, TICKER, shares, average_cost
        initial_portfolio = pd.DataFrame([
            {"type": "CASH", "key": "CASH", "val1": INITIAL_CASH, "val2": 0.0}
        ])
        initial_portfolio.to_csv(PORTFOLIO_CSV, index=False)

    if not os.path.exists(ORDERS_CSV):
        orders_df = pd.DataFrame(columns=[
            "order_id", "timestamp", "ticker", "side", "order_type", "quantity", "price", "filled_price", "status"
        ])
        orders_df.to_csv(ORDERS_CSV, index=False)


def read_portfolio_csv():
    initialize_csv_storage()
    df = pd.read_csv(PORTFOLIO_CSV)
    
    cash = INITIAL_CASH
    cash_row = df[df["type"] == "CASH"]
    if not cash_row.empty:
        cash = float(cash_row.iloc[0]["val1"])

    positions_df = df[df["type"] == "POSITION"]
    positions = {}
    for _, row in positions_df.iterrows():
        ticker = row["key"]
        shares = int(row["val1"])
        avg_cost = float(row["val2"])
        if shares > 0:
            positions[ticker] = {"shares": shares, "average_cost": avg_cost}

    return cash, positions


def save_portfolio_csv(cash: float, positions: dict):
    os.makedirs(os.path.dirname(PORTFOLIO_CSV), exist_ok=True)
    rows = [{"type": "CASH", "key": "CASH", "val1": cash, "val2": 0.0}]
    for ticker, pos in positions.items():
        if pos["shares"] > 0:
            rows.append({
                "type": "POSITION",
                "key": ticker,
                "val1": pos["shares"],
                "val2": pos["average_cost"]
            })
    pd.DataFrame(rows).to_csv(PORTFOLIO_CSV, index=False)


def read_orders_csv():
    initialize_csv_storage()
    if not os.path.exists(ORDERS_CSV) or os.path.getsize(ORDERS_CSV) == 0:
        return []
    df = pd.read_csv(ORDERS_CSV)
    return df.to_dict(orient="records")


def append_order_csv(order_dict: dict):
    initialize_csv_storage()
    df = pd.read_csv(ORDERS_CSV) if os.path.exists(ORDERS_CSV) and os.path.getsize(ORDERS_CSV) > 0 else pd.DataFrame()
    new_df = pd.concat([df, pd.DataFrame([order_dict])], ignore_index=True)
    new_df.to_csv(ORDERS_CSV, index=False)
