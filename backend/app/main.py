from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
import os
import pandas as pd

from .models import OrderRequest, OrderResponse, PortfolioResponse, StressTestRequest, StressTestResponse
from .data_engine import get_available_tickers, get_most_recent_day_data
from .trading_engine import process_order, get_full_portfolio
from .simulator import run_trade_simulation, TRADES_DIR

app = FastAPI(
    title="Block Trade Engine API",
    description="High-throughput intraday trade simulator and execution engine",
    version="1.0.0"
)

# CORS setup for React frontend
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.get("/api/tickers")
def list_tickers():
    """
    Returns available stocks for trading and simulation.
    """
    return get_available_tickers()


@app.get("/api/prices/recent")
def get_recent_prices(ticker: str = Query("NVDA")):
    """
    Returns 1-minute OHLCV bars for the most recent day available for ticker.
    """
    try:
        recent_df, latest_date = get_most_recent_day_data(ticker.upper())
        if recent_df.empty:
            raise HTTPException(status_code=404, detail=f"No data for ticker {ticker}")
        
        # Convert Datetime to string for JSON serialization
        recent_df["Datetime"] = recent_df["Datetime"].dt.strftime("%Y-%m-%d %H:%M:%S")
        records = recent_df.to_dict(orient="records")
        return {
            "ticker": ticker.upper(),
            "date": latest_date,
            "total_bars": len(records),
            "bars": records
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/portfolio", response_model=PortfolioResponse)
def get_portfolio():
    """
    Returns user cash balance, current holdings, total equity, and order history.
    """
    return get_full_portfolio()


@app.post("/api/orders", response_model=OrderResponse)
def place_order(order: OrderRequest):
    """
    Executes a BUY or SELL order against current market price and updates CSVs.
    """
    try:
        return process_order(order)
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))


@app.post("/api/stresstest/run", response_model=StressTestResponse)
def run_stresstest(request: StressTestRequest):
    """
    Simulates trades matching the price profile of the most recent day's available price data
    at the user-specified trades_per_minute rate.
    """
    try:
        return run_trade_simulation(request.ticker.upper(), request.trades_per_minute)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/trades/recent")
def get_recent_trades(ticker: str = Query("NVDA"), limit: int = Query(100)):
    """
    Returns recent generated trade stream for ticker.
    """
    trade_file = os.path.join(TRADES_DIR, f"{ticker.upper()}.csv")
    if not os.path.exists(trade_file):
        return {"ticker": ticker.upper(), "trades": []}

    df = pd.read_csv(trade_file)
    trades = df.tail(limit).to_dict(orient="records")
    return {"ticker": ticker.upper(), "total": len(df), "trades": trades}
