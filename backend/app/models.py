from typing import List, Dict, Optional
from pydantic import BaseModel, Field


class OrderRequest(BaseModel):
    ticker: str
    side: str = Field(..., description="BUY or SELL")
    order_type: str = Field("MARKET", description="MARKET or LIMIT")
    quantity: int = Field(..., gt=0)
    price: Optional[float] = Field(None, description="Price for limit orders or execution context")


class OrderResponse(BaseModel):
    order_id: str
    trade_id: str
    timestamp: str
    ticker: str
    side: str
    order_type: str
    quantity: int
    price: float
    filled_price: float
    status: str
    message: str
    leaf_hash: Optional[str] = None


class Position(BaseModel):
    ticker: str
    shares: int
    average_cost: float
    current_price: float
    market_value: float
    unrealized_pnl: float


class PortfolioResponse(BaseModel):
    cash: float
    total_equity: float
    positions: List[Position]
    orders: List[Dict]


class StressTestRequest(BaseModel):
    ticker: str
    trades_per_minute: int = Field(..., ge=1, le=10000000, description="Number of trades per minute to simulate (up to 10 Million)")
    days_to_simulate: int = Field(1, ge=1, le=28, description="Number of recent days to include")


class TradeRecord(BaseModel):
    timestamp: str
    price: float
    size: int


class StressTestResponse(BaseModel):
    ticker: str
    trades_per_minute: int
    total_bars_processed: int
    total_trades_generated: int
    elapsed_seconds: float
    throughput_tps: float
    output_file: str
    sample_trades: List[TradeRecord]
