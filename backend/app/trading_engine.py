import uuid
from datetime import datetime

from .data_engine import (
    read_portfolio_csv,
    save_portfolio_csv,
    append_order_csv,
    read_orders_csv,
    get_most_recent_day_data,
    get_available_tickers
)
from .models import OrderRequest, OrderResponse, PortfolioResponse, Position
from .clickhouse_storage import storage_engine
from .merkle_engine import hash_trade
from .simulator import global_simulator


def get_current_price_for_ticker(ticker: str) -> float:
    """
    Returns current live price: from active simulation bar if available, else recent day data.
    """
    ticker_upper = ticker.upper()
    if ticker_upper == "NVDA" and hasattr(global_simulator, "session_df") and not global_simulator.session_df.empty:
        idx = max(0, min(global_simulator.current_minute - 1 if global_simulator.current_minute > 0 else 0, len(global_simulator.session_df) - 1))
        return round(float(global_simulator.session_df.iloc[idx]["Close"]), 2)

    recent_df, _ = get_most_recent_day_data(ticker)
    if not recent_df.empty and "Close" in recent_df.columns:
        return round(float(recent_df.iloc[-1]["Close"]), 2)
    return 180.00


def process_order(order: OrderRequest) -> OrderResponse:
    cash, positions = read_portfolio_csv()
    ticker = order.ticker.upper()
    side = order.side.upper()
    order_type = order.order_type.upper()
    qty = order.quantity

    # Determine execution price
    market_price = get_current_price_for_ticker(ticker)
    exec_price = round(float(order.price) if (order_type == "LIMIT" and order.price is not None and order.price > 0) else market_price, 2)

    total_cost = round(exec_price * qty, 2)
    timestamp_str = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    order_id = f"ORD-{uuid.uuid4().hex[:8].upper()}"
    trade_id = f"TRD-{ticker}-{uuid.uuid4().hex[:8].upper()}"

    if side == "BUY":
        if cash < total_cost:
            res = OrderResponse(
                order_id=order_id,
                trade_id="",
                timestamp=timestamp_str,
                ticker=ticker,
                side=side,
                order_type=order_type,
                quantity=qty,
                price=exec_price,
                filled_price=0.0,
                status="REJECTED",
                message=f"Insufficient cash: Required ${total_cost:,.2f}, Available ${cash:,.2f}"
            )
            append_order_csv(res.model_dump())
            return res

        # Execute BUY
        cash = round(cash - total_cost, 2)
        curr_pos = positions.get(ticker, {"shares": 0, "average_cost": 0.0})
        total_shares = curr_pos["shares"] + qty
        new_avg_cost = round(((curr_pos["shares"] * curr_pos["average_cost"]) + total_cost) / total_shares, 2)
        positions[ticker] = {"shares": total_shares, "average_cost": new_avg_cost}

    elif side == "SELL":
        curr_pos = positions.get(ticker, {"shares": 0, "average_cost": 0.0})
        if curr_pos["shares"] < qty:
            res = OrderResponse(
                order_id=order_id,
                trade_id="",
                timestamp=timestamp_str,
                ticker=ticker,
                side=side,
                order_type=order_type,
                quantity=qty,
                price=exec_price,
                filled_price=0.0,
                status="REJECTED",
                message=f"Insufficient shares: Holding {curr_pos['shares']} shares, requested {qty}"
            )
            append_order_csv(res.model_dump())
            return res

        # Execute SELL
        cash = round(cash + total_cost, 2)
        remaining_shares = curr_pos["shares"] - qty
        if remaining_shares == 0:
            del positions[ticker]
        else:
            positions[ticker]["shares"] = remaining_shares

    save_portfolio_csv(cash, positions)

    # Persist verified trade record in SQLite storage for instant query by trade_id
    sim_ts = datetime.now().strftime("%Y-%m-%dT%H:%M:%S.000+05:30")
    trade_record = {
        "trade_id": trade_id,
        "simulation_timestamp": sim_ts,
        "source_timestamp": sim_ts,
        "symbol": ticker,
        "side": side,
        "price": float(exec_price),
        "quantity": int(qty)
    }
    leaf_hash = "0x" + hash_trade(trade_record).hex()

    storage_engine.insert_trades_batch(
        [trade_record],
        minute_index=getattr(global_simulator, "current_minute", 0) or 0,
        simulation_date=getattr(global_simulator, "simulation_date", "") or datetime.now().strftime("%Y-%m-%d")
    )

    res = OrderResponse(
        order_id=order_id,
        trade_id=trade_id,
        timestamp=timestamp_str,
        ticker=ticker,
        side=side,
        order_type=order_type,
        quantity=qty,
        price=exec_price,
        filled_price=exec_price,
        status="FILLED",
        message="Order placed and matched successfully.",
        leaf_hash=leaf_hash
    )
    append_order_csv(res.model_dump())
    return res


def get_full_portfolio() -> PortfolioResponse:
    cash, positions = read_portfolio_csv()
    pos_models = []
    total_positions_value = 0.0

    for ticker, pinfo in positions.items():
        shares = pinfo["shares"]
        avg_cost = pinfo["average_cost"]
        curr_price = get_current_price_for_ticker(ticker)
        mkt_val = shares * curr_price
        unrealized = (curr_price - avg_cost) * shares

        total_positions_value += mkt_val
        pos_models.append(Position(
            ticker=ticker,
            shares=shares,
            average_cost=round(avg_cost, 2),
            current_price=round(curr_price, 2),
            market_value=round(mkt_val, 2),
            unrealized_pnl=round(unrealized, 2)
        ))

    orders = read_orders_csv()
    total_equity = cash + total_positions_value

    return PortfolioResponse(
        cash=round(cash, 2),
        total_equity=round(total_equity, 2),
        positions=pos_models,
        orders=orders
    )
