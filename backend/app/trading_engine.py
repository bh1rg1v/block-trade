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


def get_current_price_for_ticker(ticker: str) -> float:
    """
    Returns the latest close price from the most recent day available for ticker.
    """
    recent_df, _ = get_most_recent_day_data(ticker)
    if not recent_df.empty and "Close" in recent_df.columns:
        return float(recent_df.iloc[-1]["Close"])
    return 100.0


def process_order(order: OrderRequest) -> OrderResponse:
    cash, positions = read_portfolio_csv()
    ticker = order.ticker.upper()
    side = order.side.upper()
    order_type = order.order_type.upper()
    qty = order.quantity

    # Determine execution price
    market_price = get_current_price_for_ticker(ticker)
    exec_price = order.price if (order_type == "LIMIT" and order.price) else market_price

    total_cost = exec_price * qty
    timestamp_str = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    order_id = f"ORD-{uuid.uuid4().hex[:8].upper()}"

    if side == "BUY":
        if cash < total_cost:
            res = OrderResponse(
                order_id=order_id,
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
        cash -= total_cost
        curr_pos = positions.get(ticker, {"shares": 0, "average_cost": 0.0})
        total_shares = curr_pos["shares"] + qty
        new_avg_cost = ((curr_pos["shares"] * curr_pos["average_cost"]) + total_cost) / total_shares
        positions[ticker] = {"shares": total_shares, "average_cost": new_avg_cost}

    elif side == "SELL":
        curr_pos = positions.get(ticker, {"shares": 0, "average_cost": 0.0})
        if curr_pos["shares"] < qty:
            res = OrderResponse(
                order_id=order_id,
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
        cash += total_cost
        remaining_shares = curr_pos["shares"] - qty
        if remaining_shares == 0:
            del positions[ticker]
        else:
            positions[ticker]["shares"] = remaining_shares

    save_portfolio_csv(cash, positions)

    res = OrderResponse(
        order_id=order_id,
        timestamp=timestamp_str,
        ticker=ticker,
        side=side,
        order_type=order_type,
        quantity=qty,
        price=exec_price,
        filled_price=exec_price,
        status="FILLED",
        message="Order executed successfully"
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
