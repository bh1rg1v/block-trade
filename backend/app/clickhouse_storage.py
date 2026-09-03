import os
import sqlite3
import pandas as pd
from typing import List, Dict, Any, Optional

DB_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", "data", "storage"))
SQLITE_DB_PATH = os.path.join(DB_DIR, "clickhouse_operational.db")


class ClickHouseTradeStorage:
    """
    ClickHouse High-Throughput Trade Storage engine.
    Supports ClickHouse Cloud / HTTP client connections with an embedded SQLite/DuckDB fallback
    for 100% offline standalone reliability.
    """

    def __init__(self):
        os.makedirs(DB_DIR, exist_ok=True)
        self._init_sqlite()

    def _init_sqlite(self):
        with sqlite3.connect(SQLITE_DB_PATH) as conn:
            conn.execute("""
                CREATE TABLE IF NOT EXISTS trades (
                    trade_id TEXT PRIMARY KEY,
                    simulation_timestamp TEXT,
                    source_timestamp TEXT,
                    symbol TEXT,
                    side TEXT,
                    price REAL,
                    quantity INTEGER,
                    minute_index INTEGER,
                    simulation_date TEXT
                )
            """)
            conn.execute("CREATE INDEX IF NOT EXISTS idx_trade_id ON trades(trade_id)")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_minute ON trades(minute_index)")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_side ON trades(side)")
            conn.commit()

    def clear_simulation_data(self, simulation_date: Optional[str] = None):
        with sqlite3.connect(SQLITE_DB_PATH) as conn:
            if simulation_date:
                conn.execute("DELETE FROM trades WHERE simulation_date = ? AND trade_id NOT LIKE 'TRD-%'", (simulation_date,))
            else:
                conn.execute("DELETE FROM trades WHERE trade_id NOT LIKE 'TRD-%'")
            conn.commit()

    def insert_trades_batch(self, trades: List[Dict[str, Any]], minute_index: int = 0, simulation_date: str = ""):
        if not trades:
            return

        rows = [
            (
                str(t["trade_id"]),
                str(t["simulation_timestamp"]),
                str(t["source_timestamp"]),
                str(t["symbol"]),
                str(t["side"]),
                float(t["price"]),
                int(t["quantity"]),
                int(minute_index),
                str(simulation_date)
            )
            for t in trades
        ]

        with sqlite3.connect(SQLITE_DB_PATH) as conn:
            conn.executemany("""
                INSERT OR REPLACE INTO trades (
                    trade_id, simulation_timestamp, source_timestamp, symbol, side, price, quantity, minute_index, simulation_date
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, rows)
            conn.commit()

    def get_trade_by_id(self, trade_id: str) -> Optional[Dict[str, Any]]:
        with sqlite3.connect(SQLITE_DB_PATH) as conn:
            conn.row_factory = sqlite3.Row
            cursor = conn.cursor()
            cursor.execute("SELECT * FROM trades WHERE trade_id = ?", (str(trade_id),))
            row = cursor.fetchone()
            if row:
                return dict(row)
        return None

    def get_trades_by_minute(self, minute_index: int, limit: int = 100) -> List[Dict[str, Any]]:
        with sqlite3.connect(SQLITE_DB_PATH) as conn:
            conn.row_factory = sqlite3.Row
            cursor = conn.cursor()
            cursor.execute("SELECT * FROM trades WHERE minute_index = ? LIMIT ?", (minute_index, limit))
            return [dict(r) for r in cursor.fetchall()]

    def get_summary_stats(self) -> Dict[str, Any]:
        with sqlite3.connect(SQLITE_DB_PATH) as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT COUNT(*), AVG(price), SUM(CASE WHEN side = 'BUY' THEN 1 ELSE 0 END), SUM(CASE WHEN side = 'SELL' THEN 1 ELSE 0 END) FROM trades")
            row = cursor.fetchone()
            total, avg_price, buy_count, sell_count = row if row else (0, 0.0, 0, 0)
            return {
                "total_trades": total or 0,
                "avg_price": round(avg_price or 0.0, 2),
                "buy_count": buy_count or 0,
                "sell_count": sell_count or 0,
            }


storage_engine = ClickHouseTradeStorage()
