import pytest
import os
import pandas as pd
from backend.app.data_acquisition import load_nvda_csv, sync_nvda_market_data, get_latest_completed_session
from backend.app.merkle_engine import hash_trade, MerkleTree, verify_trade_proof, canonical_cbor_serialize
from backend.app.clickhouse_storage import storage_engine
from backend.app.simulator import generate_trades_for_minute, NVDATradeSimulator
from backend.app.dataset_exporter import export_canonical_dataset
from backend.app.storage_publisher import publish_to_decentralized_storage
from backend.app.blockchain_committer import blockchain_committer


def test_merkle_engine_and_cbor():
    trade = {
        "trade_id": "000000083291",
        "simulation_timestamp": "2026-08-28T09:31:42.123+05:30",
        "source_timestamp": "2026-08-27T09:31:00-04:00",
        "symbol": "NVDA",
        "side": "BUY",
        "price": 181.42,
        "quantity": 37
    }

    cbor_bytes = canonical_cbor_serialize(trade)
    assert len(cbor_bytes) > 0

    h1 = hash_trade(trade)
    h2 = hash_trade(trade)
    assert h1 == h2  # Deterministic SHA-256

    # Test Merkle Tree
    trades = [trade]
    for i in range(1, 10):
        t_copy = trade.copy()
        t_copy["trade_id"] = f"{i:012d}"
        trades.append(t_copy)

    hashes = [hash_trade(t) for t in trades]
    tree = MerkleTree(hashes)
    assert tree.root_hex.startswith("0x")

    proof = tree.get_proof(0)
    is_valid = verify_trade_proof(trade, proof, tree.root_hex)
    assert is_valid is True


def test_clickhouse_storage():
    storage_engine.clear_simulation_data()
    sample_trades = [
        {
            "trade_id": "000000000001",
            "simulation_timestamp": "2026-08-28T09:30:00.000+05:30",
            "source_timestamp": "2026-08-27T09:30:00-04:00",
            "symbol": "NVDA",
            "side": "BUY",
            "price": 180.50,
            "quantity": 100
        }
    ]
    storage_engine.insert_trades_batch(sample_trades, minute_index=0, simulation_date="2026-08-28")
    t = storage_engine.get_trade_by_id("000000000001")
    assert t is not None
    assert t["symbol"] == "NVDA"
    assert t["price"] == 180.50


def test_simulator_and_acquisition():
    # Ensure NVDA.csv has at least basic test rows
    df = load_nvda_csv()
    if df.empty:
        df = pd.DataFrame([
            {"Datetime": "2026-08-27 09:30:00", "Open": 180.0, "High": 182.0, "Low": 179.5, "Close": 181.0, "Volume": 10000},
            {"Datetime": "2026-08-27 09:31:00", "Open": 181.0, "High": 182.5, "Low": 180.5, "Close": 181.5, "Volume": 12000},
        ])
        from backend.app.data_acquisition import save_nvda_csv
        save_nvda_csv(df)

    session_df, session_date = get_latest_completed_session(df)
    assert not session_df.empty

    row = session_df.iloc[0]
    trades, hashes = generate_trades_for_minute(0, row, trades_per_minute=100, seed=42)
    assert len(trades) == 100
    assert len(hashes) == 100
    assert trades[0]["symbol"] == "NVDA"
