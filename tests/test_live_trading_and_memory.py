import pytest
from backend.app.models import OrderRequest
from backend.app.trading_engine import process_order, get_full_portfolio
from backend.app.clickhouse_storage import storage_engine
from backend.app.merkle_engine import hash_trade, TwoTierMerkleTree, compute_merkle_root_streaming, verify_trade_proof, MerkleTree
from backend.app.simulator import generate_trades_for_minute, generate_minute_sample_trades
import pandas as pd


def test_two_tier_merkle_tree():
    # Create 3 minutes with 10 trades each
    minute_roots = []
    all_minute_trades = []
    all_minute_hashes = []

    row = pd.Series({"Datetime": "2026-08-27 09:30:00", "Open": 180.0, "High": 182.0, "Low": 179.5, "Close": 181.0, "Volume": 10000})

    for m in range(3):
        trades, hashes = generate_trades_for_minute(m, row, trades_per_minute=10, seed=42)
        all_minute_trades.append(trades)
        all_minute_hashes.append(hashes)
        m_root = compute_merkle_root_streaming(hashes)
        minute_roots.append(m_root)

    tree = TwoTierMerkleTree(minute_roots)
    assert tree.root_hex.startswith("0x")

    # Generate proof for minute 1, trade 3
    target_trade = all_minute_trades[1][3]
    proof = tree.get_two_tier_proof(minute_idx=1, leaf_idx=3, minute_leaf_hashes=all_minute_hashes[1])

    assert len(proof) > 0
    is_valid = verify_trade_proof(target_trade, proof, tree.root_hex)
    assert is_valid is True


def test_lightweight_minute_sample_generator():
    row = pd.Series({"Datetime": "2026-08-27 09:30:00", "Open": 180.0, "High": 182.0, "Low": 179.5, "Close": 181.0, "Volume": 10000})
    sample = generate_minute_sample_trades(0, row, sample_count=20, trades_per_minute=100_000)
    assert len(sample) == 20
    assert sample[0]["symbol"] == "NVDA"
    assert sample[0]["trade_id"] == "000000000001"


def test_buy_and_sell_order_execution():
    # Test BUY order
    buy_order = OrderRequest(
        ticker="NVDA",
        side="BUY",
        order_type="MARKET",
        quantity=5,
        price=180.0
    )
    buy_res = process_order(buy_order)
    assert buy_res.status == "FILLED"
    assert buy_res.trade_id.startswith("TRD-NVDA-")
    assert buy_res.filled_price > 0
    assert buy_res.leaf_hash.startswith("0x")

    # Verify trade is retrievable from storage by unique trade ID
    stored_trade = storage_engine.get_trade_by_id(buy_res.trade_id)
    assert stored_trade is not None
    assert stored_trade["trade_id"] == buy_res.trade_id
    assert stored_trade["side"] == "BUY"
    assert stored_trade["quantity"] == 5

    # Test SELL order
    sell_order = OrderRequest(
        ticker="NVDA",
        side="SELL",
        order_type="MARKET",
        quantity=2,
        price=180.0
    )
    sell_res = process_order(sell_order)
    assert sell_res.status == "FILLED"
    assert sell_res.trade_id.startswith("TRD-NVDA-")
    assert sell_res.trade_id != buy_res.trade_id  # Ensure IDs are unique

    # Check portfolio reflects trades
    port = get_full_portfolio()
    assert port.cash > 0
    pos = next((p for p in port.positions if p.ticker == "NVDA"), None)
    assert pos is not None
    assert pos.shares >= 3


def test_api_orders_and_verification():
    from fastapi.testclient import TestClient
    from backend.app.main import app

    client = TestClient(app)

    # 1. Place order via API
    resp = client.post("/api/orders", json={
        "ticker": "NVDA",
        "side": "BUY",
        "order_type": "MARKET",
        "quantity": 10,
        "price": 180.0
    })
    assert resp.status_code == 200
    data = resp.json()
    assert data["status"] == "FILLED"
    assert data["trade_id"].startswith("TRD-NVDA-")
    assert data["leaf_hash"].startswith("0x")
    user_trade_id = data["trade_id"]

    # 2. Retrieve trade by ID
    get_resp = client.get(f"/api/trades/{user_trade_id}")
    assert get_resp.status_code == 200
    trade_data = get_resp.json()
    assert trade_data["trade"]["trade_id"] == user_trade_id
    assert trade_data["trade"]["side"] == "BUY"
    assert trade_data["trade"]["quantity"] == 10
    assert trade_data["leaf_hash"] == data["leaf_hash"]
    assert trade_data["trade_type"] == "USER_TRADE"

    # 3. Verify user trade
    verify_resp = client.post("/api/verify/trade", json={
        "trade": trade_data["trade"],
        "expected_merkle_root": trade_data["merkle_root"]
    })
    assert verify_resp.status_code == 200
    v_data = verify_resp.json()
    assert v_data["verified"] is True
    assert v_data["trade_id"] == user_trade_id

    # 4. Verify non-existent trade returns 404
    non_existent = client.get("/api/trades/NON_EXISTENT_TRADE_999999")
    assert non_existent.status_code == 404


def test_user_trades_preserved_across_simulation_runs():
    from backend.app.models import OrderRequest
    from backend.app.trading_engine import process_order

    # Place a user trade
    order = OrderRequest(
        ticker="NVDA",
        side="BUY",
        order_type="MARKET",
        quantity=7,
        price=181.5
    )
    res = process_order(order)
    user_trade_id = res.trade_id
    assert storage_engine.get_trade_by_id(user_trade_id) is not None

    # Clear simulation data (as happens when a new simulation starts)
    storage_engine.clear_simulation_data()

    # User trade MUST still be present in storage!
    retained_trade = storage_engine.get_trade_by_id(user_trade_id)
    assert retained_trade is not None
    assert retained_trade["trade_id"] == user_trade_id
    assert retained_trade["quantity"] == 7


def test_multiple_simulation_runs_lifecycle():
    from backend.app.simulator import NVDATradeSimulator

    sim = NVDATradeSimulator(seed=100, trades_per_minute=10)
    meta1 = sim.prepare_simulation()
    assert meta1["run_number"] == 1
    assert "-R1" in meta1["dataset_id"]

    # Run 1
    res1 = sim.run_full_simulation(sample_storage_limit=50)
    assert res1["run_number"] == 1
    assert len(sim.minute_roots) == len(sim.session_df)
    assert len(sim.simulation_history) == 1
    root1 = res1["merkle_root"]

    # Reset for Run 2
    meta2 = sim.reset_simulation(next_run=True)
    assert meta2["run_number"] == 2
    assert "-R2" in meta2["dataset_id"]
    assert sim.current_minute == 0
    assert len(sim.minute_roots) == 0  # No state leakage!

    # Run 2
    res2 = sim.run_full_simulation(sample_storage_limit=50)
    assert res2["run_number"] == 2
    assert len(sim.minute_roots) == len(sim.session_df)  # Still exactly 390, NOT 780!
    assert len(sim.simulation_history) == 2
    root2 = res2["merkle_root"]

    # Merkle roots should both be valid 0x hex strings
    assert root1.startswith("0x")
    assert root2.startswith("0x")


def test_api_multi_run_and_history():
    from fastapi.testclient import TestClient
    from backend.app.main import app

    client = TestClient(app)

    # 1. Check status includes run_number
    status_res = client.get("/api/simulation/status")
    assert status_res.status_code == 200
    s_data = status_res.json()
    assert "run_number" in s_data
    assert "total_runs_completed" in s_data

    # 2. Reset endpoint
    reset_res = client.post("/api/simulation/reset?next_run=true")
    assert reset_res.status_code == 200
    r_data = reset_res.json()
    assert r_data["run_number"] >= 2

    # 3. History endpoint
    hist_res = client.get("/api/simulation/history")
    assert hist_res.status_code == 200
    h_data = hist_res.json()
    assert "current_run_number" in h_data
    assert "history" in h_data


def test_simulation_timeline_and_instant_start():
    from fastapi.testclient import TestClient
    from backend.app.main import app
    from backend.app.simulator import global_simulator

    client = TestClient(app)

    # Test timeline endpoint for laptop client caching (< 2 MB)
    timeline_res = client.get("/api/simulation/timeline")
    assert timeline_res.status_code == 200
    t_data = timeline_res.json()
    assert "cache_key" in t_data
    assert "frames" in t_data
    assert len(t_data["frames"]) > 0
    first_frame = t_data["frames"][0]
    assert "current_price" in first_frame
    assert "sample_trades" in first_frame
    assert len(first_frame["sample_trades"]) > 0

    # Test instant background start endpoint (non-blocking)
    # Set trades_per_minute small during test so TestClient's synchronous background drain finishes in <0.5s
    orig_tpm = global_simulator.trades_per_minute
    try:
        global_simulator.trades_per_minute = 5
        start_res = client.post("/api/simulation/start?background=true")
        assert start_res.status_code == 200
        s_data = start_res.json()
        assert s_data["status"] == "RUNNING"
        assert s_data["instant_start"] is True
    finally:
        global_simulator.trades_per_minute = orig_tpm
