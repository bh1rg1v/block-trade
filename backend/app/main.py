import os
import asyncio
import json
import pytz
from datetime import datetime
from fastapi import FastAPI, HTTPException, WebSocket, WebSocketDisconnect, Query
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from typing import Dict, Any, List, Optional

from .data_acquisition import sync_nvda_market_data, get_latest_completed_session, load_nvda_csv
from .simulator import global_simulator, generate_trades_for_minute
from .merkle_engine import verify_trade_proof, hash_trade, canonical_cbor_serialize
from .clickhouse_storage import storage_engine
from .blockchain_committer import blockchain_committer

IST_TZ = pytz.timezone("Asia/Kolkata")

app = FastAPI(
    title="NVDA High-Throughput Verifiable Trade Simulation API",
    description="Backend platform for 100k trades/min NVDA market replay and Ethereum L2 Merkle proof verification",
    version="1.0.0"
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


class VerifyTradeRequest(BaseModel):
    trade: Dict[str, Any]
    proof: List[Dict[str, str]]
    expected_merkle_root: str


@app.get("/api/simulation/status")
def get_simulation_status():
    """
    Returns live simulation progress, IST time, source trading date, trade counts, and metrics.
    """
    now_ist = datetime.now(IST_TZ).strftime("%Y-%m-%d %H:%M:%S IST")
    meta = global_simulator.dataset_metadata

    summary_stats = storage_engine.get_summary_stats()

    return {
        "status": meta.get("status", "READY"),
        "current_time_ist": now_ist,
        "symbol": "NVDA",
        "source_trading_date": meta.get("source_trading_date", "Pending Sync"),
        "simulation_date": meta.get("simulation_date", datetime.now(IST_TZ).strftime("%Y-%m-%d")),
        "current_minute": global_simulator.current_minute,
        "total_source_minutes": meta.get("total_source_minutes", 390),
        "target_rate_tpm": 100_000,
        "target_rate_tps": 1667,
        "total_generated_trades": global_simulator.total_generated_trades or summary_stats["total_trades"],
        "buy_count": summary_stats["buy_count"],
        "sell_count": summary_stats["sell_count"],
        "avg_price": summary_stats["avg_price"],
        "merkle_root": meta.get("merkle_root", None),
        "dataset_hash": meta.get("dataset_hash", None),
        "ipfs_cid": meta.get("ipfs_cid", None),
        "l2_tx_hash": meta.get("l2_tx_hash", None)
    }


@app.post("/api/simulation/sync")
def sync_market_data():
    """
    Daily Data Acquisition Flow:
    yfinance -> Latest NVDA 1m data -> Validate -> Deduplicate -> Merge NVDA.csv -> Select completed session.
    """
    try:
        df = sync_nvda_market_data()
        session_df, session_date = get_latest_completed_session(df)
        metadata = global_simulator.prepare_simulation()
        return {
            "message": "NVDA.csv updated successfully.",
            "total_records_in_csv": len(df),
            "completed_session_date": session_date,
            "session_minutes": len(session_df),
            "metadata": metadata
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/api/simulation/start")
def start_simulation():
    """
    Executes the 100k trades/min simulation for the completed NVDA session,
    constructs the Merkle tree, exports Parquet dataset, publishes to IPFS, and commits to Ethereum L2.
    """
    try:
        result = global_simulator.run_full_simulation()
        return {
            "message": "Simulation completed successfully.",
            "result": result
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/trades/{trade_id}")
def get_trade_details(trade_id: str):
    """
    Retrieves exact trade record, canonical CBOR bytes (hex), SHA-256 leaf hash, and Merkle proof path.
    """
    trade = storage_engine.get_trade_by_id(trade_id)
    if not trade:
        # Search sample buffer in simulator memory
        for t in global_simulator.all_trades_sample:
            if t["trade_id"] == trade_id:
                trade = t
                break

    if not trade:
        # Generate on-demand if valid ID range
        try:
            val_id = int(trade_id)
            min_idx = (val_id - 1) // 100_000
            if 0 <= min_idx < len(global_simulator.session_df):
                trades, _ = generate_trades_for_minute(
                    minute_idx=min_idx,
                    source_row=global_simulator.session_df.iloc[min_idx],
                    trades_per_minute=global_simulator.trades_per_minute,
                    seed=global_simulator.seed,
                    simulation_date_str=global_simulator.simulation_date
                )
                sub_idx = (val_id - 1) % 100_000
                trade = trades[sub_idx]
        except Exception:
            pass

    if not trade:
        raise HTTPException(status_code=404, detail=f"Trade #{trade_id} not found.")

    cbor_hex = "0x" + canonical_cbor_serialize(trade).hex()
    leaf_hash_hex = "0x" + hash_trade(trade).hex()

    proof = []
    merkle_root = global_simulator.dataset_metadata.get("merkle_root", "0x0000000000000000000000000000000000000000000000000000000000000000")

    if global_simulator.master_merkle_tree:
        try:
            val_id = int(trade_id) - 1
            if 0 <= val_id < len(global_simulator.master_merkle_tree.leaf_hashes):
                proof = global_simulator.master_merkle_tree.get_proof(val_id)
        except Exception:
            pass

    return {
        "trade": trade,
        "cbor_hex": cbor_hex,
        "leaf_hash": leaf_hash_hex,
        "merkle_root": merkle_root,
        "proof": proof,
        "l2_commitment": blockchain_committer.get_latest_commitment()
    }


@app.post("/api/verify/trade")
def verify_trade(req: VerifyTradeRequest):
    """
    Independently verifies if a trade belongs to the dataset committed to Ethereum L2.
    """
    is_valid = verify_trade_proof(
        trade=req.trade,
        proof=req.proof,
        expected_root_hex=req.expected_merkle_root
    )
    return {
        "verified": is_valid,
        "trade_id": req.trade.get("trade_id"),
        "expected_merkle_root": req.expected_merkle_root,
        "calculated_leaf_hash": "0x" + hash_trade(req.trade).hex()
    }


@app.get("/api/dataset/latest")
def get_latest_dataset_commitment():
    """
    Returns latest canonical dataset metadata, IPFS CID, and Ethereum L2 commitment.
    """
    latest_l2 = blockchain_committer.get_latest_commitment()
    return {
        "metadata": global_simulator.dataset_metadata,
        "l2_commitment": latest_l2
    }


@app.websocket("/ws/simulation")
async def websocket_simulation_feed(websocket: WebSocket):
    """
    WebSocket streaming endpoint pushing real-time trade updates to React frontend.
    """
    await websocket.accept()

    try:
        if global_simulator.session_df.empty:
            global_simulator.prepare_simulation()

        num_minutes = len(global_simulator.session_df)

        for min_idx in range(num_minutes):
            row = global_simulator.session_df.iloc[min_idx]
            trades, _ = generate_trades_for_minute(
                minute_idx=min_idx,
                source_row=row,
                trades_per_minute=100_000,
                seed=global_simulator.seed,
                simulation_date_str=global_simulator.simulation_date
            )

            # Send top 20 sample trades for frontend virtual list feed + minute aggregate metrics
            msg = {
                "type": "MINUTE_TICK",
                "minute_index": min_idx + 1,
                "total_source_minutes": num_minutes,
                "timestamp_ist": trades[0]["simulation_timestamp"],
                "trades_generated_this_minute": 100_000,
                "total_generated_so_far": (min_idx + 1) * 100_000,
                "current_price": float(row["Close"]),
                "sample_trades": trades[:20]
            }

            await websocket.send_json(msg)
            await asyncio.sleep(0.5)  # Smooth 2-Hz UI update tick

        # Final completion event
        if not global_simulator.dataset_metadata.get("merkle_root"):
            global_simulator.run_full_simulation()

        await websocket.send_json({
            "type": "SIMULATION_COMPLETE",
            "metadata": global_simulator.dataset_metadata
        })

    except WebSocketDisconnect:
        pass
    except Exception as e:
        print(f"WebSocket error: {e}")
