import os
import asyncio
import json
import pytz
from datetime import datetime
from fastapi import FastAPI, HTTPException, WebSocket, WebSocketDisconnect, Query, BackgroundTasks
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from typing import Dict, Any, List, Optional

from .data_acquisition import sync_nvda_market_data, get_latest_completed_session, load_nvda_csv
from .simulator import global_simulator, generate_trades_for_minute, generate_minute_sample_trades
from .merkle_engine import verify_trade_proof, hash_trade, canonical_cbor_serialize
from .clickhouse_storage import storage_engine
from .blockchain_committer import blockchain_committer
from .trading_engine import process_order, get_full_portfolio, get_current_price_for_ticker
from .models import OrderRequest, OrderResponse, PortfolioResponse

IST_TZ = pytz.timezone("Asia/Kolkata")

app = FastAPI(
    title="NVDA High-Throughput Verifiable Trade Simulation API",
    description="Backend platform for 100k trades/min NVDA market replay, live trading, and Ethereum L2 Merkle proof verification",
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
    proof: Optional[List[Dict[str, str]]] = None
    expected_merkle_root: Optional[str] = None


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
        "run_number": global_simulator.run_number,
        "total_runs_completed": len(global_simulator.simulation_history),
        "is_running": global_simulator.is_running,
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
        "l2_tx_hash": meta.get("l2_tx_hash", None),
        "dataset_id": meta.get("dataset_id", None)
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


@app.get("/api/simulation/timeline")
def get_simulation_timeline():
    """
    Supplies the 390-minute session replay timeline and sample trade frames
    for local client caching on the user's laptop (< 2 MB).
    """
    try:
        if global_simulator.session_df.empty:
            global_simulator.prepare_simulation()

        num_minutes = len(global_simulator.session_df)
        frames = []

        for min_idx in range(num_minutes):
            row = global_simulator.session_df.iloc[min_idx]
            sample_trades = generate_minute_sample_trades(
                minute_idx=min_idx,
                source_row=row,
                sample_count=10,
                trades_per_minute=global_simulator.trades_per_minute,
                seed=global_simulator.seed,
                simulation_date_str=global_simulator.simulation_date
            )

            frames.append({
                "minute_index": min_idx + 1,
                "total_minutes": num_minutes,
                "timestamp": sample_trades[0]["simulation_timestamp"] if sample_trades else "",
                "current_price": float(row["Close"]),
                "open": float(row["Open"]),
                "high": float(row["High"]),
                "low": float(row["Low"]),
                "volume": int(row.get("Volume", 10000)),
                "sample_trades": sample_trades
            })

        return {
            "cache_key": f"NVDA-{global_simulator.source_trading_date}-R{global_simulator.run_number}",
            "source_trading_date": global_simulator.source_trading_date,
            "simulation_date": global_simulator.simulation_date,
            "run_number": global_simulator.run_number,
            "total_minutes": num_minutes,
            "frames": frames
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/api/simulation/start")
def start_simulation(
    background_tasks: BackgroundTasks,
    runs: int = Query(1, ge=1, le=10),
    reset: bool = Query(False),
    background: bool = Query(True)
):
    """
    Executes memory-optimized 100k trades/min simulation for the completed NVDA session.
    If background=True, starts calculation asynchronously so the frontend responds instantly (< 10 ms).
    """
    try:
        if reset:
            global_simulator.reset_simulation(next_run=True)

        if runs > 1:
            if background:
                background_tasks.add_task(global_simulator.run_multiple_simulations, runs)
                return {
                    "status": "RUNNING",
                    "message": f"Started {runs} simulation runs in background.",
                    "run_number": global_simulator.run_number,
                    "runs_queued": runs,
                    "instant_start": True
                }
            else:
                results = global_simulator.run_multiple_simulations(run_count=runs)
                return {
                    "message": f"{len(results)} simulation runs completed successfully.",
                    "total_runs": len(results),
                    "runs": results,
                    "latest_result": results[-1]
                }
        else:
            if background:
                background_tasks.add_task(global_simulator.run_full_simulation)
                return {
                    "status": "RUNNING",
                    "message": f"Simulation Run #{global_simulator.run_number} started instantly in background.",
                    "run_number": global_simulator.run_number,
                    "instant_start": True
                }
            else:
                result = global_simulator.run_full_simulation()
                return {
                    "message": f"Simulation Run #{global_simulator.run_number} completed successfully.",
                    "result": result
                }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/api/simulation/reset")
def reset_simulation(next_run: bool = Query(True)):
    """
    Resets the simulator state to prepare for another run in the current session.
    Preserves user orders while resetting minute roots and simulation tables.
    """
    try:
        metadata = global_simulator.reset_simulation(next_run=next_run)
        return {
            "message": f"Simulation reset. Prepared for Run #{global_simulator.run_number}.",
            "run_number": global_simulator.run_number,
            "metadata": metadata
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/simulation/history")
def get_simulation_history():
    """
    Returns the history of all completed simulation runs in the current session.
    """
    return {
        "current_run_number": global_simulator.run_number,
        "total_runs_completed": len(global_simulator.simulation_history),
        "history": global_simulator.simulation_history
    }


@app.post("/api/orders", response_model=OrderResponse)
def place_order(order: OrderRequest):
    """
    Places and matches a BUY or SELL order while simulation is running.
    Associates the trade with a unique Trade ID and records it in verifiable storage.
    """
    try:
        res = process_order(order)
        return res
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/portfolio", response_model=PortfolioResponse)
def get_user_portfolio():
    """
    Returns current user portfolio cash balance, holdings, total equity, and executed orders.
    """
    try:
        return get_full_portfolio()
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/prices/current")
def get_live_price(ticker: str = "NVDA"):
    """
    Returns current price for ticker from the active simulation state.
    """
    price = get_current_price_for_ticker(ticker)
    return {"symbol": ticker.upper(), "price": price}


@app.get("/api/trades/{trade_id}")
def get_trade_details(trade_id: str):
    """
    Retrieves exact trade record, canonical CBOR bytes (hex), SHA-256 leaf hash, and Merkle proof path.
    Supports both user-placed orders (TRD-NVDA-...) and simulation replay trades.
    """
    clean_id = trade_id.strip()
    trade = storage_engine.get_trade_by_id(clean_id)
    if not trade:
        # Search sample buffer in simulator memory
        for t in global_simulator.all_trades_sample:
            if t["trade_id"] == clean_id:
                trade = t
                break

    if not trade and clean_id.isdigit():
        # Deterministically reconstruct on demand for valid simulation trade ID
        try:
            val_id = int(clean_id)
            min_idx = (val_id - 1) // global_simulator.trades_per_minute
            if 0 <= min_idx < len(global_simulator.session_df):
                trades, _ = generate_trades_for_minute(
                    minute_idx=min_idx,
                    source_row=global_simulator.session_df.iloc[min_idx],
                    trades_per_minute=global_simulator.trades_per_minute,
                    seed=global_simulator.seed,
                    simulation_date_str=global_simulator.simulation_date
                )
                sub_idx = (val_id - 1) % global_simulator.trades_per_minute
                trade = trades[sub_idx]
        except Exception:
            pass

    if not trade:
        raise HTTPException(status_code=404, detail=f"Trade #{clean_id} not found in database or dataset.")

    cbor_hex = "0x" + canonical_cbor_serialize(trade).hex()
    leaf_hash_hex = "0x" + hash_trade(trade).hex()

    # Get proof from simulator master merkle tree
    proof = global_simulator.get_proof_for_trade(clean_id)
    merkle_root = global_simulator.dataset_metadata.get("merkle_root", "0x0000000000000000000000000000000000000000000000000000000000000000")

    is_user_trade = str(clean_id).startswith("TRD-")

    return {
        "trade": trade,
        "trade_type": "USER_TRADE" if is_user_trade else "SIMULATION_TRADE",
        "cbor_hex": cbor_hex,
        "leaf_hash": leaf_hash_hex,
        "merkle_root": merkle_root,
        "proof": proof,
        "l2_commitment": blockchain_committer.get_latest_commitment()
    }


@app.post("/api/verify/trade")
def verify_trade(req: VerifyTradeRequest):
    """
    Independently verifies if a trade belongs to the dataset committed to Ethereum L2,
    or verifies cryptographic integrity of a user-executed trade.
    """
    calculated_leaf_hash = "0x" + hash_trade(req.trade).hex()

    if req.proof and req.expected_merkle_root:
        is_valid = verify_trade_proof(
            trade=req.trade,
            proof=req.proof,
            expected_root_hex=req.expected_merkle_root
        )
    else:
        # Check if trade exists in verified storage engine
        trade_id = req.trade.get("trade_id")
        stored = storage_engine.get_trade_by_id(str(trade_id))
        is_valid = stored is not None

    return {
        "verified": is_valid,
        "trade_id": req.trade.get("trade_id"),
        "expected_merkle_root": req.expected_merkle_root,
        "calculated_leaf_hash": calculated_leaf_hash
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
    Supports running multiple simulations consecutively in a single session without disconnecting.
    """
    await websocket.accept()

    try:
        if global_simulator.session_df.empty:
            global_simulator.prepare_simulation()

        while True:
            # Broadcast run started
            await websocket.send_json({
                "type": "RUN_STARTED",
                "run_number": global_simulator.run_number,
                "dataset_id": global_simulator.dataset_metadata.get("dataset_id", ""),
                "simulation_date": global_simulator.simulation_date,
                "source_trading_date": global_simulator.source_trading_date
            })

            num_minutes = len(global_simulator.session_df)

            for min_idx in range(num_minutes):
                row = global_simulator.session_df.iloc[min_idx]
                global_simulator.current_minute = min_idx + 1

                # Memory-optimized: produce only 20 sample trades for UI virtual list
                sample_trades = generate_minute_sample_trades(
                    minute_idx=min_idx,
                    source_row=row,
                    sample_count=20,
                    trades_per_minute=global_simulator.trades_per_minute,
                    seed=global_simulator.seed,
                    simulation_date_str=global_simulator.simulation_date
                )

                current_p = float(row["Close"])

                msg = {
                    "type": "MINUTE_TICK",
                    "run_number": global_simulator.run_number,
                    "minute_index": min_idx + 1,
                    "total_source_minutes": num_minutes,
                    "timestamp_ist": sample_trades[0]["simulation_timestamp"] if sample_trades else "",
                    "trades_generated_this_minute": global_simulator.trades_per_minute,
                    "total_generated_so_far": (min_idx + 1) * global_simulator.trades_per_minute,
                    "current_price": current_p,
                    "sample_trades": sample_trades
                }

                await websocket.send_json(msg)
                await asyncio.sleep(0.5)  # Smooth 2-Hz UI update tick

            # Final completion event: only run simulation if not already computed for this run
            if not global_simulator.dataset_metadata.get("merkle_root"):
                global_simulator.run_full_simulation()

            await websocket.send_json({
                "type": "SIMULATION_COMPLETE",
                "run_number": global_simulator.run_number,
                "metadata": global_simulator.dataset_metadata
            })

            # Keep connection alive; wait for client to trigger next run
            while True:
                msg_text = await websocket.receive_text()
                try:
                    payload = json.loads(msg_text)
                    action = str(payload.get("action", "")).upper()
                    if action in ("RESTART", "NEXT_RUN", "START"):
                        global_simulator.reset_simulation(next_run=True)
                        break  # Break inner wait loop to stream next simulation run!
                    elif action == "RESET":
                        global_simulator.reset_simulation(next_run=False)
                        await websocket.send_json({
                            "type": "RESET_COMPLETE",
                            "run_number": global_simulator.run_number
                        })
                    elif action == "PING":
                        await websocket.send_json({"type": "PONG"})
                except Exception:
                    pass

    except WebSocketDisconnect:
        pass
    except Exception as e:
        print(f"WebSocket error: {e}")
