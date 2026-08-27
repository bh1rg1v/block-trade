import os
import time
import hashlib
import numpy as np
import pandas as pd
import pytz
from datetime import datetime, timedelta
from typing import List, Dict, Any, Generator, Tuple

from .data_acquisition import sync_nvda_market_data, get_latest_completed_session
from .merkle_engine import hash_trade, MerkleTree
from .clickhouse_storage import storage_engine
from .dataset_exporter import export_canonical_dataset
from .storage_publisher import publish_to_decentralized_storage
from .blockchain_committer import blockchain_committer

IST_TZ = pytz.timezone("Asia/Kolkata")
ET_TZ = pytz.timezone("America/New_York")


def generate_trades_for_minute(
    minute_idx: int,
    source_row: pd.Series,
    trades_per_minute: int = 100_000,
    seed: int = 42,
    simulation_date_str: str = "2026-08-28"
) -> Tuple[List[Dict[str, Any]], List[bytes]]:
    """
    Vectorized NumPy generator producing `trades_per_minute` (100,000) deterministic trades for a given market minute observation.
    Returns (trades_list, leaf_hashes_list).
    """
    # Deterministic RNG derived from master seed + minute index
    rng = np.random.default_rng(seed + minute_idx)

    open_p = float(source_row["Open"])
    high_p = float(source_row["High"])
    low_p = float(source_row["Low"])
    close_p = float(source_row["Close"])
    source_time_str = str(source_row["Datetime"])

    # Fast bounds calculation
    min_p = min(open_p, close_p, low_p)
    max_p = max(open_p, close_p, high_p)
    span = max(0.01, max_p - min_p)

    N = trades_per_minute

    # Generate random prices and quantities
    rand_floats = rng.random(N, dtype=np.float32)
    prices = np.round(min_p + rand_floats * span, 2)
    quantities = rng.integers(1, 500, size=N, dtype=np.int32)
    sides = rng.choice(["BUY", "SELL"], size=N)

    # Base IST timestamp for this minute: 09:30 IST + minute_idx minutes
    base_ist_time = datetime.strptime(f"{simulation_date_str} 09:30:00", "%Y-%m-%d %H:%M:%S") + timedelta(minutes=minute_idx)
    base_ist_str = base_ist_time.strftime("%Y-%m-%d")

    # Generate trade records
    trades = []
    leaf_hashes = []
    base_id = minute_idx * N + 1

    for i in range(N):
        t_id = f"{base_id + i:012d}"
        # Spread timestamps evenly over 60 seconds
        sub_sec = (i * 60.0) / N
        sec = int(sub_sec)
        msec = int((sub_sec - sec) * 1000)
        sim_ts = f"{simulation_date_str}T{base_ist_time.hour:02d}:{base_ist_time.minute:02d}:{sec:02d}.{msec:03d}+05:30"
        src_ts = f"{source_time_str}-04:00"

        trade = {
            "trade_id": t_id,
            "simulation_timestamp": sim_ts,
            "source_timestamp": src_ts,
            "symbol": "NVDA",
            "side": str(sides[i]),
            "price": float(prices[i]),
            "quantity": int(quantities[i])
        }
        trades.append(trade)

        # Hash for Merkle tree
        h = hash_trade(trade)
        leaf_hashes.append(h)

    return trades, leaf_hashes


class NVDATradeSimulator:
    """
    Stateful NVDA High-Throughput Trade Simulator Engine.
    Replays completed 1m U.S. trading session from 09:30 IST to 16:00 IST at 100,000 trades/minute.
    """

    def __init__(self, seed: int = 42, trades_per_minute: int = 100_000):
        self.seed = seed
        self.trades_per_minute = trades_per_minute
        self.simulator_version = "v1.0.0"
        self.is_running = False
        self.current_minute = 0
        self.total_generated_trades = 0
        self.session_df = pd.DataFrame()
        self.source_trading_date = ""
        self.simulation_date = datetime.now(IST_TZ).strftime("%Y-%m-%d")
        self.leaf_hashes: List[bytes] = []
        self.master_merkle_tree: Optional[MerkleTree] = None
        self.dataset_metadata: Dict[str, Any] = {}
        self.all_trades_sample: List[Dict[str, Any]] = []

    def prepare_simulation(self) -> Dict[str, Any]:
        """
        Pre-simulation step:
        1. Fetch & sync latest NVDA 1m market data -> NVDA.csv.
        2. Identify most recent completed U.S. trading session.
        3. Precompute metadata and hash commitments.
        """
        sync_nvda_market_data()
        self.session_df, self.source_trading_date = get_latest_completed_session()

        if self.session_df.empty:
            raise ValueError("No valid completed U.S. trading session found in NVDA.csv.")

        source_bytes = self.session_df.to_csv(index=False).encode("utf-8")
        source_dataset_hash = "0x" + hashlib.sha256(source_bytes).hexdigest()

        config_str = f"symbol=NVDA;tpm={self.trades_per_minute};seed={self.seed};ver={self.simulator_version}"
        config_hash = "0x" + hashlib.sha256(config_str.encode("utf-8")).hexdigest()

        dataset_id = f"NVDA-SIM-{self.simulation_date}-{self.source_trading_date}"

        self.dataset_metadata = {
            "dataset_id": dataset_id,
            "symbol": "NVDA",
            "source_trading_date": self.source_trading_date,
            "simulation_date": self.simulation_date,
            "total_source_minutes": len(self.session_df),
            "expected_trade_count": len(self.session_df) * self.trades_per_minute,
            "trades_per_minute": self.trades_per_minute,
            "source_dataset_hash": source_dataset_hash,
            "simulator_version": self.simulator_version,
            "simulation_config_hash": config_hash,
            "random_seed": self.seed,
            "status": "PREPARED"
        }

        # Clear previous simulation state from ClickHouse storage
        storage_engine.clear_simulation_data()
        self.current_minute = 0
        self.total_generated_trades = 0
        self.leaf_hashes.clear()
        self.all_trades_sample.clear()

        return self.dataset_metadata

    def run_full_simulation(self, sample_storage_limit: int = 100_000) -> Dict[str, Any]:
        """
        Executes complete simulation run generating 100k trades/min across all session minutes.
        Builds Merkle Tree, exports Parquet dataset, publishes to IPFS, and commits to Ethereum L2.
        """
        if not self.dataset_metadata or self.session_df.empty:
            self.prepare_simulation()

        start_time = time.time()
        num_minutes = len(self.session_df)

        print(f"[*] Starting NVDA simulation for {num_minutes} minutes at {self.trades_per_minute} trades/min...")

        all_leaf_hashes = []
        sample_trades = []

        for min_idx in range(num_minutes):
            row = self.session_df.iloc[min_idx]
            trades, hashes = generate_trades_for_minute(
                minute_idx=min_idx,
                source_row=row,
                trades_per_minute=self.trades_per_minute,
                seed=self.seed,
                simulation_date_str=self.simulation_date
            )

            all_leaf_hashes.extend(hashes)
            self.total_generated_trades += len(trades)
            self.current_minute = min_idx + 1

            # Persist batch to ClickHouse operational storage (sample for fast query)
            clickhouse_batch = trades[:min(1000, len(trades))]
            storage_engine.insert_trades_batch(clickhouse_batch, minute_index=min_idx, simulation_date=self.simulation_date)

            if len(sample_trades) < sample_storage_limit:
                sample_trades.extend(trades[:min(500, len(trades))])

        # Store sample trades for verification UI
        self.all_trades_sample = sample_trades

        # Build Merkle Tree over generated trade hashes
        print("[*] Constructing cryptographic Merkle tree...")
        self.master_merkle_tree = MerkleTree(all_leaf_hashes)
        merkle_root_hex = self.master_merkle_tree.root_hex

        # Export Canonical Parquet Dataset
        print("[*] Exporting canonical Parquet dataset compressed with Zstandard...")
        export_result = export_canonical_dataset(
            trades=sample_trades,
            metadata=self.dataset_metadata
        )

        # Publish to IPFS / Arweave
        print("[*] Publishing dataset to IPFS / Arweave decentralized storage...")
        storage_pub = publish_to_decentralized_storage(
            parquet_path=export_result["parquet_path"],
            metadata=export_result["metadata"]
        )

        # Anchor commitment on Ethereum L2
        print("[*] Anchoring Merkle root and dataset commitment on Ethereum L2...")
        l2_commitment = blockchain_committer.commit_dataset(
            dataset_id=self.dataset_metadata["dataset_id"],
            dataset_hash=export_result["dataset_hash"],
            merkle_root=merkle_root_hex,
            trade_count=self.total_generated_trades,
            dataset_uri=storage_pub["dataset_uri"]
        )

        elapsed = max(0.001, time.time() - start_time)
        tps = self.total_generated_trades / elapsed

        self.dataset_metadata.update({
            "status": "COMPLETED",
            "actual_trade_count": self.total_generated_trades,
            "elapsed_seconds": round(elapsed, 3),
            "throughput_tps": round(tps, 2),
            "merkle_root": merkle_root_hex,
            "dataset_hash": export_result["dataset_hash"],
            "ipfs_cid": storage_pub["ipfs_cid"],
            "dataset_uri": storage_pub["dataset_uri"],
            "l2_tx_hash": l2_commitment["tx_hash"],
            "l2_contract": l2_commitment["contract_address"]
        })

        return self.dataset_metadata


global_simulator = NVDATradeSimulator()
