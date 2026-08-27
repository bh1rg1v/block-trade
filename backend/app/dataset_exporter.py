import os
import json
import hashlib
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from typing import List, Dict, Any

DATASET_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", "dataset"))


def export_canonical_dataset(
    trades: List[Dict[str, Any]],
    metadata: Dict[str, Any],
    output_dir: str = DATASET_DIR
) -> Dict[str, Any]:
    """
    Exports generated trades to Apache Parquet format compressed with Zstandard,
    calculates dataset SHA-256 hash, and saves metadata.json.
    """
    os.makedirs(output_dir, exist_ok=True)

    df = pd.DataFrame(trades)

    # Standardize column types
    df["trade_id"] = df["trade_id"].astype(str)
    df["simulation_timestamp"] = df["simulation_timestamp"].astype(str)
    df["source_timestamp"] = df["source_timestamp"].astype(str)
    df["symbol"] = df["symbol"].astype(str)
    df["side"] = df["side"].astype(str)
    df["price"] = df["price"].astype(float)
    df["quantity"] = df["quantity"].astype(int)

    parquet_path = os.path.join(output_dir, "trades.parquet")
    table = pa.Table.from_pandas(df)
    pq.write_table(table, parquet_path, compression="zstd")

    # Compute binary dataset SHA-256 hash
    hasher = hashlib.sha256()
    with open(parquet_path, "rb") as f:
        while chunk := f.read(65536):
            hasher.update(chunk)
    dataset_hash = "0x" + hasher.hexdigest()

    metadata_copy = metadata.copy()
    metadata_copy["dataset_hash"] = dataset_hash
    metadata_copy["trade_count"] = len(trades)
    metadata_copy["file_size_bytes"] = os.path.getsize(parquet_path)

    metadata_path = os.path.join(output_dir, "metadata.json")
    with open(metadata_path, "w") as f:
        json.dump(metadata_copy, f, indent=2)

    return {
        "parquet_path": parquet_path,
        "metadata_path": metadata_path,
        "dataset_hash": dataset_hash,
        "trade_count": len(trades),
        "metadata": metadata_copy
    }
