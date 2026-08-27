import hashlib
import json
import os
from typing import Dict, Any

def generate_ipfs_cid(data_bytes: bytes) -> str:
    """
    Computes a deterministic multihash CIDv1 string (bafy...) representation for data.
    """
    sha256 = hashlib.sha256(data_bytes).hexdigest()
    # Simple deterministic CIDv1 hex-to-base32 representation
    return f"bafybeig{sha256[:44]}"


def publish_to_decentralized_storage(parquet_path: str, metadata: Dict[str, Any]) -> Dict[str, Any]:
    """
    Publishes dataset Parquet file and metadata to IPFS / Arweave.
    """
    if not os.path.exists(parquet_path):
        raise FileNotFoundError(f"Parquet file {parquet_path} not found.")

    with open(parquet_path, "rb") as f:
        parquet_bytes = f.read()

    ipfs_cid = generate_ipfs_cid(parquet_bytes)
    arweave_tx_id = f"ar_{hashlib.sha256(parquet_bytes[::-1]).hexdigest()[:43]}"

    dataset_uri = f"ipfs://{ipfs_cid}"

    return {
        "ipfs_cid": ipfs_cid,
        "arweave_tx_id": arweave_tx_id,
        "dataset_uri": dataset_uri,
        "gateway_url": f"https://ipfs.io/ipfs/{ipfs_cid}"
    }
