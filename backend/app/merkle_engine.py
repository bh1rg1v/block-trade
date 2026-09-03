import hashlib
import cbor2
from typing import List, Dict, Any, Tuple

def canonical_cbor_serialize(trade: Dict[str, Any]) -> bytes:
    """
    Serializes a trade dict into deterministic (canonical) CBOR bytes.
    Keys are sorted canonically.
    """
    # Sort keys canonically
    canonical_trade = {
        "price": float(trade["price"]),
        "quantity": int(trade["quantity"]),
        "side": str(trade["side"]),
        "simulation_timestamp": str(trade["simulation_timestamp"]),
        "source_timestamp": str(trade["source_timestamp"]),
        "symbol": str(trade["symbol"]),
        "trade_id": str(trade["trade_id"])
    }
    return cbor2.dumps(canonical_trade, canonical=True)


def hash_trade(trade: Dict[str, Any]) -> bytes:
    """
    Computes SHA-256 hash of canonical CBOR representation of a trade.
    Returns 32-byte SHA-256 binary digest.
    """
    cbor_bytes = canonical_cbor_serialize(trade)
    return hashlib.sha256(cbor_bytes).digest()


def hash_pair(left: bytes, right: bytes) -> bytes:
    """
    Computes SHA-256 hash of combined left and right node hashes.
    """
    return hashlib.sha256(left + right).digest()


class MerkleTree:
    """
    High-performance Merkle Tree implementation supporting binary SHA-256 hashing,
    proof generation, and root verification.
    """
    def __init__(self, leaf_hashes: List[bytes]):
        if not leaf_hashes:
            raise ValueError("Cannot construct Merkle tree with empty leaves.")
        
        self.leaf_hashes = leaf_hashes
        self.levels: List[List[bytes]] = [leaf_hashes]

        current_level = leaf_hashes
        while len(current_level) > 1:
            next_level = []
            for i in range(0, len(current_level), 2):
                left = current_level[i]
                right = current_level[i + 1] if i + 1 < len(current_level) else left
                next_level.append(hash_pair(left, right))
            self.levels.append(next_level)
            current_level = next_level

    @property
    def root(self) -> bytes:
        return self.levels[-1][0]

    @property
    def root_hex(self) -> str:
        return "0x" + self.root.hex()

    def get_proof(self, index: int) -> List[Dict[str, str]]:
        """
        Generates Merkle proof for leaf at `index`.
        Proof format: list of dicts {"position": "left" | "right", "hash": "0x..."}.
        """
        if index < 0 or index >= len(self.leaf_hashes):
            raise IndexError("Trade index out of bounds for Merkle proof generation.")

        proof = []
        curr_idx = index
        for level in self.levels[:-1]:
            is_right = (curr_idx % 2 == 1)
            sibling_idx = curr_idx - 1 if is_right else curr_idx + 1

            if sibling_idx < len(level):
                sibling_hash = level[sibling_idx]
            else:
                sibling_hash = level[curr_idx]  # Duplicated rightmost leaf

            proof.append({
                "position": "left" if is_right else "right",
                "hash": "0x" + sibling_hash.hex()
            })
            curr_idx //= 2

        return proof


def compute_merkle_root_streaming(leaf_hashes: List[bytes]) -> bytes:
    """
    Computes Merkle root for a list of leaf hashes without retaining intermediate levels in memory.
    """
    if not leaf_hashes:
        raise ValueError("Cannot compute Merkle root with empty leaves.")

    current = leaf_hashes
    while len(current) > 1:
        next_level = []
        for i in range(0, len(current), 2):
            left = current[i]
            right = current[i + 1] if i + 1 < len(current) else left
            next_level.append(hash_pair(left, right))
        current = next_level
    return current[0]


class TwoTierMerkleTree:
    """
    High-scalability Two-Tier Merkle Tree for millions of trades.
    Tier 1: Minute sub-trees (computed and discarded per minute).
    Tier 2: Master tree constructed across all minute roots (O(minutes) memory ~12 KB).
    Full 26-step Merkle proofs are generated on demand.
    """
    def __init__(self, minute_roots: List[bytes]):
        if not minute_roots:
            raise ValueError("Cannot construct TwoTierMerkleTree with empty minute roots.")
        self.minute_roots = minute_roots
        self.master_tree = MerkleTree(minute_roots)

    @property
    def root(self) -> bytes:
        return self.master_tree.root

    @property
    def root_hex(self) -> str:
        return self.master_tree.root_hex

    def get_two_tier_proof(self, minute_idx: int, leaf_idx: int, minute_leaf_hashes: List[bytes]) -> List[Dict[str, str]]:
        """
        Constructs full deterministic Merkle proof path on demand:
        [Proof inside minute subtree (17 steps)] + [Proof of minute root in master tree (9 steps)].
        """
        if minute_idx < 0 or minute_idx >= len(self.minute_roots):
            raise IndexError("Minute index out of bounds.")
        if leaf_idx < 0 or leaf_idx >= len(minute_leaf_hashes):
            raise IndexError("Leaf index out of bounds in minute subtree.")

        # Minute level proof
        sub_tree = MerkleTree(minute_leaf_hashes)
        minute_proof = sub_tree.get_proof(leaf_idx)

        # Master level proof
        master_proof = self.master_tree.get_proof(minute_idx)

        return minute_proof + master_proof


def verify_trade_proof(trade: Dict[str, Any], proof: List[Dict[str, str]], expected_root_hex: str) -> bool:
    """
    Independently verifies if a trade belongs to the Merkle tree with `expected_root_hex`.
    """
    current_hash = hash_trade(trade)

    for step in proof:
        sibling_hash = bytes.fromhex(step["hash"].replace("0x", ""))
        if step["position"] == "left":
            current_hash = hash_pair(sibling_hash, current_hash)
        else:
            current_hash = hash_pair(current_hash, sibling_hash)

    calculated_root_hex = "0x" + current_hash.hex()
    return calculated_root_hex.lower() == expected_root_hex.lower()
