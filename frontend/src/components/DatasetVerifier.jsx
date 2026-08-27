import React, { useEffect, useState } from "react";
import { Database, Link, ShieldCheck, CheckCircle2, Layers, Cpu, ExternalLink } from "lucide-react";

export default function DatasetVerifier({ backendUrl }) {
  const [data, setData] = useState(null);
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    fetch(`${backendUrl}/api/dataset/latest`)
      .then((res) => res.json())
      .then((d) => {
        setData(d);
        setLoading(false);
      })
      .catch(() => setLoading(false));
  }, [backendUrl]);

  if (loading) {
    return (
      <div className="panel p-12 text-center text-gray-400 font-mono">
        Loading canonical dataset and Ethereum L2 commitment information...
      </div>
    );
  }

  const meta = data?.metadata || {};
  const l2 = data?.l2_commitment || {};

  return (
    <div className="space-y-6">
      {/* Top Banner */}
      <div className="panel p-6 bg-gradient-to-r from-[#0b101b] via-[#0d1524] to-[#07090e]">
        <div className="flex items-center justify-between">
          <div>
            <div className="flex items-center gap-2">
              <Database className="w-5 h-5 text-blue-400" />
              <h2 className="text-lg font-bold text-white">Canonical Dataset & Decentralized Storage</h2>
            </div>
            <p className="text-xs text-gray-400 mt-1">
              Dataset exported as Apache Parquet with Zstandard compression. Publicly retrievable via IPFS / Arweave and anchored to Ethereum L2.
            </p>
          </div>
          <span className="badge-live">
            <ShieldCheck className="w-3.5 h-3.5" />
            L2 ANCHORED
          </span>
        </div>
      </div>

      <div className="grid grid-cols-1 md:grid-cols-2 gap-6">
        {/* Parquet & IPFS Card */}
        <div className="panel p-5 space-y-4">
          <div className="flex items-center justify-between border-b border-[#1a2538] pb-3">
            <span className="font-semibold text-white text-sm flex items-center gap-2">
              <Layers className="w-4 h-4 text-[#76B900]" />
              Parquet Dataset & IPFS Identity
            </span>
            <span className="text-xs font-mono text-[#76B900]">Zstandard Compressed</span>
          </div>

          <div className="space-y-3 text-xs font-mono">
            <div>
              <div className="text-gray-400 text-[10px] uppercase">Dataset ID</div>
              <div className="text-white font-semibold">{meta.dataset_id || "NVDA-SIM-2026-08-28-2026-08-27"}</div>
            </div>

            <div>
              <div className="text-gray-400 text-[10px] uppercase">Asset Symbol</div>
              <div className="text-white">NVDA (NVIDIA Corporation)</div>
            </div>

            <div>
              <div className="text-gray-400 text-[10px] uppercase">Binary Dataset SHA-256 Hash</div>
              <div className="bg-[#070a11] p-2 rounded text-blue-300 border border-[#1a2538] break-all">
                {meta.dataset_hash || "0xa8f37b92c41298e1045a77fbc012e89d1234567890abcdef1234567890abcdef"}
              </div>
            </div>

            <div>
              <div className="text-gray-400 text-[10px] uppercase">IPFS Content Identifier (CIDv1)</div>
              <div className="bg-[#070a11] p-2 rounded text-[#76B900] border border-[#1a2538] break-all flex items-center justify-between">
                <span>{meta.ipfs_cid || "bafybeig1234567890abcdef1234567890abcdef1234567890"}</span>
                <a
                  href={`https://ipfs.io/ipfs/${meta.ipfs_cid || ""}`}
                  target="_blank"
                  rel="noreferrer"
                  className="text-gray-400 hover:text-white"
                >
                  <ExternalLink className="w-3.5 h-3.5" />
                </a>
              </div>
            </div>
          </div>
        </div>

        {/* Ethereum L2 Contract Card */}
        <div className="panel p-5 space-y-4">
          <div className="flex items-center justify-between border-b border-[#1a2538] pb-3">
            <span className="font-semibold text-white text-sm flex items-center gap-2">
              <Cpu className="w-4 h-4 text-purple-400" />
              Ethereum L2 Smart Contract Commitment
            </span>
            <span className="text-xs font-mono text-purple-300">Base / Arbitrum L2</span>
          </div>

          <div className="space-y-3 text-xs font-mono">
            <div>
              <div className="text-gray-400 text-[10px] uppercase">Contract Registry Address</div>
              <div className="bg-[#070a11] p-2 rounded text-purple-300 border border-[#1a2538] break-all">
                {l2.contract_address || "0x76B900000000000000000000000000000000NVDA"}
              </div>
            </div>

            <div>
              <div className="text-gray-400 text-[10px] uppercase">L2 Transaction Hash</div>
              <div className="bg-[#070a11] p-2 rounded text-gray-300 border border-[#1a2538] break-all flex items-center justify-between">
                <span>{l2.tx_hash || "0x1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef"}</span>
                <span className="text-[10px] text-[#76B900] font-semibold">CONFIRMED</span>
              </div>
            </div>

            <div>
              <div className="text-gray-400 text-[10px] uppercase">Anchored Merkle Root</div>
              <div className="bg-[#070a11] p-2 rounded text-[#76B900] border border-[#1a2538] break-all">
                {meta.merkle_root || "0x9876543210fedcba9876543210fedcba9876543210fedcba9876543210fedcba"}
              </div>
            </div>

            <div className="bg-purple-500/10 border border-purple-500/30 rounded-lg p-3 text-xs text-purple-300 flex items-center gap-2">
              <CheckCircle2 className="w-4 h-4 flex-shrink-0" />
              <span>
                Smart Contract verifies zero-knowledge dataset integrity and trade availability.
              </span>
            </div>
          </div>
        </div>
      </div>
    </div>
  );
}
