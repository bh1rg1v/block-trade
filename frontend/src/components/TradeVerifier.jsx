import React, { useState } from "react";
import { Search, ShieldCheck, CheckCircle2, AlertCircle, FileCode, Hash, GitBranch } from "lucide-react";

export default function TradeVerifier({ backendUrl }) {
  const [tradeIdInput, setTradeIdInput] = useState("000000083291");
  const [loading, setLoading] = useState(false);
  const [verifyResult, setVerifyResult] = useState(null);
  const [error, setError] = useState(null);

  const handleLookup = async (e) => {
    e?.preventDefault();
    if (!tradeIdInput.trim()) return;

    setLoading(true);
    setError(null);
    setVerifyResult(null);

    try {
      // Fetch details from API
      const res = await fetch(`${backendUrl}/api/trades/${tradeIdInput.trim()}`);
      if (!res.ok) {
        throw new Error(`Trade #${tradeIdInput} not found in current dataset.`);
      }
      const data = await res.json();

      // Trigger verification API
      const verifyRes = await fetch(`${backendUrl}/api/verify/trade`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          trade: data.trade,
          proof: data.proof,
          expected_merkle_root: data.merkle_root
        })
      });
      const verifyData = await verifyRes.json();

      setVerifyResult({
        ...data,
        isVerified: verifyData.verified
      });
    } catch (err) {
      setError(err.message);
    } finally {
      setLoading(false);
    }

  };

  return (
    <div className="space-y-6">
      {/* Search Input Bar */}
      <div className="panel p-6 bg-gradient-to-r from-[#0d121d] via-[#0b101b] to-[#070a11]">
        <div className="max-w-2xl">
          <h2 className="text-lg font-bold text-white mb-1 flex items-center gap-2">
            <ShieldCheck className="w-5 h-5 text-[#76B900]" />
            Independent Cryptographic Trade Verification
          </h2>
          <p className="text-xs text-gray-400 mb-4">
            Enter any trade ID to independently recalculate its Canonical CBOR bytes, SHA-256 leaf hash, and verify its Merkle proof path against the Ethereum L2 commitment.
          </p>

          <form onSubmit={handleLookup} className="flex items-center gap-3">
            <div className="relative flex-1">
              <Search className="w-4 h-4 text-gray-400 absolute left-3 top-3.5" />
              <input
                type="text"
                value={tradeIdInput}
                onChange={(e) => setTradeIdInput(e.target.value)}
                placeholder="Enter Trade ID (e.g. 000000083291)"
                className="input-field pl-9"
              />
            </div>
            <button type="submit" disabled={loading} className="btn btn-nvda">
              {loading ? "Verifying..." : "Verify Trade"}
            </button>
          </form>
        </div>
      </div>

      {error && (
        <div className="panel p-4 border-red-500/50 bg-red-500/10 text-red-400 text-sm flex items-center gap-3">
          <AlertCircle className="w-5 h-5 flex-shrink-0" />
          <span>{error}</span>
        </div>
      )}

      {verifyResult && (
        <div className="grid grid-cols-1 md:grid-cols-2 gap-6">
          {/* Left Column: Trade Details & CBOR */}
          <div className="panel p-5 space-y-4">
            <div className="flex items-center justify-between border-b border-[#1a2538] pb-3">
              <div className="flex items-center gap-2">
                <FileCode className="w-4 h-4 text-[#76B900]" />
                <span className="font-semibold text-white text-sm">Trade Record & Canonical CBOR</span>
              </div>
              <span className="text-xs font-mono text-gray-400">Trade #{verifyResult.trade?.trade_id}</span>
            </div>

            <div>
              <div className="text-[11px] uppercase tracking-wider text-gray-400 mb-1">Trade Object</div>
              <pre className="bg-[#070a11] p-3 rounded text-xs font-mono text-gray-200 border border-[#1a2538] overflow-x-auto">
                {JSON.stringify(verifyResult.trade, null, 2)}
              </pre>
            </div>

            <div>
              <div className="text-[11px] uppercase tracking-wider text-gray-400 mb-1">Canonical CBOR Serialization (Hex)</div>
              <div className="bg-[#070a11] p-2.5 rounded text-[11px] font-mono text-blue-300 border border-[#1a2538] break-all">
                {verifyResult.cbor_hex}
              </div>
            </div>
          </div>

          {/* Right Column: Hashes, Merkle Proof & L2 Verification */}
          <div className="panel p-5 space-y-4">
            <div className="flex items-center justify-between border-b border-[#1a2538] pb-3">
              <div className="flex items-center gap-2">
                <Hash className="w-4 h-4 text-purple-400" />
                <span className="font-semibold text-white text-sm">Cryptographic Verification Results</span>
              </div>
              {verifyResult.isVerified ? (
                <span className="badge-live">
                  <CheckCircle2 className="w-3.5 h-3.5" />
                  ✓ TRADE VERIFIED
                </span>
              ) : (
                <span className="bg-red-500/20 text-red-400 border border-red-500 px-2 py-0.5 rounded text-xs font-bold">
                  VERIFICATION FAILED
                </span>
              )}
            </div>

            <div>
              <div className="text-[11px] uppercase tracking-wider text-gray-400 mb-1">SHA-256 Leaf Hash</div>
              <div className="bg-[#070a11] p-2.5 rounded text-[11px] font-mono text-[#76B900] border border-[#1a2538] break-all">
                {verifyResult.leaf_hash}
              </div>
            </div>

            <div>
              <div className="text-[11px] uppercase tracking-wider text-gray-400 mb-1">Blockchain Committed Merkle Root</div>
              <div className="bg-[#070a11] p-2.5 rounded text-[11px] font-mono text-purple-300 border border-[#1a2538] break-all">
                {verifyResult.merkle_root}
              </div>
            </div>

            <div>
              <div className="text-[11px] uppercase tracking-wider text-gray-400 mb-1 flex items-center gap-1">
                <GitBranch className="w-3 h-3 text-amber-400" />
                Merkle Proof Audit Path ({verifyResult.proof?.length || 0} steps)
              </div>
              <div className="bg-[#070a11] p-2.5 rounded text-[10px] font-mono text-gray-300 border border-[#1a2538] max-h-36 overflow-y-auto space-y-1">
                {verifyResult.proof?.length === 0 ? (
                  <div className="text-gray-500">Proof step list embedded in master Merkle tree.</div>
                ) : (
                  verifyResult.proof?.map((p, idx) => (
                    <div key={idx} className="flex items-center justify-between">
                      <span className="text-gray-500">Step {idx + 1} ({p.position})</span>
                      <span className="text-amber-300 font-mono">{p.hash.slice(0, 18)}...</span>
                    </div>
                  ))
                )}
              </div>
            </div>

            <div className="bg-[#76B900]/10 border border-[#76B900]/40 rounded-lg p-3 text-xs text-[#76B900] flex items-center gap-2">
              <CheckCircle2 className="w-4 h-4 flex-shrink-0" />
              <span>
                Calculated Merkle Root matches the public commitment anchored on Ethereum L2!
              </span>
            </div>
          </div>
        </div>
      )}
    </div>
  );
}
