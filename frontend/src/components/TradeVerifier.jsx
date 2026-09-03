import React, { useState, useEffect } from "react";
import {
  Search,
  ShieldCheck,
  CheckCircle2,
  AlertCircle,
  FileCode,
  Hash,
  GitBranch,
  Copy,
  Check,
  UserCheck,
  Cpu,
  Layers,
  ArrowUpRight,
  ArrowDownRight
} from "lucide-react";

export default function TradeVerifier({ backendUrl, initialTradeId = "", recentTradeIds = [] }) {
  const [tradeIdInput, setTradeIdInput] = useState(initialTradeId || "000000083291");
  const [loading, setLoading] = useState(false);
  const [verifyResult, setVerifyResult] = useState(null);
  const [error, setError] = useState(null);
  const [copiedField, setCopiedField] = useState(null);

  // Auto-verify if initialTradeId is passed or changed
  useEffect(() => {
    if (initialTradeId) {
      setTradeIdInput(initialTradeId);
      performLookup(initialTradeId);
    }
  }, [initialTradeId]);

  const handleCopy = (text, fieldName) => {
    navigator.clipboard.writeText(text);
    setCopiedField(fieldName);
    setTimeout(() => setCopiedField(null), 2000);
  };

  const performLookup = async (idToSearch) => {
    const cleanId = (idToSearch || tradeIdInput || "").trim();
    if (!cleanId) return;

    setLoading(true);
    setError(null);
    setVerifyResult(null);

    try {
      // 1. Verify existence & retrieve trade details from backend
      const res = await fetch(`${backendUrl}/api/trades/${cleanId}`);
      if (!res.ok) {
        if (res.status === 404) {
          throw new Error(`Trade ID #${cleanId} does not exist in the ledger or simulation dataset.`);
        }
        throw new Error(`Failed to retrieve trade #${cleanId} (HTTP ${res.status}).`);
      }
      const data = await res.json();

      // 2. Perform independent cryptographic verification
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

  const handleSubmit = (e) => {
    e?.preventDefault();
    performLookup(tradeIdInput);
  };

  const defaultSampleIds = ["000000083291", "000000000001", "000000150000"];
  const allSuggestedIds = Array.from(new Set([...recentTradeIds, ...defaultSampleIds])).slice(0, 6);

  return (
    <div className="space-y-6">
      {/* Search Bar Panel */}
      <div className="panel p-6 bg-gradient-to-r from-[#0d121d] via-[#0b101b] to-[#070a11] border border-[#1a2538]">
        <div className="max-w-3xl">
          <div className="flex items-center gap-2 mb-1">
            <ShieldCheck className="w-5 h-5 text-[#76B900]" />
            <h2 className="text-lg font-bold text-white tracking-wide">
              Trade ID Verification & Data Retrieval
            </h2>
          </div>
          <p className="text-xs text-gray-400 mb-4">
            Enter any Trade ID (user-executed order <code className="text-[#76B900] bg-[#76B900]/10 px-1 py-0.5 rounded">TRD-NVDA-...</code> or simulation trade <code className="text-gray-300 bg-gray-800 px-1 py-0.5 rounded">000000083291</code>) to verify existence, retrieve complete execution parameters, and audit the SHA-256 Merkle proof path.
          </p>

          <form onSubmit={handleSubmit} className="flex flex-col sm:flex-row items-stretch sm:items-center gap-3">
            <div className="relative flex-1">
              <Search className="w-4 h-4 text-gray-400 absolute left-3 top-3.5" />
              <input
                type="text"
                value={tradeIdInput}
                onChange={(e) => setTradeIdInput(e.target.value)}
                placeholder="Enter Trade ID (e.g. TRD-NVDA-8F29A10B or 000000083291)"
                className="input-field pl-9 font-mono"
              />
            </div>
            <button type="submit" disabled={loading} className="btn btn-nvda flex-shrink-0">
              {loading ? "Verifying & Retrieving..." : "Verify & Retrieve Trade"}
            </button>
          </form>

          {/* Suggested Trade IDs Quick Chips */}
          <div className="mt-3 flex flex-wrap items-center gap-2 text-xs font-mono">
            <span className="text-gray-500 text-[11px]">Quick Select:</span>
            {allSuggestedIds.map((id) => (
              <button
                key={id}
                type="button"
                onClick={() => {
                  setTradeIdInput(id);
                  performLookup(id);
                }}
                className={`px-2 py-0.5 rounded text-[11px] border transition-colors flex items-center gap-1 ${
                  id.startsWith("TRD-")
                    ? "bg-[#76B900]/10 border-[#76B900]/40 text-[#76B900] hover:bg-[#76B900]/20"
                    : "bg-[#090e17] border-[#1a2538] text-gray-400 hover:text-white hover:border-gray-500"
                }`}
              >
                {id.startsWith("TRD-") && <UserCheck className="w-3 h-3 text-[#76B900]" />}
                <span>#{id}</span>
              </button>
            ))}
          </div>
        </div>
      </div>

      {/* Error Banner: Trade Not Found */}
      {error && (
        <div className="panel p-5 border-red-500/50 bg-red-500/10 text-red-300 text-sm flex items-start gap-3 animate-fade-in">
          <AlertCircle className="w-5 h-5 flex-shrink-0 text-red-400 mt-0.5" />
          <div className="space-y-1">
            <div className="font-bold text-red-400">Trade Verification Check Failed</div>
            <div>{error}</div>
            <div className="text-xs text-red-400/80 font-mono">
              Status: 404 NOT FOUND • Verify that the Trade ID matches an executed order or replay index.
            </div>
          </div>
        </div>
      )}

      {/* Verified Trade Details Card */}
      {verifyResult && (
        <div className="grid grid-cols-1 lg:grid-cols-12 gap-6 animate-fade-in">
          {/* Left Column: Retrieved Trade Details (7 cols) */}
          <div className="lg:col-span-7 panel p-5 space-y-4">
            <div className="flex items-center justify-between border-b border-[#1a2538] pb-3">
              <div className="flex items-center gap-2">
                <FileCode className="w-4 h-4 text-[#76B900]" />
                <span className="font-semibold text-white text-sm">Retrieved Trade Record</span>
              </div>
              <div className="flex items-center gap-2">
                <span className="badge-live text-[10px]">
                  <CheckCircle2 className="w-3 h-3" />
                  VERIFIED IN LEDGER
                </span>
                <span className="text-xs font-mono text-gray-400">#{verifyResult.trade?.trade_id}</span>
              </div>
            </div>

            {/* Structured Trade Parameter Cards */}
            <div className="grid grid-cols-2 sm:grid-cols-4 gap-3 font-mono text-xs">
              <div className="bg-[#070a11] p-2.5 rounded border border-[#1a2538]">
                <span className="text-gray-500 block text-[10px] uppercase">Symbol</span>
                <span className="font-bold text-white text-sm">{verifyResult.trade?.symbol}</span>
              </div>

              <div className="bg-[#070a11] p-2.5 rounded border border-[#1a2538]">
                <span className="text-gray-500 block text-[10px] uppercase">Side</span>
                <span className={`inline-flex items-center gap-1 font-bold ${verifyResult.trade?.side === "BUY" ? "text-[#76B900]" : "text-red-400"}`}>
                  {verifyResult.trade?.side === "BUY" ? <ArrowUpRight className="w-3.5 h-3.5" /> : <ArrowDownRight className="w-3.5 h-3.5" />}
                  {verifyResult.trade?.side}
                </span>
              </div>

              <div className="bg-[#070a11] p-2.5 rounded border border-[#1a2538]">
                <span className="text-gray-500 block text-[10px] uppercase">Execution Price</span>
                <span className="font-bold text-[#76B900] text-sm">${parseFloat(verifyResult.trade?.price || 0).toFixed(2)}</span>
              </div>

              <div className="bg-[#070a11] p-2.5 rounded border border-[#1a2538]">
                <span className="text-gray-500 block text-[10px] uppercase">Quantity</span>
                <span className="font-bold text-gray-200 text-sm">{verifyResult.trade?.quantity} shares</span>
              </div>
            </div>

            {/* Total Value & Timestamps */}
            <div className="bg-[#070a11] p-3 rounded-lg border border-[#1a2538] font-mono text-xs space-y-1.5">
              <div className="flex justify-between text-gray-400">
                <span>Total Notional Value:</span>
                <span className="font-bold text-white">
                  ${((verifyResult.trade?.price || 0) * (verifyResult.trade?.quantity || 0)).toLocaleString("en-US", { minimumFractionDigits: 2, maximumFractionDigits: 2 })}
                </span>
              </div>
              <div className="flex justify-between text-gray-400">
                <span>Simulation Timestamp (IST):</span>
                <span className="text-gray-300">{verifyResult.trade?.simulation_timestamp}</span>
              </div>
              <div className="flex justify-between text-gray-400">
                <span>Source Observation (ET):</span>
                <span className="text-gray-300">{verifyResult.trade?.source_timestamp}</span>
              </div>
              <div className="flex justify-between text-gray-400">
                <span>Trade Type Origin:</span>
                <span className="text-blue-400 font-bold">{verifyResult.trade_type || "SIMULATION_TRADE"}</span>
              </div>
            </div>

            {/* Canonical CBOR Serialization */}
            <div>
              <div className="flex items-center justify-between text-[11px] uppercase tracking-wider text-gray-400 mb-1">
                <span>Canonical CBOR Serialization (Hex)</span>
                <button
                  type="button"
                  onClick={() => handleCopy(verifyResult.cbor_hex, "cbor")}
                  className="text-[#76B900] hover:underline flex items-center gap-1 text-[10px]"
                >
                  {copiedField === "cbor" ? <Check className="w-3 h-3" /> : <Copy className="w-3 h-3" />}
                  {copiedField === "cbor" ? "Copied" : "Copy Hex"}
                </button>
              </div>
              <div className="bg-[#070a11] p-2.5 rounded text-[11px] font-mono text-blue-300 border border-[#1a2538] break-all max-h-24 overflow-y-auto">
                {verifyResult.cbor_hex}
              </div>
            </div>
          </div>

          {/* Right Column: Cryptographic Proof & L2 Verification (5 cols) */}
          <div className="lg:col-span-5 panel p-5 space-y-4 flex flex-col justify-between">
            <div className="space-y-4">
              <div className="flex items-center justify-between border-b border-[#1a2538] pb-3">
                <div className="flex items-center gap-2">
                  <Hash className="w-4 h-4 text-purple-400" />
                  <span className="font-semibold text-white text-sm">Cryptographic Proof & Integrity</span>
                </div>
                {verifyResult.isVerified ? (
                  <span className="badge-live">
                    <CheckCircle2 className="w-3.5 h-3.5" />
                    VERIFIED
                  </span>
                ) : (
                  <span className="bg-red-500/20 text-red-400 border border-red-500 px-2 py-0.5 rounded text-xs font-bold">
                    FAILED
                  </span>
                )}
              </div>

              {/* SHA-256 Leaf Hash */}
              <div>
                <div className="flex items-center justify-between text-[11px] uppercase tracking-wider text-gray-400 mb-1">
                  <span>SHA-256 Leaf Hash</span>
                  <button
                    type="button"
                    onClick={() => handleCopy(verifyResult.leaf_hash, "leaf")}
                    className="text-[#76B900] hover:underline flex items-center gap-1 text-[10px]"
                  >
                    {copiedField === "leaf" ? <Check className="w-3 h-3" /> : <Copy className="w-3 h-3" />}
                    {copiedField === "leaf" ? "Copied" : "Copy"}
                  </button>
                </div>
                <div className="bg-[#070a11] p-2 rounded text-[11px] font-mono text-[#76B900] border border-[#1a2538] break-all">
                  {verifyResult.leaf_hash}
                </div>
              </div>

              {/* Merkle Root */}
              <div>
                <div className="text-[11px] uppercase tracking-wider text-gray-400 mb-1">
                  Committed Merkle Root
                </div>
                <div className="bg-[#070a11] p-2 rounded text-[11px] font-mono text-purple-300 border border-[#1a2538] break-all">
                  {verifyResult.merkle_root}
                </div>
              </div>

              {/* Merkle Proof Audit Steps */}
              <div>
                <div className="text-[11px] uppercase tracking-wider text-gray-400 mb-1 flex items-center gap-1">
                  <GitBranch className="w-3 h-3 text-amber-400" />
                  Merkle Proof Path ({verifyResult.proof?.length || 0} Audit Steps)
                </div>
                <div className="bg-[#070a11] p-2.5 rounded text-[10px] font-mono text-gray-300 border border-[#1a2538] max-h-36 overflow-y-auto space-y-1">
                  {verifyResult.proof?.length === 0 ? (
                    <div className="text-gray-500 py-1">
                      {verifyResult.trade_type === "USER_TRADE"
                        ? "User order recorded directly in verified operational ledger."
                        : "Proof path computed on demand via Two-Tier tree."}
                    </div>
                  ) : (
                    verifyResult.proof?.map((p, idx) => (
                      <div key={idx} className="flex items-center justify-between py-0.5 border-b border-[#131b2b] last:border-0">
                        <span className="text-gray-500">Step {idx + 1} ({p.position})</span>
                        <span className="text-amber-300 font-mono">{p.hash.slice(0, 16)}...</span>
                      </div>
                    ))
                  )}
                </div>
              </div>
            </div>

            {/* Bottom Status Banner */}
            <div className="bg-[#76B900]/10 border border-[#76B900]/40 rounded-lg p-3 text-xs text-[#76B900] flex items-center gap-2.5">
              <CheckCircle2 className="w-5 h-5 flex-shrink-0" />
              <div>
                <div className="font-bold">Cryptographic Identity Verified</div>
                <div className="text-[11px] text-gray-300">
                  Trade #{verifyResult.trade?.trade_id} is authentic and cryptographically valid.
                </div>
              </div>
            </div>
          </div>
        </div>
      )}
    </div>
  );
}
