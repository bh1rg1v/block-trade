import React from "react";
import { Activity, ShieldCheck, Database, RefreshCw, Play } from "lucide-react";

export default function Header({
  activeTab,
  setActiveTab,
  simStatus,
  istClock,
  sourceDate,
  onSyncData,
  onStartSim,
  loadingSync,
  loadingSim
}) {
  return (
    <header className="border-b border-[#1a2538] bg-[#090d16] px-6 py-4">
      <div className="flex flex-col md:flex-row md:items-center justify-between gap-4">
        {/* Title & Platform Branding */}
        <div className="flex items-center gap-3">
          <div className="w-10 h-10 rounded-lg bg-[#76B900]/10 border border-[#76B900] flex items-center justify-center text-[#76B900]">
            <Activity className="w-6 h-6 animate-pulse" />
          </div>
          <div>
            <div className="flex items-center gap-2">
              <h1 className="text-xl font-bold text-white tracking-wide">
                NVDA <span className="text-[#76B900]">Trade Engine</span>
              </h1>
              <span className="badge-live">
                <span className="w-2 h-2 rounded-full bg-[#76B900] animate-ping" />
                100K TRADES / MIN
              </span>
            </div>
            <p className="text-xs text-gray-400">
              Verifiable High-Throughput Market Simulation Platform • Ethereum L2 Merkle Commitments
            </p>
          </div>
        </div>

        {/* Live IST Clock & Source Date Info */}
        <div className="flex items-center gap-4 bg-[#0d121d] px-4 py-2 rounded-lg border border-[#1a2538]">
          <div>
            <div className="text-[10px] uppercase tracking-wider text-gray-400">Source Session (ET)</div>
            <div className="text-sm font-semibold text-gray-200 font-mono">{sourceDate || "2026-08-27"}</div>
          </div>
          <div className="h-6 w-px bg-[#1a2538]" />
          <div>
            <div className="text-[10px] uppercase tracking-wider text-gray-400">Simulation Clock (IST)</div>
            <div className="text-sm font-semibold text-[#76B900] font-mono">{istClock}</div>
          </div>
          <div className="h-6 w-px bg-[#1a2538]" />
          <div>
            <div className="text-[10px] uppercase tracking-wider text-gray-400">Target Throughput</div>
            <div className="text-sm font-semibold text-blue-400 font-mono">1,667 Trades/Sec</div>
          </div>
        </div>

        {/* Action Controls */}
        <div className="flex items-center gap-2">
          <button
            onClick={onSyncData}
            disabled={loadingSync}
            className="btn btn-outline text-xs"
            title="Fetch latest NVDA 1m data via yfinance and update NVDA.csv"
          >
            <RefreshCw className={`w-3.5 h-3.5 ${loadingSync ? "animate-spin" : ""}`} />
            Sync NVDA.csv
          </button>

          <button
            onClick={onStartSim}
            disabled={loadingSim}
            className="btn btn-nvda text-xs"
            title="Run 100k trades/min simulation for completed NVDA session"
          >
            <Play className="w-3.5 h-3.5 fill-current" />
            {loadingSim ? "Simulating..." : "Run Simulation"}
          </button>
        </div>
      </div>

      {/* Navigation Tabs */}
      <div className="flex items-center gap-2 mt-4 pt-3 border-t border-[#1a2538]/50">
        <button
          onClick={() => setActiveTab("live")}
          className={`btn text-xs ${activeTab === "live" ? "btn-nvda" : "btn-outline"}`}
        >
          <Activity className="w-3.5 h-3.5" />
          Live Trade Feed
        </button>

        <button
          onClick={() => setActiveTab("verify_trade")}
          className={`btn text-xs ${activeTab === "verify_trade" ? "btn-nvda" : "btn-outline"}`}
        >
          <ShieldCheck className="w-3.5 h-3.5" />
          Trade Cryptographic Verifier
        </button>

        <button
          onClick={() => setActiveTab("dataset_l2")}
          className={`btn text-xs ${activeTab === "dataset_l2" ? "btn-nvda" : "btn-outline"}`}
        >
          <Database className="w-3.5 h-3.5" />
          IPFS & Ethereum L2 Commitment
        </button>
      </div>
    </header>
  );
}
