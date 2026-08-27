import React from "react";
import { ArrowUpRight, ArrowDownRight, Radio } from "lucide-react";

export default function LiveTradeFeed({ trades, isConnected }) {
  return (
    <div className="panel overflow-hidden">
      {/* Table Title Bar */}
      <div className="panel-header bg-[#090e17]">
        <div className="flex items-center gap-2">
          <Radio className={`w-4 h-4 ${isConnected ? "text-[#76B900] animate-pulse" : "text-amber-400"}`} />
          <span className="text-white font-semibold">Live NVDA Trade Feed Stream</span>
        </div>
        <div className="flex items-center gap-3 text-xs">
          <span className="text-gray-400">Connection:</span>
          <span className={isConnected ? "text-[#76B900] font-semibold" : "text-amber-400 font-semibold"}>
            {isConnected ? "● WebSocket Connected" : "○ Polling Active"}
          </span>
          <span className="text-gray-500">|</span>
          <span className="text-gray-400">Rate: 1,667 trades/sec</span>
        </div>
      </div>

      {/* Table Headers */}
      <div className="grid grid-cols-7 bg-[#0b101b] border-b border-[#1a2538] text-[11px] font-semibold text-gray-400 uppercase tracking-wider px-4 py-2.5">
        <div>Trade ID</div>
        <div>Simulation Time (IST)</div>
        <div>Source Time (ET)</div>
        <div>Symbol</div>
        <div>Side</div>
        <div className="text-right">Price ($)</div>
        <div className="text-right">Quantity</div>
      </div>

      {/* Virtualized/Scrollable Trades List */}
      <div className="max-h-[520px] overflow-y-auto divide-y divide-[#131b2b] bg-[#070a11]">
        {trades.length === 0 ? (
          <div className="p-12 text-center text-gray-500 font-mono text-sm">
            Waiting for live trade simulation feed... Click "Run Simulation" or "Sync NVDA.csv" to start streaming.
          </div>
        ) : (
          trades.map((t, idx) => {
            const isBuy = t.side === "BUY";
            return (
              <div
                key={`${t.trade_id}-${idx}`}
                className="grid grid-cols-7 px-4 py-2 text-xs font-mono items-center hover:bg-[#0f172a] transition-colors"
              >
                <div className="text-gray-300 font-bold">#{t.trade_id}</div>
                <div className="text-gray-400">{t.simulation_timestamp?.split("T")[1]}</div>
                <div className="text-gray-500">{t.source_timestamp?.split("T")[1]}</div>
                <div className="font-semibold text-white">{t.symbol}</div>
                <div>
                  <span
                    className={`inline-flex items-center gap-1 px-2 py-0.5 rounded text-[10px] font-bold ${
                      isBuy
                        ? "bg-[#76B900]/10 text-[#76B900] border border-[#76B900]/30"
                        : "bg-red-500/10 text-red-400 border border-red-500/30"
                    }`}
                  >
                    {isBuy ? <ArrowUpRight className="w-3 h-3" /> : <ArrowDownRight className="w-3 h-3" />}
                    {t.side}
                  </span>
                </div>
                <div className={`text-right font-bold ${isBuy ? "text-[#76B900]" : "text-red-400"}`}>
                  ${parseFloat(t.price).toFixed(2)}
                </div>
                <div className="text-right text-gray-300">{t.quantity}</div>
              </div>
            );
          })
        )}
      </div>
    </div>
  );
}
