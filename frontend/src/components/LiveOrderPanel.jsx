import React, { useState, useEffect } from "react";
import { ArrowUpCircle, ArrowDownCircle, CheckCircle2, Copy, Check, ExternalLink, RefreshCw, AlertCircle, ShieldCheck } from "lucide-react";

export default function LiveOrderPanel({
  currentPrice,
  portfolio,
  onOrderPlaced,
  onNavigateToVerifier,
  backendUrl = ""
}) {
  const [side, setSide] = useState("BUY");
  const [orderType, setOrderType] = useState("MARKET");
  const [quantity, setQuantity] = useState(25);
  const [limitPrice, setLimitPrice] = useState("");
  const [isSubmitting, setIsSubmitting] = useState(false);
  const [errorMsg, setErrorMsg] = useState("");
  const [receipt, setReceipt] = useState(null);
  const [copied, setCopied] = useState(false);

  // Sync limit price with current price if empty
  useEffect(() => {
    if (currentPrice && !limitPrice) {
      setLimitPrice(currentPrice.toFixed(2));
    }
  }, [currentPrice]);

  const activePrice = orderType === "LIMIT" && parseFloat(limitPrice) > 0
    ? parseFloat(limitPrice)
    : (currentPrice || 180.00);

  const totalValue = (quantity || 0) * activePrice;
  const cash = portfolio?.cash ?? 100000.00;
  const nvdaPosition = (portfolio?.positions || []).find((p) => p.ticker.toUpperCase() === "NVDA");
  const holdingShares = nvdaPosition?.shares ?? 0;

  const handleCopyTradeId = (tradeId) => {
    navigator.clipboard.writeText(tradeId);
    setCopied(true);
    setTimeout(() => setCopied(false), 2000);
  };

  const handleSubmit = async (e) => {
    e.preventDefault();
    setErrorMsg("");
    setIsSubmitting(true);

    try {
      const res = await fetch(`${backendUrl}/api/orders`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          ticker: "NVDA",
          side: side,
          order_type: orderType,
          quantity: parseInt(quantity, 10),
          price: activePrice
        })
      });

      const data = await res.json();
      if (!res.ok || data.status === "REJECTED") {
        setErrorMsg(data.message || data.detail || "Order could not be matched.");
      } else {
        // Order filled! Display receipt with unique Trade ID
        setReceipt({
          order_id: data.order_id,
          trade_id: data.trade_id,
          side: data.side,
          quantity: data.quantity,
          filled_price: data.filled_price,
          total: data.filled_price * data.quantity,
          timestamp: data.timestamp,
          leaf_hash: data.leaf_hash
        });

        if (onOrderPlaced) {
          onOrderPlaced(data);
        }
      }
    } catch (err) {
      setErrorMsg("Failed to connect to trading engine: " + err.message);
    } finally {
      setIsSubmitting(false);
    }
  };

  return (
    <div className="panel overflow-hidden flex flex-col h-full bg-[#0d121d] border border-[#1a2538]">
      {/* Header */}
      <div className="panel-header bg-[#090e17] flex items-center justify-between">
        <div className="flex items-center gap-2">
          <span className="w-2 h-2 rounded-full bg-[#76B900] animate-ping" />
          <span className="text-white font-semibold text-xs tracking-wider">LIVE TRADE EXECUTION</span>
        </div>
        <div className="font-mono text-xs text-[#76B900] bg-[#76B900]/10 px-2.5 py-0.5 rounded border border-[#76B900]/30 font-bold">
          NVDA ${currentPrice ? currentPrice.toFixed(2) : "180.00"}
        </div>
      </div>

      <div className="p-4 flex-1 flex flex-col justify-between space-y-4">
        {/* If Order Just Executed: Show Unique Trade ID Receipt */}
        {receipt ? (
          <div className="space-y-4 bg-[#070a11] p-4 rounded-lg border border-[#76B900]/40 animate-fade-in">
            <div className="flex items-center justify-between border-b border-[#1a2538] pb-2.5">
              <div className="flex items-center gap-2 text-[#76B900] font-bold text-xs">
                <CheckCircle2 className="w-4 h-4" />
                <span>TRADE MATCHED & EXECUTED</span>
              </div>
              <span className="text-[10px] font-mono text-gray-400">{receipt.timestamp}</span>
            </div>

            {/* Prominent Unique Trade ID Banner */}
            <div className="bg-[#090e17] p-3 rounded-lg border border-[#1a2538] space-y-1">
              <div className="text-[10px] uppercase font-mono tracking-wider text-gray-400 flex items-center justify-between">
                <span>Unique Trade ID</span>
                <span className="text-emerald-400 font-bold text-[10px]">VERIFIABLE</span>
              </div>
              <div className="flex items-center justify-between gap-2">
                <span className="font-mono text-sm font-bold text-[#76B900] tracking-wide select-all">
                  {receipt.trade_id}
                </span>
                <button
                  type="button"
                  onClick={() => handleCopyTradeId(receipt.trade_id)}
                  className="btn btn-outline text-xs px-2.5 py-1 text-gray-300 hover:text-white"
                  title="Copy Trade ID"
                >
                  {copied ? (
                    <>
                      <Check className="w-3.5 h-3.5 text-[#76B900]" />
                      <span className="text-[11px] text-[#76B900]">Copied</span>
                    </>
                  ) : (
                    <>
                      <Copy className="w-3.5 h-3.5" />
                      <span className="text-[11px]">Copy ID</span>
                    </>
                  )}
                </button>
              </div>
            </div>

            {/* Execution Details Table */}
            <div className="grid grid-cols-2 gap-2 text-xs font-mono bg-[#090e17] p-3 rounded-lg border border-[#1a2538]">
              <div>
                <span className="text-gray-500 block text-[10px]">SIDE / ACTION</span>
                <span className={`font-bold ${receipt.side === "BUY" ? "text-[#76B900]" : "text-red-400"}`}>
                  {receipt.side} {receipt.quantity} NVDA
                </span>
              </div>
              <div>
                <span className="text-gray-500 block text-[10px]">FILLED PRICE</span>
                <span className="text-gray-200 font-bold">${receipt.filled_price?.toFixed(2)}</span>
              </div>
              <div>
                <span className="text-gray-500 block text-[10px]">TOTAL VALUE</span>
                <span className="text-white font-bold">${receipt.total?.toLocaleString("en-US", { minimumFractionDigits: 2, maximumFractionDigits: 2 })}</span>
              </div>
              <div>
                <span className="text-gray-500 block text-[10px]">LEAF HASH (SHA-256)</span>
                <span className="text-gray-400 text-[10px] font-mono truncate block" title={receipt.leaf_hash}>
                  {receipt.leaf_hash ? `${receipt.leaf_hash.slice(0, 10)}...` : "--"}
                </span>
              </div>
            </div>

            {/* Action Buttons: Verify Now & Place Another */}
            <div className="space-y-2 pt-1">
              <button
                type="button"
                onClick={() => onNavigateToVerifier && onNavigateToVerifier(receipt.trade_id)}
                className="btn btn-nvda w-full text-xs py-2.5 flex items-center justify-center gap-2"
              >
                <ShieldCheck className="w-4 h-4" />
                <span>Verify Trade ID #{receipt.trade_id} Now</span>
              </button>

              <button
                type="button"
                onClick={() => {
                  setReceipt(null);
                  setErrorMsg("");
                }}
                className="btn btn-outline w-full text-xs py-2"
              >
                Place Another Order
              </button>
            </div>
          </div>
        ) : (
          <form onSubmit={handleSubmit} className="space-y-4">
            {/* BUY / SELL Switcher */}
            <div className="grid grid-cols-2 gap-2">
              <button
                type="button"
                onClick={() => setSide("BUY")}
                className={`py-2 px-3 rounded-md font-mono text-xs font-bold flex items-center justify-center gap-1.5 transition-all ${
                  side === "BUY"
                    ? "bg-[#76B900] text-black shadow-lg shadow-[#76B900]/20"
                    : "bg-[#090e17] text-gray-400 border border-[#1a2538] hover:text-white"
                }`}
              >
                <ArrowUpCircle className="w-4 h-4" />
                <span>BUY NVDA</span>
              </button>

              <button
                type="button"
                onClick={() => setSide("SELL")}
                className={`py-2 px-3 rounded-md font-mono text-xs font-bold flex items-center justify-center gap-1.5 transition-all ${
                  side === "SELL"
                    ? "bg-red-500 text-white shadow-lg shadow-red-500/20"
                    : "bg-[#090e17] text-gray-400 border border-[#1a2538] hover:text-white"
                }`}
              >
                <ArrowDownCircle className="w-4 h-4" />
                <span>SELL NVDA</span>
              </button>
            </div>

            {/* Order Type Tabs (MARKET / LIMIT) */}
            <div>
              <div className="flex justify-between items-center mb-1 text-[10px] font-mono text-gray-400 uppercase">
                <span>Execution Type</span>
                <span>MATCH: {orderType === "MARKET" ? "IMMEDIATE (SIM TICK)" : "LIMIT THRESHOLD"}</span>
              </div>
              <div className="grid grid-cols-2 gap-2 font-mono text-xs">
                <button
                  type="button"
                  onClick={() => setOrderType("MARKET")}
                  className={`py-1.5 rounded border transition-all ${
                    orderType === "MARKET"
                      ? "bg-[#131b2b] border-[#76B900] text-[#76B900] font-bold"
                      : "bg-[#070a11] border-[#1a2538] text-gray-400 hover:text-white"
                  }`}
                >
                  MARKET
                </button>
                <button
                  type="button"
                  onClick={() => setOrderType("LIMIT")}
                  className={`py-1.5 rounded border transition-all ${
                    orderType === "LIMIT"
                      ? "bg-[#131b2b] border-[#76B900] text-[#76B900] font-bold"
                      : "bg-[#070a11] border-[#1a2538] text-gray-400 hover:text-white"
                  }`}
                >
                  LIMIT
                </button>
              </div>
            </div>

            {/* Quantity Input with Quick Select */}
            <div>
              <div className="flex justify-between items-center mb-1 text-[10px] font-mono text-gray-400 uppercase">
                <span>Quantity (Shares)</span>
                <span className="text-gray-300">Holding: {holdingShares} shares</span>
              </div>
              <div className="flex gap-2">
                <input
                  type="number"
                  min="1"
                  max="100000"
                  value={quantity}
                  onChange={(e) => setQuantity(Math.max(1, parseInt(e.target.value) || 1))}
                  className="input-field text-sm font-bold"
                  required
                />
              </div>
              {/* Quick Share Buttons */}
              <div className="grid grid-cols-4 gap-1.5 mt-2 font-mono text-[11px]">
                {[10, 50, 100, 500].map((q) => (
                  <button
                    key={q}
                    type="button"
                    onClick={() => setQuantity(q)}
                    className="py-1 bg-[#090e17] border border-[#1a2538] rounded text-gray-400 hover:text-[#76B900] hover:border-[#76B900]/40 transition-colors"
                  >
                    +{q}
                  </button>
                ))}
              </div>
            </div>

            {/* Limit Price Input if LIMIT selected */}
            {orderType === "LIMIT" && (
              <div>
                <div className="flex justify-between items-center mb-1 text-[10px] font-mono text-gray-400 uppercase">
                  <span>Limit Price ($)</span>
                  <span className="text-gray-400">Current: ${currentPrice?.toFixed(2)}</span>
                </div>
                <input
                  type="number"
                  step="0.01"
                  value={limitPrice}
                  onChange={(e) => setLimitPrice(e.target.value)}
                  className="input-field text-sm"
                  required
                />
              </div>
            )}

            {/* Order Summary & Buying Power */}
            <div className="bg-[#070a11] p-3 rounded-lg border border-[#1a2538] font-mono text-xs space-y-1.5">
              <div className="flex justify-between text-gray-400">
                <span>Exec Price:</span>
                <span className="text-gray-200 font-bold">${activePrice.toFixed(2)}</span>
              </div>
              <div className="flex justify-between text-gray-400">
                <span>Est. Order Value:</span>
                <span className="font-bold text-white">${totalValue.toLocaleString("en-US", { minimumFractionDigits: 2, maximumFractionDigits: 2 })}</span>
              </div>
              <div className="h-px bg-[#1a2538] my-1" />
              <div className="flex justify-between text-[11px] text-gray-400">
                <span>Available Cash:</span>
                <span className="text-emerald-400 font-bold">${cash.toLocaleString("en-US", { minimumFractionDigits: 2, maximumFractionDigits: 2 })}</span>
              </div>
            </div>

            {errorMsg && (
              <div className="panel p-2.5 border-red-500/50 bg-red-500/10 text-red-400 text-xs flex items-center gap-2 font-mono">
                <AlertCircle className="w-4 h-4 flex-shrink-0" />
                <span>{errorMsg}</span>
              </div>
            )}

            {/* Submit Button */}
            <button
              type="submit"
              disabled={isSubmitting}
              className={`w-full py-2.5 rounded-lg font-mono font-bold text-xs uppercase tracking-wider transition-all shadow-md ${
                side === "BUY"
                  ? "btn-nvda"
                  : "bg-red-500 hover:bg-red-600 text-white shadow-red-500/30"
              } disabled:opacity-50`}
            >
              {isSubmitting ? (
                <span className="flex items-center justify-center gap-2">
                  <RefreshCw className="w-3.5 h-3.5 animate-spin" />
                  MATCHING ORDER...
                </span>
              ) : (
                `CONFIRM ${side} ${quantity} NVDA @ $${activePrice.toFixed(2)}`
              )}
            </button>
          </form>
        )}
      </div>
    </div>
  );
}
