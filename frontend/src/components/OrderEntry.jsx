import React, { useState } from 'react';
import { ArrowUpCircle, ArrowDownCircle, AlertCircle } from 'lucide-react';

export default function OrderEntry({ ticker, currentPrice, onOrderExecuted, cash, holdings }) {
  const [side, setSide] = useState('BUY');
  const [orderType, setOrderType] = useState('MARKET');
  const [quantity, setQuantity] = useState(10);
  const [limitPrice, setLimitPrice] = useState(currentPrice ? currentPrice.toFixed(2) : '100.00');
  const [isSubmitting, setIsSubmitting] = useState(false);
  const [errorMsg, setErrorMsg] = useState('');
  const [successMsg, setSuccessMsg] = useState('');

  const execPrice = orderType === 'LIMIT' ? parseFloat(limitPrice) || currentPrice : currentPrice;
  const totalCost = (quantity || 0) * (execPrice || 0);

  const handleSubmit = async (e) => {
    e.preventDefault();
    setErrorMsg('');
    setSuccessMsg('');
    setIsSubmitting(true);

    try {
      const resp = await fetch('/api/orders', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          ticker: ticker,
          side: side,
          order_type: orderType,
          quantity: parseInt(quantity, 10),
          price: execPrice
        })
      });

      const data = await resp.json();
      if (!resp.ok || data.status === 'REJECTED') {
        setErrorMsg(data.message || data.detail || 'Order execution failed');
      } else {
        setSuccessMsg(`Successfully executed ${side} ${quantity} ${ticker} @ $${data.filled_price.toFixed(2)}`);
        onOrderExecuted();
      }
    } catch (err) {
      setErrorMsg('Failed to connect to backend engine.');
    } finally {
      setIsSubmitting(false);
    }
  };

  return (
    <div className="panel flex flex-col h-full">
      <div className="panel-header border-b border-slate-800">
        <span className="text-slate-200">ORDER EXECUTION</span>
        <span className="font-mono text-xs text-emerald-400">{ticker}</span>
      </div>

      <div className="p-4 flex-1 flex flex-col justify-between">
        {/* BUY / SELL Switch */}
        <div className="grid grid-cols-2 gap-2 mb-4">
          <button
            type="button"
            onClick={() => setSide('BUY')}
            className={`py-2 px-3 rounded font-mono text-xs font-bold flex items-center justify-center space-x-1.5 transition-all ${
              side === 'BUY'
                ? 'bg-emerald-600 text-white shadow-lg shadow-emerald-900/30'
                : 'bg-slate-900 text-slate-400 border border-slate-800 hover:text-slate-200'
            }`}
          >
            <ArrowUpCircle className="w-4 h-4" />
            <span>BUY</span>
          </button>
          <button
            type="button"
            onClick={() => setSide('SELL')}
            className={`py-2 px-3 rounded font-mono text-xs font-bold flex items-center justify-center space-x-1.5 transition-all ${
              side === 'SELL'
                ? 'bg-rose-600 text-white shadow-lg shadow-rose-900/30'
                : 'bg-slate-900 text-slate-400 border border-slate-800 hover:text-slate-200'
            }`}
          >
            <ArrowDownCircle className="w-4 h-4" />
            <span>SELL</span>
          </button>
        </div>

        <form onSubmit={handleSubmit} className="space-y-4">
          {/* Order Type Toggle */}
          <div>
            <label className="block text-[11px] font-mono text-slate-400 mb-1.5 uppercase">Order Type</label>
            <div className="grid grid-cols-2 gap-2 font-mono text-xs">
              <button
                type="button"
                onClick={() => setOrderType('MARKET')}
                className={`py-1.5 rounded border transition-all ${
                  orderType === 'MARKET'
                    ? 'bg-slate-800 border-slate-600 text-slate-100 font-semibold'
                    : 'bg-slate-950 border-slate-800 text-slate-500'
                }`}
              >
                MARKET
              </button>
              <button
                type="button"
                onClick={() => setOrderType('LIMIT')}
                className={`py-1.5 rounded border transition-all ${
                  orderType === 'LIMIT'
                    ? 'bg-slate-800 border-slate-600 text-slate-100 font-semibold'
                    : 'bg-slate-950 border-slate-800 text-slate-500'
                }`}
              >
                LIMIT
              </button>
            </div>
          </div>

          {/* Quantity Input */}
          <div>
            <div className="flex justify-between items-center mb-1.5">
              <label className="text-[11px] font-mono text-slate-400 uppercase">Quantity (Shares)</label>
              <span className="text-[10px] font-mono text-slate-500">
                Holding: {holdings ? holdings.shares : 0}
              </span>
            </div>
            <input
              type="number"
              min="1"
              max="100000"
              value={quantity}
              onChange={(e) => setQuantity(Math.max(1, parseInt(e.target.value) || 0))}
              className="input-field"
              required
            />
          </div>

          {/* Limit Price Input if LIMIT */}
          {orderType === 'LIMIT' && (
            <div>
              <label className="block text-[11px] font-mono text-slate-400 mb-1.5 uppercase">Limit Price ($)</label>
              <input
                type="number"
                step="0.01"
                value={limitPrice}
                onChange={(e) => setLimitPrice(e.target.value)}
                className="input-field"
                required
              />
            </div>
          )}

          {/* Summary Box */}
          <div className="bg-slate-950 p-3 rounded border border-slate-850 font-mono text-xs space-y-1.5">
            <div className="flex justify-between text-slate-400">
              <span>Exec Price:</span>
              <span className="text-slate-200">${execPrice ? execPrice.toFixed(2) : '0.00'}</span>
            </div>
            <div className="flex justify-between text-slate-400">
              <span>Total Est. Value:</span>
              <span className="font-bold text-slate-100">${totalCost.toLocaleString('en-US', { minimumFractionDigits: 2, maximumFractionDigits: 2 })}</span>
            </div>
          </div>

          {errorMsg && (
            <div className="bg-rose-950/60 border border-rose-800/80 text-rose-300 p-2.5 rounded text-xs flex items-center space-x-2 font-mono">
              <AlertCircle className="w-4 h-4 shrink-0" />
              <span>{errorMsg}</span>
            </div>
          )}

          {successMsg && (
            <div className="bg-emerald-950/60 border border-emerald-800/80 text-emerald-300 p-2.5 rounded text-xs font-mono">
              {successMsg}
            </div>
          )}

          <button
            type="submit"
            disabled={isSubmitting}
            className={`w-full py-2.5 rounded font-mono font-bold text-xs uppercase tracking-wider transition-all ${
              side === 'BUY'
                ? 'bg-emerald-600 hover:bg-emerald-500 text-white'
                : 'bg-rose-600 hover:bg-rose-500 text-white'
            } disabled:opacity-50`}
          >
            {isSubmitting ? 'EXECUTING...' : `CONFIRM ${side} ${quantity} ${ticker}`}
          </button>
        </form>
      </div>
    </div>
  );
}
