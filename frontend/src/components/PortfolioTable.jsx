import React, { useState } from 'react';
import { PieChart, ListFilter, CheckCircle2, XCircle } from 'lucide-react';

export default function PortfolioTable({ portfolio, onRefresh }) {
  const [tab, setTab] = useState('positions');

  const positions = portfolio?.positions || [];
  const orders = portfolio?.orders || [];

  return (
    <div className="panel flex flex-col">
      {/* Sub-header Tabs */}
      <div className="panel-header border-b border-slate-800 flex justify-between items-center bg-slate-900/80">
        <div className="flex space-x-4">
          <button
            onClick={() => setTab('positions')}
            className={`flex items-center space-x-2 pb-1 text-xs font-semibold uppercase tracking-wider border-b-2 transition-all ${
              tab === 'positions'
                ? 'border-emerald-400 text-emerald-400'
                : 'border-transparent text-slate-400 hover:text-slate-200'
            }`}
          >
            <PieChart className="w-3.5 h-3.5" />
            <span>POSITIONS ({positions.length})</span>
          </button>

          <button
            onClick={() => setTab('orders')}
            className={`flex items-center space-x-2 pb-1 text-xs font-semibold uppercase tracking-wider border-b-2 transition-all ${
              tab === 'orders'
                ? 'border-emerald-400 text-emerald-400'
                : 'border-transparent text-slate-400 hover:text-slate-200'
            }`}
          >
            <ListFilter className="w-3.5 h-3.5" />
            <span>ORDER HISTORY ({orders.length})</span>
          </button>
        </div>

        <button
          onClick={onRefresh}
          className="text-[11px] font-mono text-slate-400 hover:text-slate-200 underline"
        >
          REFRESH CSV DATA
        </button>
      </div>

      <div className="overflow-x-auto">
        {tab === 'positions' ? (
          <table className="w-full text-left font-mono border-collapse">
            <thead>
              <tr className="bg-slate-950 border-b border-slate-850">
                <th className="table-header">Ticker</th>
                <th className="table-header text-right">Shares</th>
                <th className="table-header text-right">Avg Cost</th>
                <th className="table-header text-right">Current Price</th>
                <th className="table-header text-right">Market Value</th>
                <th className="table-header text-right">Unrealized P&L</th>
              </tr>
            </thead>
            <tbody>
              {positions.length === 0 ? (
                <tr>
                  <td colSpan="6" className="text-center py-6 text-slate-500 text-xs">
                    No open positions. Use the order execution panel to buy stocks.
                  </td>
                </tr>
              ) : (
                positions.map((pos) => {
                  const pnl = pos.unrealized_pnl;
                  return (
                    <tr key={pos.ticker} className="hover:bg-slate-850/50 transition-colors">
                      <td className="table-cell font-bold text-slate-100">{pos.ticker}</td>
                      <td className="table-cell text-right text-slate-300">{pos.shares.toLocaleString()}</td>
                      <td className="table-cell text-right text-slate-400">${pos.average_cost.toFixed(2)}</td>
                      <td className="table-cell text-right text-slate-200">${pos.current_price.toFixed(2)}</td>
                      <td className="table-cell text-right text-slate-100 font-semibold">${pos.market_value.toLocaleString('en-US', { minimumFractionDigits: 2 })}</td>
                      <td className={`table-cell text-right font-bold ${pnl >= 0 ? 'text-emerald-400' : 'text-rose-400'}`}>
                        {pnl >= 0 ? '+' : ''}${pnl.toLocaleString('en-US', { minimumFractionDigits: 2, maximumFractionDigits: 2 })}
                      </td>
                    </tr>
                  );
                })
              )}
            </tbody>
          </table>
        ) : (
          <table className="w-full text-left font-mono border-collapse">
            <thead>
              <tr className="bg-slate-950 border-b border-slate-850">
                <th className="table-header">Order ID</th>
                <th className="table-header">Timestamp</th>
                <th className="table-header">Ticker</th>
                <th className="table-header">Side</th>
                <th className="table-header">Type</th>
                <th className="table-header text-right">Qty</th>
                <th className="table-header text-right">Price</th>
                <th className="table-header text-center">Status</th>
              </tr>
            </thead>
            <tbody>
              {orders.length === 0 ? (
                <tr>
                  <td colSpan="8" className="text-center py-6 text-slate-500 text-xs">
                    No orders logged yet.
                  </td>
                </tr>
              ) : (
                [...orders].reverse().map((ord, idx) => (
                  <tr key={idx} className="hover:bg-slate-850/50 transition-colors">
                    <td className="table-cell text-slate-400 text-xs">{ord.order_id}</td>
                    <td className="table-cell text-slate-500 text-[11px]">{ord.timestamp}</td>
                    <td className="table-cell font-bold text-slate-200">{ord.ticker}</td>
                    <td className="table-cell">
                      <span className={`px-2 py-0.5 rounded text-[10px] font-bold ${
                        ord.side === 'BUY' ? 'bg-emerald-950 text-emerald-400 border border-emerald-800' : 'bg-rose-950 text-rose-400 border border-rose-800'
                      }`}>
                        {ord.side}
                      </span>
                    </td>
                    <td className="table-cell text-slate-400 text-xs">{ord.order_type}</td>
                    <td className="table-cell text-right text-slate-300">{ord.quantity}</td>
                    <td className="table-cell text-right text-slate-200">${floatVal(ord.filled_price || ord.price).toFixed(2)}</td>
                    <td className="table-cell text-center">
                      {ord.status === 'FILLED' ? (
                        <span className="inline-flex items-center space-x-1 text-emerald-400 text-xs">
                          <CheckCircle2 className="w-3.5 h-3.5" />
                          <span>FILLED</span>
                        </span>
                      ) : (
                        <span className="inline-flex items-center space-x-1 text-rose-400 text-xs">
                          <XCircle className="w-3.5 h-3.5" />
                          <span>REJECTED</span>
                        </span>
                      )}
                    </td>
                  </tr>
                ))
              )}
            </tbody>
          </table>
        )}
      </div>
    </div>
  );
}

function floatVal(val) {
  const parsed = parseFloat(val);
  return isNaN(parsed) ? 0.0 : parsed;
}
