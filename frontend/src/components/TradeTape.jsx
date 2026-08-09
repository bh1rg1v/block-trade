import React from 'react';
import { ArrowUpRight, ArrowDownRight } from 'lucide-react';

export default function TradeTape({ trades, ticker }) {
  return (
    <div className="panel flex flex-col h-full">
      <div className="panel-header border-b border-slate-800 flex justify-between items-center">
        <span className="text-slate-200">LIVE TRADE TAPE</span>
        <span className="font-mono text-[10px] text-slate-500 uppercase">{trades.length} TICKS</span>
      </div>

      <div className="flex-1 overflow-y-auto max-h-[320px]">
        <table className="w-full text-left font-mono border-collapse">
          <thead>
            <tr className="bg-slate-950 sticky top-0 border-b border-slate-850">
              <th className="table-header">Time</th>
              <th className="table-header text-right">Price</th>
              <th className="table-header text-right">Size</th>
            </tr>
          </thead>
          <tbody>
            {trades.length === 0 ? (
              <tr>
                <td colSpan="3" className="text-center py-8 text-slate-600 font-mono text-xs">
                  Press "Start Simulation" to stream trades
                </td>
              </tr>
            ) : (
              trades.map((trade, idx) => {
                const isBuy = trade.price >= (trades[idx + 1]?.price || trade.price);
                return (
                  <tr key={idx} className="hover:bg-slate-850/50 transition-colors">
                    <td className="table-cell text-slate-400 text-[11px]">
                      {trade.timestamp ? trade.timestamp.split(' ')[1] : ''}
                    </td>
                    <td className={`table-cell text-right font-semibold ${isBuy ? 'text-emerald-400' : 'text-rose-400'}`}>
                      <div className="flex items-center justify-end space-x-1">
                        {isBuy ? <ArrowUpRight className="w-3 h-3 text-emerald-400" /> : <ArrowDownRight className="w-3 h-3 text-rose-400" />}
                        <span>${trade.price.toFixed(2)}</span>
                      </div>
                    </td>
                    <td className="table-cell text-right text-slate-300">
                      {trade.size.toLocaleString()}
                    </td>
                  </tr>
                );
              })
            )}
          </tbody>
        </table>
      </div>
    </div>
  );
}
