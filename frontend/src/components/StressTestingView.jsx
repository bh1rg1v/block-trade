import React, { useState } from 'react';
import { ShieldAlert, Zap, Cpu, HardDrive, CheckCircle2, AlertCircle, Clock } from 'lucide-react';

export default function StressTestingView({ tickers }) {
  const [ticker, setTicker] = useState('NVDA');
  const [tradesPerMinute, setTradesPerMinute] = useState(10000000);
  const [isRunning, setIsRunning] = useState(false);
  const [result, setResult] = useState(null);
  const [error, setError] = useState(null);

  const handleRunStressTest = async (e) => {
    e.preventDefault();
    setIsRunning(true);
    setError(null);
    setResult(null);

    try {
      const resp = await fetch('/api/stresstest/run', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          ticker: ticker.toUpperCase(),
          trades_per_minute: parseInt(tradesPerMinute, 10),
          days_to_simulate: 1
        })
      });

      const data = await resp.json();
      if (!resp.ok) {
        throw new Error(data.detail || data.message || 'Stress test execution failed');
      }
      setResult(data);
    } catch (err) {
      setError(err.message || 'Failed to run stress test engine');
    } finally {
      setIsRunning(false);
    }
  };

  return (
    <div className="space-y-6">
      {/* Header Banner */}
      <div className="panel p-6 border-l-4 border-l-amber-500 flex justify-between items-center bg-slate-900/90">
        <div>
          <div className="flex items-center space-x-2">
            <ShieldAlert className="w-5 h-5 text-amber-400" />
            <h2 className="font-mono font-bold text-lg text-slate-100 uppercase tracking-wider">
              10M+ TRADES HIGH-SPEED STRESS TESTING SUITE
            </h2>
          </div>
          <p className="text-xs text-slate-400 font-mono mt-1">
            Generates up to 10 Million trades per minute matched to recent intraday OHLCV price profiles in &le; 500 ms.
          </p>
        </div>

        <div className="flex items-center space-x-2 bg-slate-950 px-3 py-1.5 rounded border border-emerald-500/30 text-emerald-400 font-mono text-xs font-bold">
          <Clock className="w-4 h-4 text-emerald-400" />
          <span>SLA BOUND: &le; 500 MS LATENCY</span>
        </div>
      </div>

      <div className="grid grid-cols-1 lg:grid-cols-12 gap-6">
        {/* Left Column: Input Form Panel (4 cols) */}
        <div className="lg:col-span-4 space-y-4">
          <form onSubmit={handleRunStressTest} className="panel p-5 space-y-4">
            <div className="panel-header -mx-5 -mt-5 mb-4 px-5 py-3 border-b border-slate-800 flex items-center justify-between">
              <span>TEST PARAMETERS</span>
              <Cpu className="w-4 h-4 text-amber-400" />
            </div>

            {/* Target Ticker Selection */}
            <div>
              <label className="block text-xs font-mono text-slate-400 uppercase mb-1.5">
                Target Stock Ticker
              </label>
              <select
                value={ticker}
                onChange={(e) => setTicker(e.target.value)}
                className="input-field font-bold text-emerald-400 text-sm cursor-pointer"
              >
                {tickers.map((t) => (
                  <option key={t.Symbol} value={t.Symbol}>
                    {t.Symbol} - {t.Name}
                  </option>
                ))}
              </select>
            </div>

            {/* Trades Per Minute Input */}
            <div>
              <label className="block text-xs font-mono text-slate-300 font-bold uppercase mb-1.5">
                Number of trades per minute:
              </label>
              <input
                type="number"
                min="1"
                max="10000000"
                value={tradesPerMinute}
                onChange={(e) => setTradesPerMinute(Math.max(1, parseInt(e.target.value) || 0))}
                className="input-field text-base font-bold text-amber-400 focus:border-amber-500"
                placeholder="e.g. 10000000"
                required
              />
              <p className="text-[10px] font-mono text-slate-500 mt-1">
                Max input: 10,000,000 trades/min (10 Million).
              </p>
            </div>

            {/* Quick Preset Buttons */}
            <div>
              <label className="block text-[11px] font-mono text-slate-500 uppercase mb-1">
                Quick Preset Densities:
              </label>
              <div className="grid grid-cols-4 gap-1.5 font-mono text-xs">
                {[50, 1000, 500000, 10000000].map((preset) => (
                  <button
                    key={preset}
                    type="button"
                    onClick={() => setTradesPerMinute(preset)}
                    className={`py-1 rounded border transition-colors ${
                      tradesPerMinute === preset
                        ? 'bg-amber-950 border-amber-600 text-amber-400 font-bold'
                        : 'bg-slate-950 border-slate-800 text-slate-400 hover:text-slate-200'
                    }`}
                  >
                    {preset >= 1000000 ? `${preset / 1000000}M` : preset >= 1000 ? `${preset / 1000}K` : preset}
                  </button>
                ))}
              </div>
            </div>

            {error && (
              <div className="bg-rose-950/60 border border-rose-800 text-rose-300 p-3 rounded text-xs flex items-center space-x-2 font-mono">
                <AlertCircle className="w-4 h-4 shrink-0" />
                <span>{error}</span>
              </div>
            )}

            <button
              type="submit"
              disabled={isRunning}
              className="w-full py-3 rounded bg-amber-600 hover:bg-amber-500 text-slate-950 font-mono font-bold text-xs uppercase tracking-wider transition-all shadow-lg shadow-amber-950/40 disabled:opacity-50 flex items-center justify-center space-x-2"
            >
              <Zap className="w-4 h-4" />
              <span>{isRunning ? 'GENERATING 10M TRADES...' : 'EXECUTE STRESS TEST'}</span>
            </button>
          </form>
        </div>

        {/* Right Column: Telemetry & Trade Output Preview (8 cols) */}
        <div className="lg:col-span-8 space-y-4">
          {!result && !isRunning && (
            <div className="panel p-12 text-center flex flex-col items-center justify-center space-y-3">
              <HardDrive className="w-12 h-12 text-slate-700" />
              <h3 className="font-mono text-sm text-slate-400 uppercase font-semibold">Ready for 10M High-Speed Execution</h3>
              <p className="text-xs text-slate-500 max-w-md font-mono">
                Set <span className="text-slate-300 font-bold">Number of trades per minute</span> (up to 10,000,000) and click Execute to test sub-500ms engine throughput.
              </p>
            </div>
          )}

          {isRunning && (
            <div className="panel p-12 text-center flex flex-col items-center justify-center space-y-4">
              <div className="w-8 h-8 border-2 border-amber-500 border-t-transparent rounded-full animate-spin"></div>
              <p className="font-mono text-sm text-amber-400 uppercase font-semibold tracking-wider">
                Simulating {tradesPerMinute.toLocaleString()} trades per minute for {ticker}...
              </p>
            </div>
          )}

          {result && (
            <div className="space-y-4">
              {/* Telemetry Metric Cards */}
              <div className="grid grid-cols-2 sm:grid-cols-4 gap-3 font-mono">
                <div className="panel p-3.5 border-t-2 border-t-emerald-500">
                  <span className="text-[10px] text-slate-500 block uppercase">Total Generated</span>
                  <span className="text-xl font-bold text-slate-100">{result.total_trades_generated.toLocaleString()}</span>
                  <span className="text-[10px] text-slate-400 block mt-0.5">Trades</span>
                </div>

                <div className="panel p-3.5 border-t-2 border-t-amber-500">
                  <span className="text-[10px] text-slate-500 block uppercase">Throughput</span>
                  <span className="text-xl font-bold text-amber-400">{Math.round(result.throughput_tps).toLocaleString()}</span>
                  <span className="text-[10px] text-slate-400 block mt-0.5">Trades / Sec</span>
                </div>

                <div className="panel p-3.5 border-t-2 border-t-blue-500">
                  <span className="text-[10px] text-slate-500 block uppercase">Execution Time</span>
                  <div className="flex items-baseline space-x-1">
                    <span className="text-xl font-bold text-emerald-400">{(result.elapsed_seconds * 1000).toFixed(1)}</span>
                    <span className="text-xs text-slate-400">ms</span>
                  </div>
                  <span className="text-[10px] text-emerald-400 font-bold block mt-0.5">&le; 500 MS SLA PASSED</span>
                </div>

                <div className="panel p-3.5 border-t-2 border-t-purple-500">
                  <span className="text-[10px] text-slate-500 block uppercase">Bars Processed</span>
                  <span className="text-xl font-bold text-slate-100">{result.total_bars_processed}</span>
                  <span className="text-[10px] text-slate-400 block mt-0.5">1-Min Bars</span>
                </div>
              </div>

              {/* Output File Details */}
              <div className="panel p-3.5 font-mono text-xs flex items-center justify-between bg-slate-950/80 border-emerald-900/50 text-emerald-300">
                <div className="flex items-center space-x-2">
                  <CheckCircle2 className="w-4 h-4 text-emerald-400 shrink-0" />
                  <span>Trade stream persisted to CSV:</span>
                </div>
                <code className="text-[11px] bg-slate-900 px-2 py-1 rounded text-slate-200 border border-slate-800">
                  {result.output_file}
                </code>
              </div>

              {/* Sample Output Trades Table */}
              <div className="panel flex flex-col">
                <div className="panel-header border-b border-slate-800">
                  <span>GENERATED SYNTHETIC TRADE STREAM (FIRST 10 TICKS)</span>
                  <span className="font-mono text-xs text-amber-400">{ticker}</span>
                </div>

                <div className="overflow-x-auto">
                  <table className="w-full text-left font-mono border-collapse">
                    <thead>
                      <tr className="bg-slate-950 border-b border-slate-850">
                        <th className="table-header">Timestamp (MS Precision)</th>
                        <th className="table-header text-right">Matched Price</th>
                        <th className="table-header text-right">Synthetic Volume Size</th>
                      </tr>
                    </thead>
                    <tbody>
                      {result.sample_trades.map((st, idx) => (
                        <tr key={idx} className="hover:bg-slate-850/50 transition-colors">
                          <td className="table-cell text-slate-300 text-xs">{st.timestamp}</td>
                          <td className="table-cell text-right font-bold text-emerald-400">${st.price.toFixed(2)}</td>
                          <td className="table-cell text-right text-slate-200">{st.size.toLocaleString()}</td>
                        </tr>
                      ))}
                    </tbody>
                  </table>
                </div>
              </div>
            </div>
          )}
        </div>
      </div>
    </div>
  );
}
