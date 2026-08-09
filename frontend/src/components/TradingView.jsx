import React, { useState, useEffect, useRef } from 'react';
import { Play, Pause, RefreshCw, Calendar, TrendingUp, DollarSign, BarChart2 } from 'lucide-react';
import { ResponsiveContainer, AreaChart, Area, XAxis, YAxis, Tooltip } from 'recharts';

import OrderEntry from './OrderEntry';
import TradeTape from './TradeTape';
import PortfolioTable from './PortfolioTable';

export default function TradingView({ tickers, portfolio, onRefreshPortfolio }) {
  const [selectedTicker, setSelectedTicker] = useState('NVDA');
  const [recentData, setRecentData] = useState(null);
  const [bars, setBars] = useState([]);
  const [currentBarIdx, setCurrentBarIdx] = useState(0);
  const [isPlaying, setIsPlaying] = useState(false);
  const [playbackSpeed, setPlaybackSpeed] = useState(1);
  const [simulatedTrades, setSimulatedTrades] = useState([]);
  const [loading, setLoading] = useState(true);

  const timerRef = useRef(null);

  // Fetch recent day's price data when ticker changes
  useEffect(() => {
    fetchPriceData(selectedTicker);
  }, [selectedTicker]);

  const fetchPriceData = async (tickerSymbol) => {
    setLoading(true);
    setIsPlaying(false);
    if (timerRef.current) clearInterval(timerRef.current);

    try {
      const resp = await fetch(`/api/prices/recent?ticker=${tickerSymbol}`);
      const data = await resp.json();
      setRecentData(data);
      const allBars = data.bars || [];
      setBars(allBars);
      setCurrentBarIdx(allBars.length > 0 ? allBars.length - 1 : 0);

      // Fetch existing trades
      const tradesResp = await fetch(`/api/trades/recent?ticker=${tickerSymbol}&limit=50`);
      const tradesData = await tradesResp.json();
      setSimulatedTrades(tradesData.trades || []);
    } catch (e) {
      console.error('Error fetching price data:', e);
    } finally {
      setLoading(false);
    }
  };

  // Simulation playback loop
  useEffect(() => {
    if (isPlaying && bars.length > 0) {
      const intervalMs = Math.max(100, 1000 / playbackSpeed);
      timerRef.current = setInterval(() => {
        setCurrentBarIdx((prevIdx) => {
          if (prevIdx >= bars.length - 1) {
            setIsPlaying(false);
            clearInterval(timerRef.current);
            return prevIdx;
          }
          const nextIdx = prevIdx + 1;
          const currentBar = bars[nextIdx];

          // Generate 2 synthetic ticks for this bar in playback
          if (currentBar) {
            const mid = (currentBar.Open + currentBar.Close) / 2;
            const newTick = {
              timestamp: currentBar.Datetime,
              price: parseFloat((currentBar.Close >= currentBar.Open ? (mid + currentBar.High) / 2 : (currentBar.Low + mid) / 2).toFixed(2)),
              size: Math.max(10, Math.floor((currentBar.Volume || 1000) / 100))
            };
            setSimulatedTrades((prev) => [newTick, ...prev.slice(0, 49)]);
          }

          return nextIdx;
        });
      }, intervalMs);
    } else {
      if (timerRef.current) clearInterval(timerRef.current);
    }

    return () => {
      if (timerRef.current) clearInterval(timerRef.current);
    };
  }, [isPlaying, bars, playbackSpeed]);

  const toggleSimulation = () => {
    if (!isPlaying) {
      // If at end, loop back to start
      if (currentBarIdx >= bars.length - 1) {
        setCurrentBarIdx(0);
        setSimulatedTrades([]);
      }
    }
    setIsPlaying(!isPlaying);
  };

  const activeBar = bars[currentBarIdx] || {};
  const currentPrice = activeBar.Close || 0.0;
  const visibleBars = bars.slice(0, currentBarIdx + 1);

  const prevBar = bars[Math.max(0, currentBarIdx - 1)] || {};
  const priceChange = currentPrice - (prevBar.Close || currentPrice);
  const percentChange = prevBar.Close ? (priceChange / prevBar.Close) * 100 : 0;

  const currentHolding = (portfolio?.positions || []).find(
    (p) => p.ticker.toUpperCase() === selectedTicker.toUpperCase()
  );

  return (
    <div className="space-y-4">
      {/* Top Controls Bar */}
      <div className="panel p-4 flex flex-wrap items-center justify-between gap-4">
        {/* Ticker & Date Header */}
        <div className="flex items-center space-x-4">
          <select
            value={selectedTicker}
            onChange={(e) => setSelectedTicker(e.target.value)}
            className="input-field text-base font-bold bg-slate-900 border-slate-700 text-emerald-400 font-mono w-40 cursor-pointer"
          >
            {tickers.map((t) => (
              <option key={t.Symbol} value={t.Symbol}>
                {t.Symbol} - {t.Name}
              </option>
            ))}
          </select>

          {recentData && (
            <div className="flex items-center space-x-2 text-xs font-mono text-slate-400 bg-slate-950 px-3 py-1.5 rounded border border-slate-800">
              <Calendar className="w-3.5 h-3.5 text-slate-500" />
              <span>SIMULATING MOST RECENT DAY:</span>
              <span className="font-bold text-slate-200">{recentData.date || 'LATEST'}</span>
            </div>
          )}
        </div>

        {/* Live Simulation Controls */}
        <div className="flex items-center space-x-3">
          <div className="flex items-center space-x-1.5 font-mono text-xs text-slate-400 bg-slate-950 px-3 py-1.5 rounded border border-slate-800">
            <span>BAR:</span>
            <span className="font-bold text-emerald-400">
              {currentBarIdx + 1} / {bars.length}
            </span>
          </div>

          {/* Play / Pause Simulation Button */}
          <button
            onClick={toggleSimulation}
            disabled={bars.length === 0}
            className={`btn ${
              isPlaying
                ? 'bg-amber-600 hover:bg-amber-500 text-white'
                : 'btn-success'
            } flex items-center space-x-2 font-mono text-xs tracking-wider uppercase font-bold`}
          >
            {isPlaying ? <Pause className="w-4 h-4" /> : <Play className="w-4 h-4" />}
            <span>{isPlaying ? 'PAUSE SIMULATION' : 'START SIMULATION'}</span>
          </button>

          {/* Speed Buttons */}
          <div className="flex items-center space-x-1 font-mono text-xs bg-slate-950 p-1 rounded border border-slate-800">
            {[1, 2, 5, 10].map((spd) => (
              <button
                key={spd}
                onClick={() => setPlaybackSpeed(spd)}
                className={`px-2 py-0.5 rounded text-[11px] font-bold ${
                  playbackSpeed === spd ? 'bg-slate-800 text-emerald-400 border border-slate-700' : 'text-slate-500 hover:text-slate-300'
                }`}
              >
                {spd}x
              </button>
            ))}
          </div>
        </div>
      </div>

      {/* Main Trading Dashboard Grid */}
      <div className="grid grid-cols-1 lg:grid-cols-12 gap-4">
        {/* Left Column: Intraday Price Chart & Metrics (7 cols) */}
        <div className="lg:col-span-7 space-y-4">
          <div className="panel p-4 flex flex-col h-[420px]">
            {/* Live Ticker Header */}
            <div className="flex justify-between items-start mb-4 border-b border-slate-800 pb-3">
              <div>
                <h2 className="font-mono font-bold text-2xl text-slate-100 flex items-baseline space-x-2">
                  <span>${currentPrice.toFixed(2)}</span>
                  <span className={`text-sm font-semibold ${priceChange >= 0 ? 'text-emerald-400' : 'text-rose-400'}`}>
                    {priceChange >= 0 ? '+' : ''}${priceChange.toFixed(2)} ({priceChange >= 0 ? '+' : ''}{percentChange.toFixed(2)}%)
                  </span>
                </h2>
                <p className="text-[11px] font-mono text-slate-400 mt-0.5">
                  TIME: {activeBar.Datetime || '--:--'} | TICKER: {selectedTicker}
                </p>
              </div>

              <div className="grid grid-cols-4 gap-3 text-right font-mono text-[11px]">
                <div>
                  <span className="text-slate-500 block">OPEN</span>
                  <span className="text-slate-300 font-semibold">${(activeBar.Open || 0).toFixed(2)}</span>
                </div>
                <div>
                  <span className="text-slate-500 block">HIGH</span>
                  <span className="text-slate-300 font-semibold">${(activeBar.High || 0).toFixed(2)}</span>
                </div>
                <div>
                  <span className="text-slate-500 block">LOW</span>
                  <span className="text-slate-300 font-semibold">${(activeBar.Low || 0).toFixed(2)}</span>
                </div>
                <div>
                  <span className="text-slate-500 block">VOLUME</span>
                  <span className="text-slate-300 font-semibold">{((activeBar.Volume || 0) / 1000).toFixed(1)}k</span>
                </div>
              </div>
            </div>

            {/* Recharts Area Chart */}
            <div className="flex-1 w-full min-h-[280px]">
              {loading ? (
                <div className="flex items-center justify-center h-full text-slate-500 font-mono text-sm">
                  Loading intraday price data...
                </div>
              ) : visibleBars.length === 0 ? (
                <div className="flex items-center justify-center h-full text-slate-500 font-mono text-sm">
                  No data loaded.
                </div>
              ) : (
                <ResponsiveContainer width="100%" height="100%">
                  <AreaChart data={visibleBars} margin={{ top: 10, right: 10, left: 0, bottom: 0 }}>
                    <defs>
                      <linearGradient id="priceGradient" x1="0" y1="0" x2="0" y2="1">
                        <stop offset="5%" stopColor="#10b981" stopOpacity={0.3} />
                        <stop offset="95%" stopColor="#10b981" stopOpacity={0.0} />
                      </linearGradient>
                    </defs>
                    <XAxis
                      dataKey="Datetime"
                      tickFormatter={(str) => (str ? str.split(' ')[1] : '')}
                      stroke="#475569"
                      tick={{ fill: '#64748b', fontSize: 10 }}
                    />
                    <YAxis
                      domain={['auto', 'auto']}
                      orientation="right"
                      stroke="#475569"
                      tick={{ fill: '#64748b', fontSize: 10 }}
                      tickFormatter={(val) => `$${val.toFixed(1)}`}
                    />
                    <Tooltip
                      contentStyle={{ backgroundColor: '#0f172a', borderColor: '#334155', borderRadius: '4px' }}
                      labelStyle={{ color: '#94a3b8', fontSize: '11px', fontFamily: 'monospace' }}
                      itemStyle={{ color: '#10b981', fontSize: '12px', fontFamily: 'monospace' }}
                      formatter={(value) => [`$${parseFloat(value).toFixed(2)}`, 'Close Price']}
                    />
                    <Area type="monotone" dataKey="Close" stroke="#10b981" strokeWidth={2} fillOpacity={1} fill="url(#priceGradient)" />
                  </AreaChart>
                </ResponsiveContainer>
              )}
            </div>
          </div>
        </div>

        {/* Right Column: Order Entry & Trade Tape (5 cols) */}
        <div className="lg:col-span-5 grid grid-cols-1 md:grid-cols-2 gap-4">
          <OrderEntry
            ticker={selectedTicker}
            currentPrice={currentPrice}
            onOrderExecuted={onRefreshPortfolio}
            cash={portfolio?.cash || 0}
            holdings={currentHolding}
          />
          <TradeTape trades={simulatedTrades} ticker={selectedTicker} />
        </div>
      </div>

      {/* Bottom Row: User Portfolio & Executed Orders */}
      <PortfolioTable portfolio={portfolio} onRefresh={onRefreshPortfolio} />
    </div>
  );
}
