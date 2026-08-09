import React, { useState, useEffect } from 'react';
import Navigation from './components/Navigation';
import TradingView from './components/TradingView';
import StressTestingView from './components/StressTestingView';

export default function App() {
  const [activeTab, setActiveTab] = useState('trading');
  const [tickers, setTickers] = useState([]);
  const [portfolio, setPortfolio] = useState({ cash: 100000.0, total_equity: 100000.0, positions: [], orders: [] });

  useEffect(() => {
    fetchTickers();
    fetchPortfolio();
  }, []);

  const fetchTickers = async () => {
    try {
      const resp = await fetch('/api/tickers');
      const data = await resp.json();
      setTickers(data || []);
    } catch (e) {
      console.error('Failed to load tickers:', e);
    }
  };

  const fetchPortfolio = async () => {
    try {
      const resp = await fetch('/api/portfolio');
      const data = await resp.json();
      setPortfolio(data);
    } catch (e) {
      console.error('Failed to load portfolio:', e);
    }
  };

  return (
    <div className="min-h-screen bg-slate-950 text-slate-100 flex flex-col font-sans">
      <Navigation activeTab={activeTab} setActiveTab={setActiveTab} portfolio={portfolio} />

      <main className="flex-1 max-w-[1600px] w-full mx-auto p-4 sm:p-6 space-y-6">
        {activeTab === 'trading' ? (
          <TradingView
            tickers={tickers}
            portfolio={portfolio}
            onRefreshPortfolio={fetchPortfolio}
          />
        ) : (
          <StressTestingView tickers={tickers} />
        )}
      </main>
    </div>
  );
}
