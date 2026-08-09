import React from 'react';
import { Activity, ShieldAlert, Wallet, TrendingUp, Layers } from 'lucide-react';

export default function Navigation({ activeTab, setActiveTab, portfolio }) {
  const cash = portfolio?.cash || 0;
  const totalEquity = portfolio?.total_equity || cash;
  const pnl = totalEquity - 100000;
  const pnlPercent = (pnl / 100000) * 100;

  return (
    <header className="bg-slate-900 border-b border-slate-800 px-6 py-3.5 flex items-center justify-between sticky top-0 z-50">
      {/* Brand & Mode Switcher */}
      <div className="flex items-center space-x-8">
        <div className="flex items-center space-x-2.5">
          <div className="bg-emerald-500/10 border border-emerald-500/30 p-1.5 rounded">
            <Activity className="w-5 h-5 text-emerald-400" />
          </div>
          <div>
            <h1 className="font-mono font-bold text-base tracking-wider text-slate-100 uppercase">
              BLOCK<span className="text-emerald-400">TRADE</span>
            </h1>
            <p className="text-[10px] text-slate-400 tracking-wider font-mono">SIMULATION & STRESS TEST ENGINE</p>
          </div>
        </div>

        <nav className="flex items-center space-x-1 bg-slate-950 p-1 rounded-md border border-slate-800">
          <button
            onClick={() => setActiveTab('trading')}
            className={`flex items-center space-x-2 px-4 py-1.5 text-xs font-semibold rounded transition-all ${
              activeTab === 'trading'
                ? 'bg-slate-800 text-emerald-400 shadow-sm border border-slate-700'
                : 'text-slate-400 hover:text-slate-200'
            }`}
          >
            <TrendingUp className="w-3.5 h-3.5" />
            <span>TRADING</span>
          </button>

          <button
            onClick={() => setActiveTab('stress-testing')}
            className={`flex items-center space-x-2 px-4 py-1.5 text-xs font-semibold rounded transition-all ${
              activeTab === 'stress-testing'
                ? 'bg-slate-800 text-amber-400 shadow-sm border border-slate-700'
                : 'text-slate-400 hover:text-slate-200'
            }`}
          >
            <ShieldAlert className="w-3.5 h-3.5" />
            <span>STRESS TESTING</span>
          </button>
        </nav>
      </div>

      {/* Account Equity Bar */}
      <div className="flex items-center space-x-6 font-mono text-xs">
        <div className="flex items-center space-x-2 border-r border-slate-800 pr-6">
          <Wallet className="w-4 h-4 text-slate-400" />
          <span className="text-slate-400 uppercase">Cash:</span>
          <span className="font-semibold text-slate-200">${cash.toLocaleString('en-US', { minimumFractionDigits: 2, maximumFractionDigits: 2 })}</span>
        </div>

        <div className="flex items-center space-x-2 border-r border-slate-800 pr-6">
          <Layers className="w-4 h-4 text-slate-400" />
          <span className="text-slate-400 uppercase">Equity:</span>
          <span className="font-semibold text-slate-100">${totalEquity.toLocaleString('en-US', { minimumFractionDigits: 2, maximumFractionDigits: 2 })}</span>
        </div>

        <div className="flex items-center space-x-2">
          <span className="text-slate-400 uppercase">Total P&L:</span>
          <span className={`font-bold ${pnl >= 0 ? 'text-emerald-400' : 'text-rose-400'}`}>
            {pnl >= 0 ? '+' : ''}${pnl.toLocaleString('en-US', { minimumFractionDigits: 2, maximumFractionDigits: 2 })} ({pnl >= 0 ? '+' : ''}{pnlPercent.toFixed(2)}%)
          </span>
        </div>
      </div>
    </header>
  );
}
