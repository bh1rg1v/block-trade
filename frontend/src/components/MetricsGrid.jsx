import React from "react";
import { Zap, TrendingUp, ShieldCheck, Layers } from "lucide-react";

export default function MetricsGrid({ statusData }) {
  const totalGenerated = statusData?.total_generated_trades || 0;
  const currentMinute = statusData?.current_minute || 0;
  const totalMinutes = statusData?.total_source_minutes || 390;
  const progressPct = Math.min(100, Math.round((currentMinute / totalMinutes) * 100));

  const buyCount = statusData?.buy_count || 0;
  const sellCount = statusData?.sell_count || 0;
  const buyRatio = (buyCount + sellCount) > 0 ? Math.round((buyCount / (buyCount + sellCount)) * 100) : 52;

  return (
    <div className="grid grid-cols-1 md:grid-cols-4 gap-4 mb-6">
      {/* Metric 1: Total Generated Volume */}
      <div className="panel p-4 flex flex-col justify-between border-l-4 border-l-[#76B900]">
        <div className="flex items-center justify-between text-gray-400 text-xs font-semibold uppercase">
          <span>Total Generated Volume</span>
          <Zap className="w-4 h-4 text-[#76B900]" />
        </div>
        <div className="my-2">
          <div className="text-2xl font-bold font-mono text-white tracking-tight">
            {totalGenerated.toLocaleString()}
          </div>
          <div className="text-[11px] text-gray-400">Target: ~39,000,000 trades/day</div>
        </div>
        <div className="w-full bg-[#070a11] h-1.5 rounded-full overflow-hidden">
          <div
            className="bg-[#76B900] h-full transition-all duration-300"
            style={{ width: `${progressPct}%` }}
          />
        </div>
      </div>

      {/* Metric 2: Current Throughput */}
      <div className="panel p-4 flex flex-col justify-between border-l-4 border-l-blue-500">
        <div className="flex items-center justify-between text-gray-400 text-xs font-semibold uppercase">
          <span>Current Generation Rate</span>
          <TrendingUp className="w-4 h-4 text-blue-400" />
        </div>
        <div className="my-2">
          <div className="text-2xl font-bold font-mono text-blue-400">
            100,000 <span className="text-sm font-normal text-gray-400">trades/min</span>
          </div>
          <div className="text-[11px] text-gray-400">Vectorized PCG64 Engine • 1,667 tps</div>
        </div>
        <div className="text-[11px] text-blue-400/80 font-mono">
          Batch size: 100k / simulated minute
        </div>
      </div>

      {/* Metric 3: BUY vs SELL Order Distribution */}
      <div className="panel p-4 flex flex-col justify-between border-l-4 border-l-purple-500">
        <div className="flex items-center justify-between text-gray-400 text-xs font-semibold uppercase">
          <span>BUY / SELL Distribution</span>
          <Layers className="w-4 h-4 text-purple-400" />
        </div>
        <div className="my-2">
          <div className="flex items-center justify-between text-xs font-mono font-semibold mb-1">
            <span className="text-[#76B900]">BUY: {buyRatio}%</span>
            <span className="text-red-400">SELL: {100 - buyRatio}%</span>
          </div>
          <div className="w-full bg-red-500/20 h-2 rounded-full overflow-hidden flex">
            <div className="bg-[#76B900] h-full" style={{ width: `${buyRatio}%` }} />
            <div className="bg-red-500 h-full" style={{ width: `${100 - buyRatio}%` }} />
          </div>
        </div>
        <div className="text-[11px] text-gray-400">Balanced order flow simulator</div>
      </div>

      {/* Metric 4: Simulation Session Progress */}
      <div className="panel p-4 flex flex-col justify-between border-l-4 border-l-amber-500">
        <div className="flex items-center justify-between text-gray-400 text-xs font-semibold uppercase">
          <span>Session Replay Progress</span>
          <ShieldCheck className="w-4 h-4 text-amber-400" />
        </div>
        <div className="my-2">
          <div className="text-2xl font-bold font-mono text-amber-400">
            {currentMinute} / {totalMinutes} <span className="text-sm font-normal text-gray-400">mins</span>
          </div>
          <div className="text-[11px] text-gray-400">Replaying NVDA 09:30 to 16:00 IST</div>
        </div>
        <div className="text-[11px] text-amber-400 font-mono flex items-center justify-between">
          <span>Progress: {progressPct}%</span>
          <span className="text-gray-400">Run #{statusData?.run_number || 1}</span>
        </div>
      </div>
    </div>
  );
}
