import React, { useEffect, useState, useRef } from "react";
import Header from "./components/Header";
import MetricsGrid from "./components/MetricsGrid";
import LiveTradeFeed from "./components/LiveTradeFeed";
import TradeVerifier from "./components/TradeVerifier";
import DatasetVerifier from "./components/DatasetVerifier";
import { cacheSessionTimeline, getCachedSessionTimeline } from "./utils/simulationCache";

const BACKEND_URL = import.meta.env.VITE_API_URL || "http://localhost:8000";
const WS_URL = import.meta.env.VITE_WS_URL || (BACKEND_URL.replace(/^http/, "ws") + "/ws/simulation");


export default function App() {
  const [activeTab, setActiveTab] = useState("live");
  const [statusData, setStatusData] = useState(null);
  const [portfolio, setPortfolio] = useState(null);
  const [currentPrice, setCurrentPrice] = useState(180.00);
  const [trades, setTrades] = useState([]);
  const [recentTradeIds, setRecentTradeIds] = useState([]);
  const [verifierTradeId, setVerifierTradeId] = useState("");
  const [isConnected, setIsConnected] = useState(false);
  const [loadingSync, setLoadingSync] = useState(false);
  const [loadingSim, setLoadingSim] = useState(false);
  const [istClock, setIstClock] = useState("");

  const wsRef = useRef(null);
  const cachedTimelineRef = useRef(null);
  const replayTimerRef = useRef(null);

  // Update IST Clock continuously
  useEffect(() => {
    const updateClock = () => {
      const options = {
        timeZone: "Asia/Kolkata",
        hour: "2-digit",
        minute: "2-digit",
        second: "2-digit",
        hour12: false
      };
      setIstClock(new Intl.DateTimeFormat("en-GB", options).format(new Date()) + " IST");
    };
    updateClock();
    const interval = setInterval(updateClock, 1000);
    return () => clearInterval(interval);
  }, []);

  // Fetch status polling
  const fetchStatus = async () => {
    try {
      const res = await fetch(`${BACKEND_URL}/api/simulation/status`);
      if (res.ok) {
        const data = await res.json();
        setStatusData(data);
        if (data.avg_price && data.avg_price > 0) {
          setCurrentPrice(data.avg_price);
        }
      }
    } catch (e) {
      console.warn("Status fetch warning:", e);
    }
  };

  // Fetch user portfolio
  const fetchPortfolio = async () => {
    try {
      const res = await fetch(`${BACKEND_URL}/api/portfolio`);
      if (res.ok) {
        const data = await res.json();
        setPortfolio(data);
        if (data.orders && data.orders.length > 0) {
          const userIds = data.orders.map((o) => o.trade_id).filter(Boolean);
          setRecentTradeIds((prev) => Array.from(new Set([...userIds, ...prev])));
        }
      }
    } catch (e) {
      console.warn("Portfolio fetch warning:", e);
    }
  };

  useEffect(() => {
    fetchStatus();
    fetchPortfolio();
    const timer = setInterval(() => {
      fetchStatus();
      fetchPortfolio();
    }, 3000);
    return () => clearInterval(timer);
  }, []);

  // Connect WebSocket for live trade feed streaming
  useEffect(() => {
    const connectWS = () => {
      try {
        const ws = new WebSocket(WS_URL);
        wsRef.current = ws;

        ws.onopen = () => {
          setIsConnected(true);
        };

        ws.onmessage = (event) => {
          try {
            const data = JSON.parse(event.data);
            if (data.type === "RUN_STARTED") {
              setStatusData((prev) => ({
                ...prev,
                status: "RUNNING",
                run_number: data.run_number
              }));
              // Preserve user-placed trades while clearing synthetic simulation trades from previous run
              setTrades((prev) => prev.filter((t) => String(t.trade_id).startsWith("TRD-")));
            } else if (data.type === "MINUTE_TICK") {
              if (data.current_price) {
                setCurrentPrice(data.current_price);
              }
              setTrades((prev) => [...data.sample_trades, ...prev].slice(0, 300));
              setStatusData((prev) => ({
                ...prev,
                run_number: data.run_number || prev?.run_number || 1,
                current_minute: data.minute_index,
                total_source_minutes: data.total_source_minutes,
                total_generated_trades: data.total_generated_so_far,
                avg_price: data.current_price || prev?.avg_price
              }));
            } else if (data.type === "SIMULATION_COMPLETE") {
              setStatusData((prev) => ({
                ...prev,
                status: "COMPLETED",
                run_number: data.run_number || prev?.run_number || 1,
                ...data.metadata
              }));
            }
          } catch (err) {
            console.error("WS Parse error:", err);
          }
        };

        ws.onclose = () => {
          setIsConnected(false);
          // Auto-reconnect after 3s
          setTimeout(connectWS, 3000);
        };

        ws.onerror = () => {
          setIsConnected(false);
        };
      } catch (err) {
        setIsConnected(false);
      }
    };

    connectWS();

    return () => {
      if (wsRef.current) {
        wsRef.current.close();
      }
    };
  }, []);

  // Pre-load and cache simulation timeline on the user's laptop (IndexedDB < 50 MB)
  useEffect(() => {
    const initLaptopCache = async () => {
      try {
        const cacheKey = `NVDA-SESSION-${statusData?.source_trading_date || "LATEST"}`;
        const cached = await getCachedSessionTimeline(cacheKey);

        if (cached && cached.length > 0) {
          cachedTimelineRef.current = cached;
          return;
        }

        // Fetch once from backend and cache into laptop's IndexedDB (~1.2 MB)
        const res = await fetch(`${BACKEND_URL}/api/simulation/timeline`);
        if (res.ok) {
          const data = await res.json();
          if (data.frames && data.frames.length > 0) {
            cachedTimelineRef.current = data.frames;
            await cacheSessionTimeline(cacheKey, data.frames);
          }
        }
      } catch (err) {
        console.warn("[Cache] Laptop cache preloading warning:", err);
      }
    };

    initLaptopCache();
  }, [statusData?.source_trading_date]);

  // Instantly plays frames from the laptop's local cache with 0ms delay
  const playFromLaptopCache = (frames) => {
    if (!frames || frames.length === 0) return;
    if (replayTimerRef.current) clearInterval(replayTimerRef.current);

    let frameIdx = 0;
    const firstFrame = frames[0];

    // Frame 0 shows up immediately (0ms delay!)
    if (firstFrame) {
      setCurrentPrice(firstFrame.current_price);
      setTrades((prev) => [
        ...firstFrame.sample_trades,
        ...prev.filter((t) => String(t.trade_id).startsWith("TRD-"))
      ].slice(0, 300));

      setStatusData((prev) => ({
        ...prev,
        status: "RUNNING",
        current_minute: 1,
        total_source_minutes: frames.length,
        total_generated_trades: 100_000,
        avg_price: firstFrame.current_price
      }));
    }

    // Stream subsequent frames from laptop cache smoothly
    replayTimerRef.current = setInterval(() => {
      frameIdx += 1;
      if (frameIdx >= frames.length) {
        clearInterval(replayTimerRef.current);
        setStatusData((prev) => ({
          ...prev,
          status: "COMPLETED",
          current_minute: frames.length,
          total_generated_trades: frames.length * 100_000
        }));
        return;
      }

      const frame = frames[frameIdx];
      setCurrentPrice(frame.current_price);
      setTrades((prev) => [
        ...frame.sample_trades,
        ...prev.filter((t) => String(t.trade_id).startsWith("TRD-"))
      ].slice(0, 300));

      setStatusData((prev) => ({
        ...prev,
        current_minute: frameIdx + 1,
        total_generated_trades: (frameIdx + 1) * 100_000,
        avg_price: frame.current_price
      }));
    }, 400);
  };

  const handleSyncData = async () => {
    setLoadingSync(true);
    try {
      const res = await fetch(`${BACKEND_URL}/api/simulation/sync`, { method: "POST" });
      if (res.ok) {
        await fetchStatus();
        // Refresh laptop cache with freshly synced data
        const timelineRes = await fetch(`${BACKEND_URL}/api/simulation/timeline`);
        if (timelineRes.ok) {
          const tData = await timelineRes.json();
          if (tData.frames) {
            cachedTimelineRef.current = tData.frames;
            await cacheSessionTimeline(`NVDA-SESSION-${tData.source_trading_date}`, tData.frames);
          }
        }
      }
    } catch (err) {
      alert("Data sync error: " + err.message);
    } finally {
      setLoadingSync(false);
    }
  };

  const handleStartSim = async (batchCount = 1) => {
    // 1. INSTANT START: Immediately render from laptop cache (0ms delay!)
    if (cachedTimelineRef.current && cachedTimelineRef.current.length > 0) {
      playFromLaptopCache(cachedTimelineRef.current);
    }

    setLoadingSim(true);
    try {
      // 2. If WebSocket is open, notify engine
      if (wsRef.current && wsRef.current.readyState === WebSocket.OPEN) {
        wsRef.current.send(JSON.stringify({ action: "NEXT_RUN" }));
      }

      // 3. Non-blocking background simulation start: returns in < 10ms!
      const url = `${BACKEND_URL}/api/simulation/start?runs=${batchCount}&reset=true&background=true`;
      const res = await fetch(url, { method: "POST" });
      if (res.ok) {
        const resJson = await res.json();
        setStatusData((prev) => ({
          ...prev,
          status: "RUNNING",
          run_number: resJson.run_number || prev?.run_number || 1
        }));
      }
    } catch (err) {
      console.warn("Simulation trigger note:", err.message);
    } finally {
      setLoadingSim(false);
    }
  };

  const handleResetSim = async () => {
    if (replayTimerRef.current) {
      clearInterval(replayTimerRef.current);
    }
    try {
      const res = await fetch(`${BACKEND_URL}/api/simulation/reset?next_run=true`, { method: "POST" });
      if (res.ok) {
        await fetchStatus();
        setTrades((prev) => prev.filter((t) => String(t.trade_id).startsWith("TRD-")));
      }
    } catch (err) {
      alert("Simulation reset error: " + err.message);
    }
  };

  const handleOrderPlaced = (orderResult) => {
    // Insert placed trade at the top of the trade feed
    const newTradeItem = {
      trade_id: orderResult.trade_id,
      simulation_timestamp: orderResult.timestamp,
      source_timestamp: orderResult.timestamp,
      symbol: orderResult.ticker,
      side: orderResult.side,
      price: orderResult.filled_price,
      quantity: orderResult.quantity
    };

    setTrades((prev) => [newTradeItem, ...prev].slice(0, 300));
    setRecentTradeIds((prev) => [orderResult.trade_id, ...prev.filter((id) => id !== orderResult.trade_id)]);
    fetchPortfolio();
    fetchStatus();
  };

  const handleNavigateToVerifier = (tradeId) => {
    setVerifierTradeId(tradeId);
    setActiveTab("verify_trade");
  };

  return (
    <div className="min-h-screen bg-[#07090e] text-gray-100 flex flex-col font-sans">
      <Header
        activeTab={activeTab}
        setActiveTab={setActiveTab}
        simStatus={statusData?.status}
        istClock={istClock}
        sourceDate={statusData?.source_trading_date}
        runNumber={statusData?.run_number || 1}
        totalRunsCompleted={statusData?.total_runs_completed || 0}
        onSyncData={handleSyncData}
        onStartSim={handleStartSim}
        onResetSim={handleResetSim}
        loadingSync={loadingSync}
        loadingSim={loadingSim}
      />

      <main className="flex-1 p-6 max-w-7xl mx-auto w-full space-y-6">
        {/* Key Metrics Grid */}
        <MetricsGrid statusData={statusData} />

        {/* Tab Views */}
        {activeTab === "live" && (
          <LiveTradeFeed
            trades={trades}
            isConnected={isConnected}
            currentPrice={currentPrice}
            portfolio={portfolio}
            onOrderPlaced={handleOrderPlaced}
            onNavigateToVerifier={handleNavigateToVerifier}
            backendUrl={BACKEND_URL}
          />
        )}

        {activeTab === "verify_trade" && (
          <TradeVerifier
            backendUrl={BACKEND_URL}
            initialTradeId={verifierTradeId}
            recentTradeIds={recentTradeIds}
          />
        )}

        {activeTab === "dataset_l2" && (
          <DatasetVerifier backendUrl={BACKEND_URL} />
        )}
      </main>

      <footer className="border-t border-[#1a2538] py-4 px-6 text-center text-xs text-gray-500 font-mono">
        NVDA Verifiable High-Throughput Trade Simulation Platform • yfinance Data Source • ClickHouse Operational DB • Parquet Zstd • Canonical CBOR SHA-256 Merkle Tree • Ethereum L2 Commitment
      </footer>
    </div>
  );
}
