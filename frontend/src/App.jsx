import React, { useEffect, useState, useRef } from "react";
import Header from "./components/Header";
import MetricsGrid from "./components/MetricsGrid";
import LiveTradeFeed from "./components/LiveTradeFeed";
import TradeVerifier from "./components/TradeVerifier";
import DatasetVerifier from "./components/DatasetVerifier";

const BACKEND_URL = import.meta.env.VITE_API_URL || "http://localhost:8000";
const WS_URL = import.meta.env.VITE_WS_URL || (BACKEND_URL.replace(/^http/, "ws") + "/ws/simulation");


export default function App() {
  const [activeTab, setActiveTab] = useState("live");
  const [statusData, setStatusData] = useState(null);
  const [trades, setTrades] = useState([]);
  const [isConnected, setIsConnected] = useState(false);
  const [loadingSync, setLoadingSync] = useState(false);
  const [loadingSim, setLoadingSim] = useState(false);
  const [istClock, setIstClock] = useState("");

  const wsRef = useRef(null);

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
      }
    } catch (e) {
      console.warn("Status fetch warning:", e);
    }
  };

  useEffect(() => {
    fetchStatus();
    const timer = setInterval(fetchStatus, 3000);
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
            if (data.type === "MINUTE_TICK") {
              setTrades((prev) => [...data.sample_trades, ...prev].slice(0, 300));
              setStatusData((prev) => ({
                ...prev,
                current_minute: data.minute_index,
                total_source_minutes: data.total_source_minutes,
                total_generated_trades: data.total_generated_so_far
              }));
            } else if (data.type === "SIMULATION_COMPLETE") {
              setStatusData((prev) => ({
                ...prev,
                status: "COMPLETED",
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

  const handleSyncData = async () => {
    setLoadingSync(true);
    try {
      const res = await fetch(`${BACKEND_URL}/api/simulation/sync`, { method: "POST" });
      if (res.ok) {
        await fetchStatus();
      }
    } catch (err) {
      alert("Data sync error: " + err.message);
    } finally {
      setLoadingSync(false);
    }
  };

  const handleStartSim = async () => {
    setLoadingSim(true);
    try {
      const res = await fetch(`${BACKEND_URL}/api/simulation/start`, { method: "POST" });
      if (res.ok) {
        await fetchStatus();
      }
    } catch (err) {
      alert("Simulation trigger error: " + err.message);
    } finally {
      setLoadingSim(false);
    }
  };

  return (
    <div className="min-h-screen bg-[#07090e] text-gray-100 flex flex-col font-sans">
      <Header
        activeTab={activeTab}
        setActiveTab={setActiveTab}
        simStatus={statusData?.status}
        istClock={istClock}
        sourceDate={statusData?.source_trading_date}
        onSyncData={handleSyncData}
        onStartSim={handleStartSim}
        loadingSync={loadingSync}
        loadingSim={loadingSim}
      />

      <main className="flex-1 p-6 max-w-7xl mx-auto w-full">
        {/* Key Metrics Grid */}
        <MetricsGrid statusData={statusData} />

        {/* Tab Views */}
        {activeTab === "live" && (
          <LiveTradeFeed trades={trades} isConnected={isConnected} />
        )}

        {activeTab === "verify_trade" && (
          <TradeVerifier backendUrl={BACKEND_URL} />
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
