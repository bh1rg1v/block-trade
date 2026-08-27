# NVDA Verifiable High-Throughput Trade Simulation Platform

[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
[![Python: 3.10+](https://img.shields.io/badge/python-3.10%2B-blue.svg)](https://www.python.org/)
[![React: 18](https://img.shields.io/badge/react-18-cyan.svg)](https://react.dev/)
[![FastAPI](https://img.shields.io/badge/FastAPI-0.100%2B-green.svg)](https://fastapi.tiangolo.com/)
[![Ethereum L2](https://img.shields.io/badge/Ethereum-Layer--2-purple.svg)](https://ethereum.org/)

An enterprise-grade, high-throughput financial trade simulation and cryptographic verification platform centered exclusively on **NVIDIA Corporation (NVDA)**. The platform ingests 1-minute historical market data using `yfinance`, maintains a deduplicated historical dataset in `NVDA.csv`, isolates the most recent completed U.S. trading session, replays it from **09:30 IST to 16:00 IST** at **100,000 simulated trades per market minute** (~39,000,000 trades/day), streams trades live to a React frontend, stores them in ClickHouse, constructs binary SHA-256 Merkle trees over Canonical CBOR encodings, publishes dataset artifacts to IPFS/Arweave, and anchors Merkle roots on an Ethereum Layer-2 blockchain for independent public verification.

---

## 💡 Fundamental Core Principle

> **The blockchain does not store 100,000 individual trades per minute. It stores an immutable cryptographic commitment to the complete dataset.**

By decoupling high-throughput trade generation ($1,667\text{ trades/second}$) from public state verification, the platform achieves **infinite scalability** while offering **absolute auditability**. Anyone can download the canonical Parquet dataset or query an individual trade and independently verify its membership against the public Ethereum L2 smart contract commitment without relying on or trusting the platform backend.

---

## 🏗 System Architecture

```text
                               ┌────────────────────────┐
                               │     yfinance API       │
                               │  NVDA 1-minute data    │
                               └───────────┬────────────┘
                                           │ Daily Sync (Before 09:30 IST)
                                           ▼
                               ┌────────────────────────┐
                               │        NVDA.csv        │
                               │ Local Historical Cache │
                               └───────────┬────────────┘
                                           │ Select Latest Completed Session
                                           ▼
                               ┌────────────────────────┐
                               │   Simulation Engine    │
                               │  (09:30 - 16:00 IST)   │
                               │   100k trades / min    │
                               └─────┬──────────────┬───┘
                                     │              │
                    ┌────────────────┘              └────────────────┐
                    ▼                                                ▼
         ┌─────────────────────┐                          ┌─────────────────────┐
         │ ClickHouse Storage  │                          │  WebSocket Stream   │
         │ Operational DB      │                          └──────────┬──────────┘
         └──────────┬──────────┘                                     │
                    │                                                ▼
                    │ Post-Simulation                     ┌─────────────────────┐
                    ▼                                     │ React Live Feed UI  │
         ┌─────────────────────┐                          └─────────────────────┘
         │ Canonical Parquet   │
         │ Dataset (Zstandard) │
         └──────────┬──────────┘
                    │
                    ▼
         ┌─────────────────────┐
         │ Merkle Tree Engine  │
         │ (Canonical CBOR +   │
         │  SHA-256 Hashes)    │
         └─────┬───────────┬───┘
               │           │
               ▼           ▼
         ┌───────────┐   ┌─────────────────────────┐
         │   IPFS /  │   │ Solidity Smart Contract │
         │  Arweave  │   │ (Ethereum L2 Commitment)│
         └───────────┘   └─────────────────────────┘
```

---

## ⚡ Key Features

1. **Exclusive NVIDIA Focus**: Uses `NVDA` market data exclusively.
2. **Automated Daily Data Acquisition**:
   - Ingests latest 1-minute NVDA historical market data via `yfinance`.
   - Validates required fields (`Datetime`, `Open`, `High`, `Low`, `Close`, `Volume`).
   - Deduplicates and chronologically appends records into local source `NVDA.csv`.
3. **Session Discovery Engine**:
   - Distinguishes U.S. Eastern (`America/New_York`) and India Standard Time (`Asia/Kolkata`).
   - Dynamically identifies the most recent completed regular U.S. trading session (09:30 ET - 16:00 ET, ~390 bars), automatically bypassing weekends and market holidays.
4. **100,000 Trades / Minute Replay Engine**:
   - Vectorized NumPy/PCG64 generator generating 100,000 trades per simulated market minute (~1,667 trades/sec).
   - Total daily output of ~39,000,000 trades per regular trading day.
   - Fully deterministic and reproducible using explicit seed and configuration hashes.
5. **Operational Query Storage (ClickHouse)**:
   - Persists simulation batches for instant sub-millisecond queries (e.g. trade by ID, minute slice, BUY/SELL volume stats). Includes embedded SQLite fallback for standalone development.
6. **Real-time WebSocket Feed**:
   - FastAPI WebSocket endpoint streaming live trades and minute aggregate stats to the frontend.
7. **Canonical CBOR & SHA-256 Merkle Verification**:
   - Serializes trade records into RFC 8949 **Canonical CBOR** bytes.
   - Hashes CBOR bytes with **SHA-256**.
   - Constructs binary Merkle tree and yields $O(\log N)$ audit proofs.
8. **Decentralized Storage & Ethereum L2 Anchoring**:
   - Exports compressed `trades.parquet` (Zstandard compression) + `metadata.json`.
   - Publishes datasets to IPFS (Content Identifier CIDv1) and Arweave.
   - Commits dataset hashes and Merkle roots to Solidity smart contract (`DatasetRegistry.sol`) deployed on Ethereum L2 (Base / Arbitrum / Optimism).
9. **React + TypeScript Verification Interface**:
   - Live Dashboard: Real-time IST clock, source session date, 100k trades/min throughput badge, 1,667 trades/sec counter, progress bar, BUY/SELL distribution.
   - **Independent Trade Verifier**: Look up any trade ID (e.g. `#000000083291`), inspect raw JSON, Canonical CBOR hex, leaf hash, Merkle proof tree, and execute independent verification against the Ethereum L2 Merkle root (`✓ Trade Verified`).
   - **Full Dataset Verifier**: Inspect Parquet file metadata, IPFS CID, and Ethereum L2 smart contract commitment state.

---

## 📊 Trade Data Structure

Every generated trade adheres to a deterministic schema:

```json
{
  "trade_id": "000000083291",
  "simulation_timestamp": "2026-08-28T09:31:42.123+05:30",
  "source_timestamp": "2026-08-27T09:31:00-04:00",
  "symbol": "NVDA",
  "side": "BUY",
  "price": 181.42,
  "quantity": 37
}
```

---

## 📁 Repository Structure

```text
block-trade/
├── backend/
│   ├── app/
│   │   ├── __init__.py
│   │   ├── main.py                   # FastAPI REST API & WebSocket Server
│   │   ├── data_acquisition.py       # yfinance fetching & NVDA.csv maintenance
│   │   ├── simulator.py              # Vectorized 100k trades/min replay engine
│   │   ├── merkle_engine.py          # Canonical CBOR, SHA-256 & Merkle Tree logic
│   │   ├── clickhouse_storage.py     # ClickHouse operational query layer
│   │   ├── dataset_exporter.py       # Parquet + Zstd exporter & SHA-256 metadata
│   │   ├── storage_publisher.py      # IPFS CID & Arweave publisher layer
│   │   └── blockchain_committer.py   # Ethereum L2 commitment integration
│   ├── requirements.txt              # Python backend dependencies
│   └── NVDA.csv                      # Maintained local historical market data
├── contracts/
│   └── DatasetRegistry.sol           # Solidity L2 Commitment Smart Contract
├── frontend/
│   ├── src/
│   │   ├── components/
│   │   │   ├── Header.jsx            # Platform header & simulation controls
│   │   │   ├── MetricsGrid.jsx       # Live trade counters & throughput gauges
│   │   │   ├── LiveTradeFeed.jsx     # Virtualized live trade feed table
│   │   │   ├── TradeVerifier.jsx     # Independent trade verification UI
│   │   │   └── DatasetVerifier.jsx   # IPFS & Ethereum L2 dataset explorer
│   │   ├── App.jsx                   # Main React application & WebSockets
│   │   ├── index.css                 # Styling system with NVIDIA green accents
│   │   └── main.jsx
│   ├── package.json
│   ├── vercel.json                   # Vercel deployment configuration
│   └── .env.example
├── tests/
│   └── test_platform.py              # Pytest end-to-end verification suite
├── render.yaml                       # Render backend blueprint file
├── paper.md                          # 8-page academic research paper on trustless systems
└── README.md                         # Project documentation
```

---

## 🚀 Quick Start (Local Setup)

### 1. Prerequisites
- Python 3.10 or higher
- Node.js 18 or higher

### 2. Backend Setup
```bash
# Clone repository
git clone https://github.com/bh1rg1v/block-trade.git
cd block-trade

# Install backend dependencies
python -m pip install -r backend/requirements.txt

# Run automated unit test suite
python -m pytest tests/

# Start FastAPI backend server
python -m uvicorn backend.app.main:app --reload --port 8000
```
Backend API will run at `http://localhost:8000`.

### 3. Frontend Setup
```bash
# In a new terminal window
cd frontend

# Install frontend dependencies
npm install

# Start Vite React development server
npm run dev
```
Frontend dashboard will run at `http://localhost:5173`.

---

## 🌐 Production Deployment Guide

### Deploying Backend to Render
1. Push your repository to GitHub.
2. Go to [Render Dashboard](https://dashboard.render.com/) -> **New** -> **Web Service** (or **Blueprint**).
3. Connect your repository — Render automatically reads `render.yaml`.
4. Render configuration settings:
   - **Build Command**: `pip install -r backend/requirements.txt`
   - **Start Command**: `uvicorn backend.app.main:app --host 0.0.0.0 --port $PORT`
5. Click **Deploy**.

### Deploying Frontend to Vercel
1. Go to [Vercel Dashboard](https://vercel.com/) -> **New Project**.
2. Select your repository and set Root Directory to `frontend`.
3. Add Environment Variables:
   - `VITE_API_URL`: `https://<your-backend-name>.onrender.com`
   - `VITE_WS_URL`: `wss://<your-backend-name>.onrender.com/ws/simulation`
4. Click **Deploy**.

---

## 🔬 Independent Cryptographic Verification Flow

To verify any trade independently without trusting the application backend:

```text
┌────────────────────────────────────────────────────────┐
│ 1. Download Canonical CBOR specification for Trade T   │
└───────────────────────────┬────────────────────────────┘
                            │
                            ▼
┌────────────────────────────────────────────────────────┐
│ 2. Compute h_T = SHA-256(Canonical_CBOR(T))            │
└───────────────────────────┬────────────────────────────┘
                            │
                            ▼
┌────────────────────────────────────────────────────────┐
│ 3. Fetch Merkle Proof path π_T from API or Parquet    │
└───────────────────────────┬────────────────────────────┘
                            │
                            ▼
┌────────────────────────────────────────────────────────┐
│ 4. Reconstruct root: R_calculated = MerkleFold(h_T, π_T)│
└───────────────────────────┬────────────────────────────┘
                            │
                            ▼
┌────────────────────────────────────────────────────────┐
│ 5. Read R_blockchain from DatasetRegistry.sol (L2)     │
└───────────────────────────┬────────────────────────────┘
                            │
                            ▼
           R_calculated == R_blockchain ?
           /                            \
      [ YES ]                         [ NO ]
         │                               │
         ▼                               ▼
✓ Trade Verified               ❌ Fraud / Tampering
```

---

## 📄 Academic Research Paper

For an in-depth mathematical analysis of high-throughput trade simulation, storage-computation decoupling, Canonical CBOR serialization, Merkle tree audit proofs, and the critical importance of trustless architectures in modern financial engineering, read our full 8-page whitepaper:

👉 **[paper.md](file:///d:/github/block-trade/paper.md)**

---

## 📜 License

This project is released under the **MIT License**.
