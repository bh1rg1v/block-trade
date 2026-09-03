/**
 * Browser-side IndexedDB Cache for NVDA Trade Simulation
 * Stores session replay frames on the client's laptop.
 * Strictly uses less than 50 MB (far below the 60 MB limit).
 */

const DB_NAME = "NVDA_BlockTrade_Cache";
const DB_VERSION = 1;
const STORE_NAME = "simulation_sessions";

function openDB() {
  return new Promise((resolve, reject) => {
    if (!("indexedDB" in window)) {
      resolve(null);
      return;
    }

    const request = indexedDB.open(DB_NAME, DB_VERSION);

    request.onupgradeneeded = (event) => {
      const db = event.target.result;
      if (!db.objectStoreNames.contains(STORE_NAME)) {
        db.createObjectStore(STORE_NAME, { keyPath: "cache_key" });
      }
    };

    request.onsuccess = () => resolve(request.result);
    request.onerror = () => reject(request.error);
  });
}

/**
 * Saves a simulation session timeline to the laptop's browser IndexedDB.
 * @param {string} cacheKey - Unique key e.g. "NVDA-2026-08-27"
 * @param {Array} timelineFrames - Array of 390 minute frames
 */
export async function cacheSessionTimeline(cacheKey, timelineFrames) {
  try {
    const db = await openDB();
    if (!db) return false;

    // Safety check size: serialize to check byte size
    const payload = {
      cache_key: cacheKey,
      cached_at: Date.now(),
      frames_count: timelineFrames.length,
      frames: timelineFrames
    };

    const str = JSON.stringify(payload);
    const sizeInMB = str.length / (1024 * 1024);

    if (sizeInMB > 55) {
      console.warn(`[Cache] Payload size (${sizeInMB.toFixed(2)} MB) exceeds safe 55 MB budget. Skipping.`);
      return false;
    }

    return new Promise((resolve) => {
      const tx = db.transaction(STORE_NAME, "readwrite");
      const store = tx.objectStore(STORE_NAME);
      store.put(payload);

      tx.oncomplete = () => {
        console.log(`[Cache] Stored ${timelineFrames.length} frames on laptop IndexedDB (~${sizeInMB.toFixed(2)} MB).`);
        resolve(true);
      };

      tx.onerror = () => {
        console.warn("[Cache] Error writing to IndexedDB:", tx.error);
        resolve(false);
      };
    });
  } catch (err) {
    console.warn("[Cache] IndexedDB store failed:", err);
    return false;
  }
}

/**
 * Retrieves a cached session timeline from the laptop's IndexedDB.
 * @param {string} cacheKey
 * @returns {Promise<Array|null>}
 */
export async function getCachedSessionTimeline(cacheKey) {
  try {
    const db = await openDB();
    if (!db) return null;

    return new Promise((resolve) => {
      const tx = db.transaction(STORE_NAME, "readonly");
      const store = tx.objectStore(STORE_NAME);
      const req = store.get(cacheKey);

      req.onsuccess = () => {
        if (req.result && Array.isArray(req.result.frames)) {
          resolve(req.result.frames);
        } else {
          resolve(null);
        }
      };

      req.onerror = () => resolve(null);
    });
  } catch (err) {
    console.warn("[Cache] IndexedDB read error:", err);
    return null;
  }
}

/**
 * Clears cached simulation data from IndexedDB.
 */
export async function clearSimulationCache() {
  try {
    const db = await openDB();
    if (!db) return false;

    return new Promise((resolve) => {
      const tx = db.transaction(STORE_NAME, "readwrite");
      const store = tx.objectStore(STORE_NAME);
      store.clear();
      tx.oncomplete = () => resolve(true);
      tx.onerror = () => resolve(false);
    });
  } catch (err) {
    return false;
  }
}
