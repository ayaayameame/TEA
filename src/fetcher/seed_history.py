import asyncio
import aiohttp
import sqlite3
import time
import sys
from datetime import datetime, timezone
from collections import defaultdict
from typing import List, Dict, Any, Tuple
from pathlib import Path

# ==========================================
# 1. Project & Core Setup
# ==========================================
PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
sys.path.append(str(PROJECT_ROOT))

DB_PATH = PROJECT_ROOT / "data" / "tarkov_economy.db"
API_URL = "https://api.tarkov.dev/graphql"

# GraphQL query requesting 'historicalPrices'
HISTORY_QUERY = """
{
  items {
    id name shortName basePrice width height
    sellFor { price vendor { normalizedName } }
    historicalPrices { price timestamp }
  }
}
"""

def extract_best_trader_price(sell_for_list: List[Dict]) -> int:
    """Finds the highest NPC trader payout."""
    if not sell_for_list: return 0
    prices = [s['price'] for s in sell_for_list if s.get('vendor') and s['vendor'].get('normalizedName') != 'flea-market']
    return max(prices) if prices else 0

async def fetch_history(session: aiohttp.ClientSession, mode: str) -> List[Dict[str, Any]]:
    """Fetches historical market data from GraphQL API."""
    print(f"[NETWORK] Fetching historical data for {mode.upper()}... (This may take 10-20 seconds)")
    headers = {"tarkov-game-mode": mode} if mode == "pve" else {}
    try:
        # Extended timeout because historical payload is massive
        async with session.post(API_URL, json={"query": HISTORY_QUERY}, headers=headers, timeout=60) as resp:
            data = await resp.json()
            return data.get('data', {}).get('items', [])
    except Exception as e:
        print(f"[ERROR] API Error ({mode}): {e}")
        return []

def process_historical_data(items: List[Dict], mode: str) -> Tuple[List[Tuple], List[Tuple]]:
    """
    Parses raw historical data and buckets them into 20m and 1h intervals.
    Returns: (records_20m, records_1h)
    """
    print(f"[PROCESS] Crunching numbers and grouping {mode.upper()} history...")
    now = datetime.now(timezone.utc)
    
    # Dictionaries to group prices by their exact timestamp bucket
    # Key: (timestamp_str, item_id) -> Value: List of prices
    buckets_20m = defaultdict(list)
    buckets_1h = defaultdict(list)
    
    # Item metadata lookup table to avoid repeating calculations
    metadata = {}

    for i in items:
        history = i.get('historicalPrices')
        if not history: 
            continue
            
        item_id = i['id']
        slots = (i.get('width') or 1) * (i.get('height') or 1)
        trader_price = extract_best_trader_price(i.get('sellFor', []))
        
        metadata[item_id] = {
            'name': i.get('name', 'Unknown'),
            'short': i.get('shortName', ''),
            'slots': slots,
            'base': i.get('basePrice', 0),
            'trader': trader_price
        }

        for point in history:
            price = point.get('price')
            ts_ms = point.get('timestamp')
            if not price or not ts_ms: continue
            
            # API returns timestamp in milliseconds string
            dt = datetime.fromtimestamp(int(ts_ms) / 1000, tz=timezone.utc)
            days_old = (now - dt).days
            
            # Skip data from the last 24 hours (handled by the real-time fetcher)
            if days_old <= 1:
                continue
            elif days_old <= 30:
                # Truncate to 20-minute boundary
                bucket_ts = datetime.fromtimestamp((int(dt.timestamp()) // 1200) * 1200, tz=timezone.utc).strftime('%Y-%m-%d %H:%M:%00')
                buckets_20m[(bucket_ts, item_id)].append(price)
            else:
                # Truncate to 1-hour boundary
                bucket_ts = dt.strftime('%Y-%m-%d %H:00:00')
                buckets_1h[(bucket_ts, item_id)].append(price)

    # Calculate Min/Max/Avg for each bucket
    records_20m = []
    for (ts, item_id), prices in buckets_20m.items():
        meta = metadata[item_id]
        records_20m.append((
            ts, item_id, meta['name'], meta['short'], mode, meta['slots'], 
            meta['base'], meta['trader'], min(prices), max(prices), int(sum(prices)/len(prices))
        ))

    records_1h = []
    for (ts, item_id), prices in buckets_1h.items():
        meta = metadata[item_id]
        records_1h.append((
            ts, item_id, meta['name'], meta['short'], mode, meta['slots'], 
            meta['base'], meta['trader'], min(prices), max(prices), int(sum(prices)/len(prices))
        ))
        
    return records_20m, records_1h

def inject_to_database(records_20m: List[Tuple], records_1h: List[Tuple]) -> None:
    """Inserts the processed historical records into the SQLite database."""
    print(f"[DATABASE] Injecting {len(records_20m)} rows into 20m table...")
    print(f"[DATABASE] Injecting {len(records_1h)} rows into 1h table...")
    
    if not DB_PATH.exists():
        print("[ERROR] Database not found. Please run the fetcher daemon first.")
        return

    try:
        # Use timeout=30.0 so we don't crash if the background Fetcher is currently writing
        conn = sqlite3.connect(DB_PATH, timeout=30.0)
        cursor = conn.cursor()
        
        # Insert 20m records
        cursor.executemany("""
            INSERT OR IGNORE INTO market_data_20m 
            (timestamp, item_id, item_name, short_name, game_mode, slots, 
             base_price, trader_price, min_price, max_price, avg_price)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, records_20m)
        
        # Insert 1h records
        cursor.executemany("""
            INSERT OR IGNORE INTO market_data_1h 
            (timestamp, item_id, item_name, short_name, game_mode, slots, 
             base_price, trader_price, min_price, max_price, avg_price)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, records_1h)

        conn.commit()
        conn.execute("PRAGMA wal_checkpoint(TRUNCATE);")
        conn.execute("VACUUM;")
        conn.close()
        print("[SUCCESS] Historical data injected and database optimized successfully!")

    except Exception as e:
        print(f"[ERROR] Database Injection Failed: {e}")

async def main() -> None:
    print("--- 🕰️ TARKOV MARKET TIME MACHINE (Historical Data Seeder) ---")
    start_time = time.perf_counter()

    async with aiohttp.ClientSession() as session:
        pvp_data, pve_data = await asyncio.gather(
            fetch_history(session, "regular"),
            fetch_history(session, "pve")
        )
        
    pvp_20m, pvp_1h = process_historical_data(pvp_data, "pvp")
    pve_20m, pve_1h = process_historical_data(pve_data, "pve")
    
    total_20m = pvp_20m + pve_20m
    total_1h = pvp_1h + pve_1h
    
    inject_to_database(total_20m, total_1h)
    
    elapsed = time.perf_counter() - start_time
    print(f"--- 🎉 DONE in {elapsed:.2f} seconds ---")

if __name__ == "__main__":
    asyncio.run(main())