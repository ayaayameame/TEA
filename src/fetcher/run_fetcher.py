import asyncio
import aiohttp
import time
import sys
from datetime import datetime, timezone
from typing import List, Dict, Any, Iterator, Tuple
from pathlib import Path

# ==========================================
# 1. Project & Core Setup
# ==========================================
PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
sys.path.append(str(PROJECT_ROOT))

from src.config import setup_logger, DB_PATH
from src.database.connection import DatabaseManager

logger = setup_logger("FetcherDaemon", "daemon.log")

# ==========================================
# 2. Constants & Queries
# ==========================================
FETCH_INTERVAL = 300  # 5 minutes
API_URL = "https://api.tarkov.dev/graphql"

MARKET_QUERY = """
{
  items {
    id name shortName basePrice width height lastLowPrice avg24hPrice
    sellFor { price vendor { normalizedName } }
  }
}
"""

# ==========================================
# 3. Database Operations
# ==========================================
def save_records(db: DatabaseManager, records: List[Tuple]) -> None:
    """Inserts processed records into the 2-day high-freq table."""
    if not records: return
    try:
        cursor = db.connection.cursor()
        cursor.executemany("""
            INSERT OR IGNORE INTO market_2d 
            (timestamp, item_id, item_name, short_name, game_mode, slots, 
             base_price, trader_price, lowest_price, avg_24h_price)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, records)
        db.connection.commit()
        db.connection.execute("PRAGMA wal_checkpoint(TRUNCATE);")
    except Exception as e:
        logger.error(f"Database Write Error: {e}")

async def run_maintenance(db: DatabaseManager) -> None:
    """
    Tiered Downsampling Strategy:
    5m -> 1h (Stored in market_30d)
    1h -> 1d (Stored in market_365d)
    """
    logger.info("Maintenance: Compressing data for tiered storage...")
    try:
        cursor = db.connection.cursor()
        
        # Step 1: Compress 5m data to 1h for the 30-day table
        cursor.execute("""
            INSERT OR IGNORE INTO market_30d
            SELECT strftime('%Y-%m-%dT%H:00:00+00:00', timestamp), item_id, item_name, short_name, 
                   game_mode, slots, base_price, trader_price, MIN(lowest_price), MAX(lowest_price), AVG(lowest_price)
            FROM market_2d 
            WHERE timestamp < datetime('now', '-1 hour')
            GROUP BY strftime('%Y-%m-%d %H', timestamp), item_id, game_mode
        """)

        # Step 2: Compress 1h data to 1d for the 365-day table
        cursor.execute("""
            INSERT OR IGNORE INTO market_365d
            SELECT strftime('%Y-%m-%dT00:00:00+00:00', timestamp), item_id, item_name, short_name, 
                   game_mode, slots, base_price, trader_price, MIN(min_price), MAX(max_price), AVG(avg_price)
            FROM market_30d 
            WHERE timestamp < datetime('now', '-1 day')
            GROUP BY strftime('%Y-%m-%d', timestamp), item_id, game_mode
        """)

        # Step 3: Evict expired data
        cursor.execute("DELETE FROM market_2d WHERE timestamp < datetime('now', '-2 days')")
        cursor.execute("DELETE FROM market_30d WHERE timestamp < datetime('now', '-30 days')")
        cursor.execute("DELETE FROM market_365d WHERE timestamp < datetime('now', '-365 days')")
        
        db.connection.commit()
        db.connection.execute("PRAGMA wal_checkpoint(TRUNCATE);")
        db.connection.execute("VACUUM;")
        logger.info("Maintenance: Tiered compression and cleanup complete.")
    except Exception as e:
        logger.error(f"Maintenance Failed: {e}")

# ==========================================
# 4. Parsing & Fetching
# ==========================================
async def fetch_api(session: aiohttp.ClientSession, mode: str) -> List[Dict[str, Any]]:
    headers = {"tarkov-game-mode": mode} if mode == "pve" else {}
    try:
        async with session.post(API_URL, json={"query": MARKET_QUERY}, headers=headers, timeout=30) as resp:
            data = await resp.json()
            return data.get('data', {}).get('items', [])
    except Exception as e:
        logger.error(f"API Error ({mode}): {e}")
        return []

def parse_items(items: List[Dict], mode: str, timestamp: str) -> Iterator[Tuple]:
    for i in items:
        price = i.get('lastLowPrice')
        if not price or price <= 0: continue
        slots = (i.get('width') or 1) * (i.get('height') or 1)
        trader_prices = [
            s['price'] for s in i.get('sellFor', []) 
            if s.get('vendor') and s['vendor'].get('normalizedName') != 'flea-market'
        ]
        best_trader = max(trader_prices) if trader_prices else 0
        yield (timestamp, i['id'], i.get('name', 'Unknown'), i.get('shortName', ''), 
               mode, slots, i.get('basePrice', 0), best_trader, price, i.get('avg24hPrice'))

# ==========================================
# 5. Execution
# ==========================================
async def main() -> None:
    logger.info("--- TIERED FETCHER STARTED (2d/30d/365d) ---")
    db = DatabaseManager(DB_PATH)
    if not db.connect(): return
    db.initialize_schema()
    
    last_maint_hour = -1
    async with aiohttp.ClientSession() as session:
        while True:
            now = time.time()
            sleep_time = FETCH_INTERVAL - (now % FETCH_INTERVAL)
            next_run = datetime.fromtimestamp(now + sleep_time, tz=timezone.utc)
            logger.info(f"Idle: Waiting {sleep_time:.2f}s -> Next sync: {next_run.strftime('%H:%M:00')} UTC")
            await asyncio.sleep(sleep_time)
            
            start_t = time.perf_counter()
            ts = datetime.now(timezone.utc).isoformat()
            
            pvp_items, pve_items = await asyncio.gather(
                fetch_api(session, "regular"), fetch_api(session, "pve")
            )
            
            pvp_recs = list(parse_items(pvp_items, 'pvp', ts))
            pve_recs = list(parse_items(pve_items, 'pve', ts))
            save_records(db, pvp_recs + pve_recs)
            
            logger.info(f"SUCCESS: Saved {len(pvp_recs)+len(pve_recs)} items (PvP:{len(pvp_recs)} | PvE:{len(pve_recs)})")

            # Trigger maintenance once per hour
            current_hour = datetime.now().hour
            if current_hour != last_maint_hour:
                await run_maintenance(db)
                last_maint_hour = current_hour

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Stopped.")