import sqlite3
import shutil
import os
import tempfile
from pathlib import Path
import sys

# ==========================================
# 1. Project Path Configuration
# ==========================================
SCRIPT_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = SCRIPT_DIR.parent.parent
sys.path.append(str(PROJECT_ROOT))

# Import centralized DB path from config
from src.config import DB_PATH

def run_direct_db_explorer():
    """
    Diagnostic tool to inspect the latest raw data 
    synced from the Idle PC to this 7950X.
    """
    print("=" * 65)
    print("TARKOV 7950X - DIRECT DATABASE EXPLORER (ENG)")
    print("=" * 65)
    
    if not DB_PATH.exists():
        print(f"STATUS: [FATAL] Database not found at {DB_PATH}")
        return

    # ==========================================
    # 2. Local Buffer Setup (Shadow Copy)
    # ==========================================
    # We copy files to TEMP to avoid OneDrive file-locking issues.
    temp_dir = Path(tempfile.gettempdir())
    local_db = temp_dir / "tarkov_debug.db"
    local_wal = temp_dir / "tarkov_debug.db-wal"
    wal_source = DB_PATH.with_name(DB_PATH.name + "-wal")

    try:
        # Step A: Physical file copy
        shutil.copy2(DB_PATH, local_db)
        if wal_source.exists():
            shutil.copy2(wal_source, local_wal)
            print(">>> INFO: WAL file detected and synced to local buffer.")
        
        # Step B: Establish Read-Only Connection
        # mode=ro & nolock=1 are safeguards for concurrent access
        conn = sqlite3.connect(local_db)
        conn.execute("PRAGMA journal_mode=WAL;")
        cursor = conn.cursor()

        print("-" * 65)
        # --- PART 1: Global Database Statistics ---
        cursor.execute("SELECT COUNT(*), MAX(timestamp) FROM market_data_5m")
        total_rows, latest_ts = cursor.fetchone()
        print(f"Total Records in DB : {total_rows}")
        print(f"Latest Sync Time    : {latest_ts}")

        if total_rows > 0:
            print("-" * 65)
            print(f"PART 2: LATEST MARKET SNAPSHOT (Top 20 Items)")
            print(f"{'ITEM_ID':<32} | {'PRICE':<12} | {'AVG_24H':<12}")
            print("-" * 65)

            # --- PART 2: Fetch the latest batch of data ---
            # We query all items that share the absolute latest timestamp.
            sql_all = """
                SELECT item_id, lowest_price, avg_24h_price 
                FROM market_data_5m 
                WHERE timestamp = (SELECT MAX(timestamp) FROM market_data_5m)
                LIMIT 20
            """
            cursor.execute(sql_all)
            rows = cursor.fetchall()

            for rid, price, avg in rows:
                # Formatting: Left-aligned ID, comma-separated currency
                print(f"{rid:<32} | {price:<12,} | {avg:<12,}")
            
            if len(rows) < 10:
                print("\n>>> WARNING: Low item count in latest batch.")
                print(">>> Your Fetcher might still be running in restricted mode.")
        else:
            print("STATUS: [EMPTY] No data found in 'market_data_5m' table.")

        conn.close()
    except Exception as e:
        print(f"STATUS: [FAILED] Error during execution: {e}")
    finally:
        # Step C: Clean up shadow files from TEMP folder
        if local_db.exists(): os.remove(local_db)
        if local_wal.exists(): os.remove(local_wal)
    
    print("=" * 65)

if __name__ == "__main__":
    run_direct_db_explorer()