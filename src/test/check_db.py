import sqlite3
from pathlib import Path
import sys

# ==========================================
# Configuration & Path Resolution
# ==========================================
PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
DB_PATH = PROJECT_ROOT / "data" / "tarkov_economy.db"

def run_diagnostics() -> None:
    """
    Runs a diagnostic check on the SQLite database, including sync cycle stats.
    """
    print("--- Tarkov Economy Database Diagnostic Tool ---")
    print(f"[INFO] Target DB resolved to: {DB_PATH}")

    if not DB_PATH.exists():
        print(f"[ERROR] Database file not found at: {DB_PATH}")
        print("[HINT] Ensure the Fetcher has run at least once to generate the DB.")
        sys.exit(1)

    try:
        conn = sqlite3.connect(DB_PATH, timeout=10.0)
        cursor = conn.cursor()

        # 1. Tiered Tables Capacity Check
        print("\n=== Tiered Tables Capacity ===")
        tables = ['market_data_5m', 'market_data_20m', 'market_data_1h']
        for table in tables:
            try:
                cursor.execute(f"SELECT COUNT(*) FROM {table}")
                count = cursor.fetchone()[0]
                print(f"[✓] {table:<17} : {count:>9,} records")
            except sqlite3.OperationalError:
                print(f"[!] {table:<17} : Table does not exist yet.")

        # 2. Fetch Cycle Statistics (NEW)
        print("\n=== Fetch Cycle Statistics ===")
        cursor.execute("SELECT COUNT(DISTINCT timestamp) FROM market_data_5m")
        total_cycles = cursor.fetchone()[0]
        print(f"[INFO] Total Successful Sync Cycles : {total_cycles}")

        if total_cycles > 0:
            print("\n[INFO] Top 5 Most Recent Syncs (Items per Cycle):")
            # Group by timestamp to count items per fetch cycle
            cursor.execute("""
                SELECT timestamp, COUNT(*) 
                FROM market_data_5m 
                GROUP BY timestamp 
                ORDER BY timestamp DESC 
                LIMIT 5
            """)
            for ts, count in cursor.fetchall():
                print(f"  -> [{ts}] Recorded {count:>5} items")

        # 3. Latest Data Integrity Check
        print("\n=== Top 5 Most Recent Item Pulses ===")
        cursor.execute("""
            SELECT timestamp, game_mode, item_name, lowest_price 
            FROM market_data_5m 
            ORDER BY timestamp DESC 
            LIMIT 5
        """)
        
        rows = cursor.fetchall()
        for row in rows:
            ts, mode, name, price = row
            short_name = (name[:25] + '..') if len(name) > 25 else name
            print(f"[{ts}] Mode: {mode.upper():<3} | Item: {short_name:<27} | Price: {price:>9,} RUB")

        conn.close()
        print("\n[SUCCESS] Diagnostics completed successfully. Database is healthy.")

    except sqlite3.OperationalError as e:
        print(f"\n[ERROR] SQLite Operational Error: {e}")
        print("[HINT] The database might be locked by an active write process. Try again.")
    except Exception as e:
        print(f"\n[ERROR] An unexpected error occurred: {e}")

if __name__ == "__main__":
    run_diagnostics()