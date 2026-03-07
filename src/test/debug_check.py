import sqlite3
from pathlib import Path

# Path to your real database
db_path = Path(__file__).parent.parent.parent / "data" / "tarkov_economy.db"

def check_raw_data():
    if not db_path.exists():
        print(f"[-] Database not found at: {db_path}")
        return

    try:
        conn = sqlite3.connect(str(db_path))
        cursor = conn.cursor()
        
        # 1. Check table names
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
        tables = cursor.fetchall()
        print(f"[*] Tables in DB: {tables}")

        # 2. Check the content of market_data_5m
        print("\n[*] First 3 rows of 'market_data_5m':")
        cursor.execute("SELECT * FROM market_data_5m LIMIT 3")
        rows = cursor.fetchall()
        
        # Get column names
        colnames = [description[0] for description in cursor.description]
        print(f"Columns: {colnames}")
        
        for row in rows:
            print(row)
            
        conn.close()
    except Exception as e:
        print(f"[-] Error: {e}")

if __name__ == "__main__":
    check_raw_data()