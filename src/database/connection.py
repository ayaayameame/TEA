import sqlite3
import logging
from typing import Optional

logger = logging.getLogger("DatabaseManager")

class DatabaseManager:
    """
    High-performance SQLite database manager optimized for OneDrive sync.
    Uses WAL mode and extended timeout to handle file locking.
    """
    def __init__(self, db_path: str):
        self.db_path = str(db_path)
        self.connection: Optional[sqlite3.Connection] = None

    def connect(self) -> bool:
        """Establish connection with 30s timeout and WAL mode."""
        try:
            # [FIXED] Added 30s timeout to survive OneDrive file locks
            self.connection = sqlite3.connect(self.db_path, timeout=30.0)
            self.connection.row_factory = sqlite3.Row 
            
            # Enable WAL mode for concurrent read/write
            self.connection.execute("PRAGMA journal_mode=WAL;")
            # Optimize synchronization for solid state drives/cloud sync
            self.connection.execute("PRAGMA synchronous=NORMAL;") 
            # Allow the database to wait for locks
            self.connection.execute("PRAGMA busy_timeout=30000;")
            
            logger.info("✅ SQLite connected (WAL + 30s Timeout).")
            return True
        except sqlite3.Error as e:
            logger.error(f"❌ SQLite connection failed: {e}")
            return False

    def initialize_schema(self) -> None:
        """Initializes the Tiered Storage Schema (2d / 30d / 365d)."""
        if not self.connection: return
        
        schema_sql = """
            -- T1: 5-min precision for 2 days
            CREATE TABLE IF NOT EXISTS market_2d (
                timestamp TEXT NOT NULL, item_id TEXT NOT NULL, item_name TEXT, 
                short_name TEXT, game_mode TEXT NOT NULL, slots INTEGER, 
                base_price INTEGER, trader_price INTEGER, lowest_price INTEGER NOT NULL, 
                avg_24h_price INTEGER, UNIQUE (timestamp, item_id, game_mode)
            );
            -- T2: 1-hour precision for 30 days
            CREATE TABLE IF NOT EXISTS market_30d (
                timestamp TEXT NOT NULL, item_id TEXT NOT NULL, item_name TEXT, 
                short_name TEXT, game_mode TEXT NOT NULL, slots INTEGER, 
                base_price INTEGER, trader_price INTEGER, min_price INTEGER, 
                max_price INTEGER, avg_price INTEGER, UNIQUE (timestamp, item_id, game_mode)
            );
            -- T3: 1-day precision for 365 days
            CREATE TABLE IF NOT EXISTS market_365d (
                timestamp TEXT NOT NULL, item_id TEXT NOT NULL, item_name TEXT, 
                short_name TEXT, game_mode TEXT NOT NULL, slots INTEGER, 
                base_price INTEGER, trader_price INTEGER, min_price INTEGER, 
                max_price INTEGER, avg_price INTEGER, UNIQUE (timestamp, item_id, game_mode)
            );
        """
        try:
            self.connection.executescript(schema_sql)
            self.connection.commit()
            logger.info("✅ Database schema initialized.")
        except Exception as e:
            logger.error(f"❌ Failed to init schema: {e}")

    def close(self):
        """Safely close the connection."""
        if self.connection:
            self.connection.close()
            logger.info("🔒 Database connection closed.")