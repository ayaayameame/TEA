-- 1. Table for HIGH PRECISION data (5 min interval, kept for ~24 hours)
CREATE TABLE IF NOT EXISTS market_data_5m (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    timestamp TEXT NOT NULL,
    item_id TEXT NOT NULL,
    item_name TEXT,
    source TEXT NOT NULL,
    lowest_price INTEGER NOT NULL,
    avg_24h_price INTEGER,
    UNIQUE (timestamp, item_id, source)
);

-- 2. Table for MEDIUM PRECISION data (20 min interval, kept for ~30 days)
CREATE TABLE IF NOT EXISTS market_data_20m (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    timestamp TEXT NOT NULL,
    item_id TEXT NOT NULL,
    source TEXT NOT NULL,
    min_price INTEGER,
    max_price INTEGER,
    avg_price INTEGER,
    UNIQUE (timestamp, item_id, source)
);

-- 3. Table for LONG TERM data (Daily precision, kept for 1 year)
CREATE TABLE IF NOT EXISTS market_data_daily (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    timestamp TEXT NOT NULL, -- Date only: YYYY-MM-DD
    item_id TEXT NOT NULL,
    source TEXT NOT NULL,
    min_price INTEGER,
    max_price INTEGER,
    avg_price INTEGER,
    UNIQUE (timestamp, item_id, source)
);

-- Create indexes for all tables to ensure 7950X-level query performance
CREATE INDEX IF NOT EXISTS idx_5m_ts ON market_data_5m (timestamp DESC);
CREATE INDEX IF NOT EXISTS idx_20m_ts ON market_data_20m (timestamp DESC);
CREATE INDEX IF NOT EXISTS idx_daily_ts ON market_data_daily (timestamp DESC);