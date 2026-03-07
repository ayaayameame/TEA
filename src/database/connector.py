"""
Module: connector.py
Description: Optimized SQLite connector for TEA. 
             Provides Local Datetime objects for calendar-based filtering.
"""
import sqlite3
import pandas as pd
import numpy as np
import datetime
from pathlib import Path

class TarkovDatabase:
    def __init__(self, db_path: str):
        self.db_path = db_path

    def get_price_history(self, item_id: str, timeframe: str, game_mode: str = "pvp"):
        if not Path(self.db_path).exists():
            return pd.DataFrame()

        table_map = {
            "24 Hours": "market_data_5m",
            "30 Days": "market_data_20m",
            "1 Year": "market_data_1h"
        }
        table_name = table_map.get(timeframe, "market_data_5m")
        
        query = f"""
        SELECT timestamp, lowest_price, avg_24h_price as avg_price 
        FROM {table_name} 
        WHERE item_id = ? AND game_mode = ?
        ORDER BY timestamp ASC
        """
        
        try:
            with sqlite3.connect(self.db_path, timeout=30.0) as conn:
                df = pd.read_sql_query(query, conn, params=(item_id, game_mode.lower()))
                if df.empty:
                    return df
                
                # Convert UTC strings to Local Datetime
                df['timestamp'] = pd.to_datetime(df['timestamp'])
                local_tz = datetime.datetime.now().astimezone().tzinfo
                df['dt_local'] = df['timestamp'].dt.tz_convert(local_tz)
                
                # Convert to Unix Seconds for math
                df['unix_ts'] = df['dt_local'].apply(lambda x: x.timestamp())
                
                return df
        except Exception as e:
            print(f"[DB Error] {e}")
            return pd.DataFrame()