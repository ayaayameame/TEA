import asyncio
import aiohttp
from datetime import datetime, timezone
import sys
from pathlib import Path

# Add project root to sys.path so we can import our modules
sys.path.append(str(Path(__file__).resolve().parent.parent.parent))

from src.config import setup_logger, DB_PATH
from src.database.connection import DatabaseManager

# Initialize logger
logger = setup_logger("TestRun", "test_fetch.log")

async def fetch_tarkov_dev(session: aiohttp.ClientSession, item_ids: list) -> list:
    """Asynchronously fetch data from Tarkov.dev using GraphQL."""
    url = "https://api.tarkov.dev/graphql"
    query = """
    query GetItems($ids: [ID!]) {
        items(ids: $ids) { id shortName lastLowPrice avg24hPrice }
    }
    """
    try:
        async with session.post(url, json={"query": query, "variables": {"ids": item_ids}}) as response:
            response.raise_for_status()
            data = await response.json()
            return data.get('data', {}).get('items', [])
    except Exception as e:
        logger.error(f"API request failed: {e}")
        return []

async def main():
    logger.info("🚀 Starting test data fetch...")
    
    # 1. Initialize and connect to the database
    db = DatabaseManager(DB_PATH)
    if not db.connect():
        return
        
    # Run setup.sql to create tables if they don't exist
    setup_sql_path = Path(__file__).resolve().parent.parent / "database" / "setup.sql"
    db.execute_script(str(setup_sql_path))
    
    # 2. Target item IDs: GPU, LEDX, Bitcoin
    target_ids = ["57347ca924597744596b4e71", "5c0530ee86f774697952d952", "59faff1d86f7746c51718c9c"]
    
    # 3. Fetch data concurrently
    async with aiohttp.ClientSession() as session:
        items = await fetch_tarkov_dev(session, target_ids)
    
    # 4. Insert data into the database
    if items:
        now_str = datetime.now(timezone.utc).isoformat()
        
        insert_query = """
            INSERT OR IGNORE INTO market_data_raw 
            (timestamp, item_id, item_name, source, lowest_price, avg_24h_price)
            VALUES (?, ?, ?, ?, ?, ?)
        """
        
        records = []
        for item in items:
            if item.get('lastLowPrice'):
                records.append((
                    now_str, 
                    item['id'], 
                    item['shortName'], 
                    'tarkov.dev', 
                    item['lastLowPrice'], 
                    item['avg24hPrice']
                ))
        
        try:
            cursor = db.connection.cursor()
            cursor.executemany(insert_query, records)
            db.connection.commit()
            logger.info(f"✅ Successfully inserted {cursor.rowcount} records into the database!")
            
            # Print to console for immediate feedback
            print("\n" + "="*40)
            for r in records:
                print(f"📦 Item: {r[2]:<10} | Lowest: {r[4]:>8} ₽ | 24h Avg: {r[5]:>8} ₽")
            print("="*40 + "\n")
                
        except Exception as e:
            logger.error(f"Failed to insert records: {e}")
            
    db.close()

if __name__ == "__main__":
    # Required for Windows compatibility with asyncio
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    asyncio.run(main())