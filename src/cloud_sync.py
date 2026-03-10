import os
import json
import time
import urllib.request
from datetime import datetime
from dotenv import load_dotenv
from supabase import create_client, Client

# ==============================================================================
# 1. Configuration & Initialization
# ==============================================================================

# Load environment variables from .env file (for local development)
load_dotenv()

SUPABASE_URL = os.environ.get("SUPABASE_URL")
SUPABASE_KEY = os.environ.get("SUPABASE_KEY")

if not SUPABASE_URL or not SUPABASE_KEY:
    raise ValueError("❌ Missing Supabase credentials. Please check your GitHub Secrets or .env file.")

# Initialize the Supabase client
supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

# ==============================================================================
# 2. Helper Functions
# ==============================================================================

def parse_tarkov_time(iso_str):
    """Converts a Tarkov API ISO8601 string to a Unix Timestamp."""
    if not iso_str: 
        return 0
    try:
        # Replace 'Z' with '+00:00' to ensure compatibility with Python's fromisoformat
        return int(datetime.fromisoformat(iso_str.replace('Z', '+00:00')).timestamp())
    except Exception:
        return 0

def fetch_tarkov_data(game_mode="regular"):
    """Fetches a full market snapshot from the Tarkov.dev GraphQL API."""
    mode_label = "PvE" if game_mode == "pve" else "PvP"
    print(f"⏳ Fetching raw {mode_label} data from Tarkov API...")
    
    # GraphQL query: Removed redundant fields (fleaMarketFee, changeLast48hPercent)
    # to reduce network payload size and improve speed.
    query = """
    {
        items(gameMode: %s) {
            id
            name
            shortName
            updated
            avg24hPrice
            lastLowPrice
            low24hPrice
            high24hPrice
            lastOfferCount
        }
    }
    """ % game_mode
    
    url = 'https://api.tarkov.dev/graphql'
    headers = {
        'Content-Type': 'application/json', 
        'User-Agent': 'TarkovDataSync/8.0'
    }
    
    req = urllib.request.Request(url, data=json.dumps({'query': query}).encode('utf-8'), headers=headers)
    
    with urllib.request.urlopen(req) as response:
        result = json.loads(response.read().decode('utf-8'))
        return result['data']['items']

# ==============================================================================
# 3. Item Mapping Logic
# ==============================================================================

def robust_mapping_sync(raw_items):
    """Syncs items via Upsert and fetches all mapping rows using pagination."""
    print("🛡️  Syncing item mapping (Upsert Logic)...")
    
    # Prepare payload for the 'items' table
    payload_dict = {
        item['name']: {
            "original_id": item['id'],
            "name": item['name'],
            "short_name": item.get('shortName', 'Unknown')
        } for item in raw_items if item.get('name')
    }
    
    # Upsert items to ensure new items are added without duplicating existing ones
    supabase.table("items").upsert(list(payload_dict.values()), on_conflict="name").execute()
    
    all_mapping_rows = []
    page_size, current_page = 1000, 0
    
    print("🔍 Fetching full mapping from database...")
    # Paginate through the items table to bypass the 1000-row limit
    while True:
        start = current_page * page_size
        end = (current_page + 1) * page_size - 1
        res = supabase.table("items").select("item_numeric_id, original_id").range(start, end).execute()
        
        if not res.data: 
            break
            
        all_mapping_rows.extend(res.data)
        
        if len(res.data) < page_size: 
            break
        current_page += 1
    
    # Create a dictionary mapping the string ID to our space-saving integer ID
    final_mapping = {row['original_id']: row['item_numeric_id'] for row in all_mapping_rows}
    print(f"✅ Mapping refreshed: {len(final_mapping)} items ready.")
    return final_mapping

# ==============================================================================
# 4. Storage Logic (Raw Data Ingestion)
# ==============================================================================

def push_market_data(raw_items, id_map, is_pve=False):
    """Pushes raw snapshots to the prices_2d table for further DB-side settlement."""
    mode_label = "PvE" if is_pve else "PvP"
    
    # Extract timestamps to determine the actual update time of this batch
    api_timestamps = [parse_tarkov_time(item.get('updated')) for item in raw_items]
    max_api_ts = max(api_timestamps) if api_timestamps else 0

    # Fetch the latest timestamp currently stored in the database for this game mode
    res = supabase.table("prices_2d").select("ts_fetch").eq("is_pve", is_pve).limit(1).order("ts_fetch", desc=True).execute()
    latest_db_ts = res.data[0]['ts_fetch'] if res.data else 0

    # If the API hasn't updated since our last fetch, skip the database write entirely
    if max_api_ts <= latest_db_ts:
        print(f"⏩ {mode_label} is up-to-date. Skipping database write.")
        return
    
    # Prepare the highly optimized payload for prices_2d
    upload_payload = [
        {
            "ts_fetch": max_api_ts,  # Using the max API timestamp as the primary key time
            "p_avg": int(item.get('avg24hPrice') or 0),
            "p_min": int(item.get('lastLowPrice') or 0),
            "p_low": int(item.get('low24hPrice') or 0),
            "p_high": int(item.get('high24hPrice') or 0),
            "item_ref": id_map[item['id']],
            "p_count": min(32767, int(item.get('lastOfferCount') or 0)),
            "is_pve": is_pve 
        } for item in raw_items if item['id'] in id_map
    ]

    # Batch insert into Supabase
    if upload_payload:
        supabase.table("prices_2d").insert(upload_payload).execute()
        print(f"✅ {mode_label} raw snapshot successfully stored in prices_2d.")

# ==============================================================================
# 5. Settlement Logic (The DB-Side "Brain")
# ==============================================================================

def run_settlement():
    """Triggers DB-side averaging and auto-cleanup with execution logging."""
    start_time = time.time()
    print("\n" + "="*50)
    print(f"🚀 [SETTLEMENT] Triggered at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    try:
        # Call the PostgreSQL RPC function
        supabase.rpc("settle_tarkov_data", {}).execute()
        
        duration = time.time() - start_time
        print(f"✅ [SETTLEMENT] Success! DB-side aggregation & cleanup finished.")
        print(f"⏱️  [SETTLEMENT] Execution time: {duration:.2f} seconds.")
        print("    Pipeline: 2d -> 7d (Hourly) -> 365d (Daily) | Garbage collection complete.")
    except Exception as e:
        print(f"❌ [SETTLEMENT] Failed: {str(e)}")
    
    print("="*50 + "\n")

# ==============================================================================
# 6. Main Orchestration
# ==============================================================================

if __name__ == "__main__":
    try:
        # Step 1: PvP Sync
        pvp_data = fetch_tarkov_data(game_mode="regular")
        id_mapping = robust_mapping_sync(pvp_data)
        push_market_data(pvp_data, id_mapping, is_pve=False)
        
        # Step 2: PvE Sync
        pve_data = fetch_tarkov_data(game_mode="pve")
        push_market_data(pve_data, id_mapping, is_pve=True)
        
        # Step 3: Global Settlement & Optimization (Hourly rate-limiting)
        current_minute = datetime.now().minute
        
        # Rate Limiter: Only run the heavy SQL settlement at the top of the hour (Minutes 00 to 04).
        # Assuming a 5-minute cron job, this triggers exactly once per hour.
        if current_minute < 5:
            print(f"⏰ Top of the hour (Minute {current_minute}): Executing heavy database settlement...")
            run_settlement()
        else:
            print(f"⏩ Skipping settlement (Minute {current_minute}): Conserving Supabase Disk IO Budget...")
        
        print("🎉 Entire sync sequence finished successfully!")
    except Exception as e:
        print(f"❌ Critical Error in sync engine: {e}")
