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

# Load environment variables from .env file
load_dotenv()

SUPABASE_URL = os.environ.get("SUPABASE_URL")
SUPABASE_KEY = os.environ.get("SUPABASE_KEY")

if not SUPABASE_URL or not SUPABASE_KEY:
    raise ValueError("❌ Missing Supabase credentials. Check your .env or GitHub Secrets.")

# Initialize the Supabase client
supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

# ==============================================================================
# 2. Helper Functions
# ==============================================================================

def parse_tarkov_time(iso_str):
    """Converts Tarkov API ISO8601 string to a Unix Timestamp."""
    if not iso_str: 
        return 0
    try:
        return int(datetime.fromisoformat(iso_str.replace('Z', '+00:00')).timestamp())
    except Exception:
        return 0

def fetch_tarkov_data(game_mode="regular"):
    """Fetches a full market snapshot from the Tarkov.dev GraphQL API."""
    mode_label = "PvE" if game_mode == "pve" else "PvP"
    print(f"⏳ Fetching raw {mode_label} data from Tarkov API...")
    
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
            changeLast48hPercent
            fleaMarketFee
            lastOfferCount
        }
    }
    """ % game_mode
    
    url = 'https://api.tarkov.dev/graphql'
    headers = {
        'Content-Type': 'application/json',
        'User-Agent': 'TarkovDataSync/3.0'
    }
    
    req = urllib.request.Request(
        url, 
        data=json.dumps({'query': query}).encode('utf-8'), 
        headers=headers
    )
    
    with urllib.request.urlopen(req) as response:
        result = json.loads(response.read().decode('utf-8'))
        return result['data']['items']

# ==============================================================================
# 3. Item Mapping Logic (Bypassing the 1000-row limit)
# ==============================================================================

def robust_mapping_sync(raw_items):
    """
    Synchronizes API items with the database. 
    Uses pagination to bypass the default 1000-row return limit of Supabase.
    """
    print("🛡️ Syncing item mapping (Upsert Logic)...")
    
    # Deduplicate by name locally to ensure a clean payload
    payload_dict = {
        item['name']: {
            "original_id": item['id'],
            "name": item['name'],
            "short_name": item.get('shortName', 'Unknown')
        } for item in raw_items if item.get('name')
    }
    
    # Bulk Upsert: Update ID if Name exists, otherwise Insert
    supabase.table("items").upsert(list(payload_dict.values()), on_conflict="name").execute()
    
    # --- FIX: PAGINATION LOOP ---
    # We must fetch in increments of 1000 to get the full list of 4000+ items
    all_mapping_rows = []
    page_size = 1000
    current_page = 0
    
    print("🔍 Fetching full mapping from database...")
    while True:
        start = current_page * page_size
        end = start + page_size - 1
        
        # Use .range() to request specific blocks of data
        res = supabase.table("items").select("item_numeric_id, original_id").range(start, end).execute()
        
        if not res.data:
            break
            
        all_mapping_rows.extend(res.data)
        
        # If we received fewer rows than the page size, we've reached the end
        if len(res.data) < page_size:
            break
        current_page += 1
    
    final_mapping = {row['original_id']: row['item_numeric_id'] for row in all_mapping_rows}
    print(f"✅ Mapping fully refreshed: {len(final_mapping)} items mapped.")
    return final_mapping

# ==============================================================================
# 4. Price Upload Logic (Bulk Mode)
# ==============================================================================

def push_prices_to_db(raw_items, id_map, is_pve=False):
    """Processes and pushes all price records in one single database request."""
    mode_label = "PvE" if is_pve else "PvP"
    
    # Heartbeat check to prevent duplicate API snapshot uploads
    res = supabase.table("prices_2d").select("ts_api")\
        .eq("is_pve", is_pve)\
        .limit(1).order("ts_api", desc=True).execute()
    
    latest_db_ts = res.data[0]['ts_api'] if res.data else 0
    api_timestamps = [parse_tarkov_time(item.get('updated')) for item in raw_items]
    max_api_ts = max(api_timestamps) if api_timestamps else 0

    if max_api_ts <= latest_db_ts:
        print(f"⏩ {mode_label} data is already current. Skipping upload.")
        return

    print(f"🚀 Preparing bulk upload for {mode_label}...")
    ts_now = int(time.time())
    
    # Transform raw items into database rows
    upload_payload = [
        {
            "ts_fetch": ts_now,
            "ts_api": parse_tarkov_time(item.get('updated')) or max_api_ts,
            "p_avg": int(item.get('avg24hPrice') or 0),
            "p_min": int(item.get('lastLowPrice') or 0),
            "p_low": int(item.get('low24hPrice') or 0),
            "p_high": int(item.get('high24hPrice') or 0),
            "p_fee": int(item.get('fleaMarketFee') or 0),
            "item_ref": id_map[item['id']],
            "p_count": min(32767, int(item.get('lastOfferCount') or 0)),
            "p_change_s": int((item.get('changeLast48hPercent') or 0) * 100),
            "is_pve": is_pve 
        } for item in raw_items if item['id'] in id_map
    ]

    # PERFORM FULL BULK UPLOAD (No Chunking)
    if upload_payload:
        supabase.table("prices_2d").insert(upload_payload).execute()
        print(f"✅ Successfully pushed {len(upload_payload)} {mode_label} records to cloud.")

# ==============================================================================
# 5. Main Execution
# ==============================================================================
if __name__ == "__main__":
    try:
        # Step 1: Sync all items and build full ID mapping
        pvp_data = fetch_tarkov_data(game_mode="regular")
        id_mapping = robust_mapping_sync(pvp_data)
        
        # Step 2: Upload PvP Prices (Bulk)
        push_prices_to_db(pvp_data, id_mapping, is_pve=False)
        
        # Step 3: Upload PvE Prices (Bulk)
        pve_data = fetch_tarkov_data(game_mode="pve")
        push_prices_to_db(pve_data, id_mapping, is_pve=True)
        
        print("🎉 Sync sequence completely finished!")
    except Exception as e:
        print(f"❌ Critical Error: {e}")