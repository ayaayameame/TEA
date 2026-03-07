import os
import json
import time
import urllib.request
from datetime import datetime
from dotenv import load_dotenv
from supabase import create_client, Client

# ==============================================================================
# Configuration & Initialization
# ==============================================================================

# Load environment variables from .env file
load_dotenv()

SUPABASE_URL = os.environ.get("SUPABASE_URL")
SUPABASE_KEY = os.environ.get("SUPABASE_KEY")

if not SUPABASE_URL or not SUPABASE_KEY:
    raise ValueError("❌ Missing Supabase credentials. Please check your GitHub Secrets or .env file.")

# Initialize the Supabase client
supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

# ==============================================================================
# Helper Functions
# ==============================================================================

def parse_tarkov_time(iso_str):
    """Converts Tarkov API ISO8601 string to a Unix Timestamp."""
    if not iso_str: 
        return 0
    try:
        # Standardize 'Z' to '+00:00' for Python's fromisoformat
        return int(datetime.fromisoformat(iso_str.replace('Z', '+00:00')).timestamp())
    except Exception:
        return 0

def fetch_tarkov_data(game_mode="regular"):
    """
    Fetches market data from Tarkov.dev API.
    game_mode: "regular" for PvP, "pve" for PvE mode.
    """
    mode_label = "PvE" if game_mode == "pve" else "PvP"
    print(f"⏳ Fetching raw {mode_label} data from Tarkov API...")
    
    # GraphQL query targeting market data
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
        'User-Agent': 'TarkovDataSync/1.0'
    }
    
    # Execute request using Python's standard library
    req = urllib.request.Request(
        url, 
        data=json.dumps({'query': query}).encode('utf-8'), 
        headers=headers
    )
    
    with urllib.request.urlopen(req) as response:
        result = json.loads(response.read().decode('utf-8'))
        return result['data']['items']

def robust_mapping_sync(raw_items):
    """
    Final ultimate version: Uses database-level UPSERT on 'name' to handle 
    ID swaps and new items simultaneously, preventing all 23505 errors.
    """
    print("🛡️ Running Database-Level Upsert for item mapping...")
    
    # 1. Prepare unique payload for the 'items' table
    # We use a dictionary keyed by 'name' to ensure local uniqueness before sending to DB
    item_payload_dict = {}
    for item in raw_items:
        name = item.get('name', 'Unknown')
        item_payload_dict[name] = {
            "original_id": item['id'],
            "name": name,
            "short_name": item.get('shortName', 'Unknown')
        }
    
    payload = list(item_payload_dict.values())
    print(f"📦 Prepared {len(payload)} unique items for synchronization.")

    # 2. Execute UPSERT on the 'items' table
    # We tell Supabase: "If 'name' conflicts, just update the 'original_id' and 'short_name'"
    # This automatically handles new items AND ID swaps in one go.
    chunk_size = 1000
    for i in range(0, len(payload), chunk_size):
        chunk = payload[i:i + chunk_size]
        supabase.table("items").upsert(
            chunk, 
            on_conflict="name"  # <--- Crucial: Use 'name' as the unique anchor
        ).execute()
        print(f"   -> Item mapping chunk {i//chunk_size + 1} synced.")

    # 3. Build the final mapping for the price upload phase
    # We fetch back the database-assigned numeric IDs
    res = supabase.table("items").select("item_numeric_id, original_id").execute()
    final_mapping = {row['original_id']: row['item_numeric_id'] for row in res.data}
    
    print(f"✅ Mapping refreshed: {len(final_mapping)} items ready.")
    return final_mapping

def push_prices_to_db(raw_items, id_map, is_pve=False):
    """
    Formats and uploads price snapshots. 
    Skips upload if the latest API data is already present in the DB.
    """
    mode_label = "PvE" if is_pve else "PvP"
    print(f"🔍 Checking {mode_label} data status in database...")
    
    # Check the latest recorded API timestamp for the selected mode
    res = supabase.table("prices_2d").select("ts_api")\
        .eq("is_pve", is_pve)\
        .limit(1).order("ts_api", desc=True).execute()
    
    latest_db_ts = res.data[0]['ts_api'] if res.data else 0
    api_timestamps = [parse_tarkov_time(item.get('updated')) for item in raw_items]
    max_api_ts = max(api_timestamps) if api_timestamps else 0

    # Avoid redundant writes if API hasn't pushed an update
    if max_api_ts <= latest_db_ts:
        print(f"⏩ SKIP {mode_label}: Current data (TS: {max_api_ts}) is already stored.")
        return

    print(f"🚀 {mode_label} data is fresh! Preparing upload payload...")
    ts_now = int(time.time())
    upload_data = []
    
    for item in raw_items:
        api_id = item['id']
        if api_id not in id_map: 
            continue
        
        item_ts_api = parse_tarkov_time(item.get('updated'))
        if item_ts_api == 0: 
            item_ts_api = max_api_ts
            
        upload_data.append({
            "ts_fetch": ts_now,
            "ts_api": item_ts_api,
            "p_avg": int(item.get('avg24hPrice') or 0),
            "p_min": int(item.get('lastLowPrice') or 0),
            "p_low": int(item.get('low24hPrice') or 0),
            "p_high": int(item.get('high24hPrice') or 0),
            "p_fee": int(item.get('fleaMarketFee') or 0),
            "item_ref": id_map[api_id],
            "p_count": min(32767, int(item.get('lastOfferCount') or 0)),
            "p_change_s": int((item.get('changeLast48hPercent') or 0) * 100),
            "is_pve": is_pve 
        })

    # Upload data in chunks to handle Supabase payload size limits
    chunk_size = 1000
    for i in range(0, len(upload_data), chunk_size):
        chunk = upload_data[i:i + chunk_size]
        supabase.table("prices_2d").insert(chunk).execute()
        print(f"   -> {mode_label} prices chunk {i//chunk_size + 1} uploaded.")

# ==============================================================================
# Main Execution Sequence
# ==============================================================================
if __name__ == "__main__":
    try:
        # Phase 1: PvP Mode Data
        # We run mapping sync first using PvP data as it typically contains all items
        pvp_raw = fetch_tarkov_data(game_mode="regular")
        id_mapping = robust_mapping_sync(pvp_raw)
        push_prices_to_db(pvp_raw, id_mapping, is_pve=False)
        
        # Phase 2: PvE Mode Data
        # Uses the mapping generated in Phase 1 for consistency
        pve_raw = fetch_tarkov_data(game_mode="pve")
        push_prices_to_db(pve_raw, id_mapping, is_pve=True)
        
        print("🎉 Sync sequence for all modes completely finished!")
    except Exception as e:
        print(f"❌ Critical Error during sync: {e}")