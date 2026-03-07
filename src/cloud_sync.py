import os
import json
import time
import urllib.request
from datetime import datetime
from dotenv import load_dotenv
from supabase import create_client, Client

# ==========================================
# Configuration & Initialization
# ==========================================

load_dotenv()

SUPABASE_URL = os.environ.get("SUPABASE_URL")
SUPABASE_KEY = os.environ.get("SUPABASE_KEY")

if not SUPABASE_URL or not SUPABASE_KEY:
    raise ValueError("❌ Missing Supabase credentials. Please check your .env file.")

# Initialize Supabase client
supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

# ==========================================
# Helper Functions
# ==========================================

def parse_tarkov_time(iso_str):
    """Converts Tarkov API ISO8601 string to Unix Timestamp."""
    if not iso_str: return 0
    try:
        return int(datetime.fromisoformat(iso_str.replace('Z', '+00:00')).timestamp())
    except Exception:
        return 0

def fetch_tarkov_data(game_mode="regular"):
    """
    Fetches market data from Tarkov.dev.
    game_mode: "regular" for PvP, "pve" for PvE mode.
    """
    mode_label = "PvE" if game_mode == "pve" else "PvP"
    print(f"⏳ Fetching raw {mode_label} data from Tarkov API...")
    
    # GraphQL query with dynamic gameMode parameter
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
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
    }
    
    req = urllib.request.Request(url, data=json.dumps({'query': query}).encode('utf-8'), headers=headers)
    
    with urllib.request.urlopen(req) as response:
        return json.loads(response.read().decode('utf-8'))['data']['items']

def robust_mapping_sync(raw_items):
    """Synchronizes items with the DB. Anchors on Name to prevent ID swap corruption."""
    print("🛡️ Running Dual-Verification item mapping...")
    
    res = supabase.table("items").select("item_numeric_id, original_id, name").execute()
    db_items = res.data
    
    name_to_num_id = {row['name']: row['item_numeric_id'] for row in db_items}
    name_to_orig_id = {row['name']: row['original_id'] for row in db_items}
    
    new_items = []
    updates_needed = []
    final_mapping = {}
    seen_new_names = set()

    for item in raw_items:
        api_id = item['id']
        api_name = item.get('name', 'Unknown')
        
        if api_name in name_to_num_id:
            num_id = name_to_num_id[api_name]
            final_mapping[api_id] = num_id
            if name_to_orig_id[api_name] != api_id:
                updates_needed.append({"item_numeric_id": num_id, "original_id": api_id, "name": api_name})
        else:
            if api_name not in seen_new_names:
                new_items.append({
                    "original_id": api_id,
                    "name": api_name,
                    "short_name": item.get('shortName', 'Unknown')
                })
                seen_new_names.add(api_name)

    if updates_needed:
        supabase.table("items").upsert(updates_needed).execute()
        print(f"🔧 Fixed {len(updates_needed)} ID-swap inconsistencies.")
        
    if new_items:
        print(f"✨ Registering {len(new_items)} new items...")
        chunk_size = 1000
        for i in range(0, len(new_items), chunk_size):
            chunk = new_items[i:i + chunk_size]
            insert_res = supabase.table("items").insert(chunk).execute()
            for row in insert_res.data:
                final_mapping[row['original_id']] = row['item_numeric_id']
            print(f"   -> Item chunk {i//chunk_size + 1} registered.")
                
    return final_mapping

def push_prices_to_db(raw_items, id_map, is_pve=False):
    """Processes and uploads price payload. Skips if mode data is not updated."""
    mode_label = "PvE" if is_pve else "PvP"
    print(f"🔍 Checking {mode_label} data status in database...")
    
    # Crucial: Filter by is_pve to track modes independently
    res = supabase.table("prices_2d").select("ts_api")\
        .eq("is_pve", is_pve)\
        .limit(1).order("ts_api", desc=True).execute()
    
    latest_db_ts = res.data[0]['ts_api'] if res.data else 0
    api_timestamps = [parse_tarkov_time(item.get('updated')) for item in raw_items]
    max_api_ts = max(api_timestamps) if api_timestamps else 0

    if max_api_ts <= latest_db_ts:
        print(f"⏩ SKIP {mode_label}: Data is already current (TS: {max_api_ts}).")
        return

    print(f"🚀 {mode_label} data is fresh! Preparing payload...")
    ts_now = int(time.time())
    upload_data = []
    
    for item in raw_items:
        api_id = item['id']
        if api_id not in id_map: continue
        
        item_ts_api = parse_tarkov_time(item.get('updated'))
        if item_ts_api == 0: item_ts_api = max_api_ts
            
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

    # Upload in chunks of 1000 to respect Supabase API limits
    chunk_size = 1000
    for i in range(0, len(upload_data), chunk_size):
        chunk = upload_data[i:i + chunk_size]
        supabase.table("prices_2d").insert(chunk).execute()
        print(f"   -> {mode_label} prices chunk {i//chunk_size + 1} uploaded.")

# ==========================================
# Main Execution
# ==========================================
if __name__ == "__main__":
    try:
        # Phase 1: PvP Data (Also updates the global item mapping)
        pvp_raw = fetch_tarkov_data(game_mode="regular")
        id_mapping = robust_mapping_sync(pvp_raw)
        push_prices_to_db(pvp_raw, id_mapping, is_pve=False)
        
        # Phase 2: PvE Data
        pve_raw = fetch_tarkov_data(game_mode="pve")
        push_prices_to_db(pve_raw, id_mapping, is_pve=True)
        
        print("🎉 Sync sequence for all modes completely finished!")
    except Exception as e:
        print(f"❌ Critical Error during sync: {e}")