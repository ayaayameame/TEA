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

load_dotenv()

SUPABASE_URL = os.environ.get("SUPABASE_URL")
SUPABASE_KEY = os.environ.get("SUPABASE_KEY")

if not SUPABASE_URL or not SUPABASE_KEY:
    raise ValueError("❌ Missing Supabase credentials. Check GitHub Secrets.")

# Initialize Supabase client
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
    headers = {'Content-Type': 'application/json', 'User-Agent': 'TarkovDataSync/7.0'}
    
    req = urllib.request.Request(url, data=json.dumps({'query': query}).encode('utf-8'), headers=headers)
    
    with urllib.request.urlopen(req) as response:
        result = json.loads(response.read().decode('utf-8'))
        return result['data']['items']

# ==============================================================================
# 3. Item Mapping Logic (Bypassing 1000-row limit)
# ==============================================================================

def robust_mapping_sync(raw_items):
    """Syncs items via Upsert and fetches all mapping rows using pagination."""
    print("🛡️ Syncing item mapping (Upsert Logic)...")
    
    payload_dict = {
        item['name']: {
            "original_id": item['id'],
            "name": item['name'],
            "short_name": item.get('shortName', 'Unknown')
        } for item in raw_items if item.get('name')
    }
    
    # Update item definitions
    supabase.table("items").upsert(list(payload_dict.values()), on_conflict="name").execute()
    
    # Pagination loop to fetch all IDs
    all_mapping_rows = []
    page_size, current_page = 1000, 0
    
    print("🔍 Fetching full mapping from database...")
    while True:
        start, end = current_page * page_size, (current_page + 1) * page_size - 1
        res = supabase.table("items").select("item_numeric_id, original_id").range(start, end).execute()
        if not res.data: break
        all_mapping_rows.extend(res.data)
        if len(res.data) < page_size: break
        current_page += 1
    
    final_mapping = {row['original_id']: row['item_numeric_id'] for row in all_mapping_rows}
    print(f"✅ Mapping refreshed: {len(final_mapping)} items ready.")
    return final_mapping

# ==============================================================================
# 4. Tiered Storage Logic (2d, 7d, 365d)
# ==============================================================================

def push_market_data(raw_items, id_map, is_pve=False):
    """Distributes data to Live (10m), Weekly (Hourly), and Yearly (Daily) tables."""
    mode_label = "PvE" if is_pve else "PvP"
    
    # Check for duplicate API snapshots
    res = supabase.table("prices_2d").select("ts_api").eq("is_pve", is_pve).limit(1).order("ts_api", desc=True).execute()
    latest_db_ts = res.data[0]['ts_api'] if res.data else 0
    api_timestamps = [parse_tarkov_time(item.get('updated')) for item in raw_items]
    max_api_ts = max(api_timestamps) if api_timestamps else 0

    if max_api_ts <= latest_db_ts:
        print(f"⏩ {mode_label} is up-to-date. Skipping.")
        return

    ts_now = int(time.time())
    now_dt = datetime.now()
    ts_hour_anchor = int(now_dt.strftime('%Y%m%d%H')) # YYYYMMDDHH
    ts_day_anchor = int(now_dt.strftime('%Y%m%d'))    # YYYYMMDD

    live_2d, history_7d, history_365d = [], [], []
    
    for item in raw_items:
        api_id = item['id']
        if api_id not in id_map: continue
        
        num_id = id_map[api_id]
        p_avg = int(item.get('avg24hPrice') or 0)
        p_count = min(32767, int(item.get('lastOfferCount') or 0))

        # Live Data (Every 10 mins)
        live_2d.append({
            "ts_fetch": ts_now, "ts_api": parse_tarkov_time(item.get('updated')) or max_api_ts,
            "p_avg": p_avg, "p_min": int(item.get('lastLowPrice') or 0),
            "p_low": int(item.get('low24hPrice') or 0), "p_high": int(item.get('high24hPrice') or 0),
            "p_fee": int(item.get('fleaMarketFee') or 0), "item_ref": num_id,
            "p_count": p_count, "p_change_s": int((item.get('changeLast48hPercent') or 0) * 100),
            "is_pve": is_pve 
        })

        # Weekly Data (Hourly snapshots)
        history_7d.append({
            "ts_fetch": ts_hour_anchor, "p_avg": p_avg, "p_min": int(item.get('lastLowPrice') or 0),
            "p_low": int(item.get('low24hPrice') or 0), "p_high": int(item.get('high24hPrice') or 0),
            "item_ref": num_id, "p_count": p_count, "is_pve": is_pve
        })

        # Yearly Data (Daily summaries)
        history_365d.append({
            "ts_fetch": ts_day_anchor, "p_avg": p_avg, "p_min": int(item.get('lastLowPrice') or 0),
            "item_ref": num_id, "p_count": p_count, "is_pve": is_pve
        })

    # Execute all uploads
    if live_2d:
        supabase.table("prices_2d").insert(live_2d).execute()
        print(f"✅ {mode_label} live records pushed.")
    if history_7d:
        supabase.table("prices_7d").upsert(history_7d, on_conflict="item_ref, is_pve, ts_fetch").execute()
        print(f"📊 {mode_label} hourly snapshot updated.")
    if history_365d:
        supabase.table("prices_365d").upsert(history_365d, on_conflict="item_ref, is_pve, ts_fetch").execute()
        print(f"📈 {mode_label} daily summary updated.")

# ==============================================================================
# 5. Main Execution
# ==============================================================================
if __name__ == "__main__":
    try:
        pvp_data = fetch_tarkov_data(game_mode="regular")
        id_mapping = robust_mapping_sync(pvp_data)
        push_market_data(pvp_data, id_mapping, is_pve=False)
        
        pve_data = fetch_tarkov_data(game_mode="pve")
        push_market_data(pve_data, id_mapping, is_pve=True)
        print("🎉 Tiered sync finished successfully!")
    except Exception as e:
        print(f"❌ Critical Error: {e}")