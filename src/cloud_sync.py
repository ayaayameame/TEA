import os
import sys
import json
import time
import traceback
import urllib.request
import urllib.error
import http.client
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
    """
    Fetches the full item snapshot from json.tarkov.dev.

    The current Tarkov JSON API returns an envelope in the form:
        {
            "data": {
                "items": ...
            },
            "translations": [...]
        }

    For the items endpoint, "items" may be represented as an ID-keyed
    dictionary rather than a list. This function normalizes both forms into
    the list-of-dicts format expected by the existing database code.
    """
    mode_label = "PvE" if game_mode == "pve" else "PvP"
    print(f"⏳ Fetching raw {mode_label} data from Tarkov JSON API...")

    if game_mode not in ("regular", "pve"):
        raise ValueError(f"Unsupported Tarkov game mode: {game_mode}")

    url = f"https://json.tarkov.dev/{game_mode}/items"

    headers = {
        "Accept": "application/json",
        "User-Agent": "TarkovDataSync/10.0"
    }

    max_retries = 3

    for attempt in range(max_retries):
        try:
            req = urllib.request.Request(
                url,
                headers=headers,
                method="GET"
            )

            with urllib.request.urlopen(req, timeout=60) as response:
                status_code = response.status
                raw_body = response.read().decode("utf-8", errors="replace")

            if status_code != 200:
                raise RuntimeError(
                    f"HTTP {status_code}: {raw_body[:1500]}"
                )

            result = json.loads(raw_body)

            if not isinstance(result, dict):
                raise RuntimeError(
                    f"Unexpected API root type: {type(result).__name__}"
                )

            # Current JSON API envelope:
            # {
            #     "data": {...},
            #     "translations": [...]
            # }
            data = result.get("data")

            if not isinstance(data, dict):
                raise RuntimeError(
                    f"API 'data' is not an object: "
                    f"{type(data).__name__}"
                )

            # Current items endpoint stores the collection under data["items"].
            raw_items = data.get("items")

            if raw_items is None:
                # Defensive compatibility:
                # some JSON endpoints are represented as a top-level
                # ID-keyed collection inside "data".
                #
                # Do not mistake metadata fields for actual items.
                candidate_items = {
                    key: value
                    for key, value in data.items()
                    if isinstance(value, dict)
                    and isinstance(value.get("id"), str)
                }

                if candidate_items:
                    raw_items = candidate_items
                else:
                    raise RuntimeError(
                        "API returned a valid envelope but no 'items' "
                        f"collection. data keys: {list(data.keys())[:30]}"
                    )

            # Normalize:
            #   list -> list
            #   dict keyed by item ID -> list(values)
            if isinstance(raw_items, list):
                item_list = raw_items

            elif isinstance(raw_items, dict):
                item_list = []

                for key, value in raw_items.items():
                    if isinstance(value, dict):
                        item = dict(value)

                        # Some keyed payloads omit id inside the value.
                        if not item.get("id"):
                            item["id"] = key

                        item_list.append(item)

            else:
                raise RuntimeError(
                    f"Unexpected 'items' type: "
                    f"{type(raw_items).__name__}"
                )

            normalized_items = []

            for item in item_list:
                if not isinstance(item, dict):
                    continue

                item_id = (
                    item.get("id")
                    or item.get("_id")
                    or item.get("itemId")
                )

                if not item_id:
                    continue

                name = (
                    item.get("name")
                    or item.get("itemName")
                    or item.get("baseName")
                    or "Unknown"
                )

                short_name = (
                    item.get("shortName")
                    or item.get("short_name")
                    or item.get("shortname")
                    or name
                )

                normalized = dict(item)

                normalized["id"] = item_id
                normalized["name"] = name
                normalized["shortName"] = short_name

                normalized["updated"] = (
                    item.get("updated")
                    or item.get("lastUpdated")
                    or item.get("updatedAt")
                )

                # These are the exact fields consumed later by
                # push_market_data().
                normalized["avg24hPrice"] = item.get("avg24hPrice")
                normalized["lastLowPrice"] = item.get("lastLowPrice")
                normalized["low24hPrice"] = item.get("low24hPrice")
                normalized["high24hPrice"] = item.get("high24hPrice")
                normalized["lastOfferCount"] = item.get("lastOfferCount")

                normalized_items.append(normalized)

            if not normalized_items:
                raise RuntimeError(
                    "API returned an item collection, but no usable items "
                    "with IDs were found."
                )

            # Verify that the market fields used by the database pipeline
            # actually exist before proceeding. This prevents silently
            # writing a snapshot full of zero prices if the upstream schema
            # changes in the future.
            market_field_count = sum(
                1
                for item in normalized_items
                if any(
                    item.get(field) is not None
                    for field in (
                        "avg24hPrice",
                        "lastLowPrice",
                        "low24hPrice",
                        "high24hPrice",
                        "lastOfferCount",
                    )
                )
            )

            if market_field_count == 0:
                sample_keys = sorted(normalized_items[0].keys())
                raise RuntimeError(
                    "Items were fetched successfully, but none of the "
                    "expected market fields were found. "
                    f"Sample keys: {sample_keys}"
                )

            sample = normalized_items[0]

            print(
                f"✅ [{mode_label} API] Successfully fetched "
                f"{len(normalized_items)} items."
            )
            print(
                f"   Sample: id={sample.get('id')}, "
                f"name={sample.get('name')}, "
                f"avg24hPrice={sample.get('avg24hPrice')}, "
                f"lastLowPrice={sample.get('lastLowPrice')}, "
                f"lastOfferCount={sample.get('lastOfferCount')}"
            )

            return normalized_items

        except urllib.error.HTTPError as e:
            try:
                error_body = e.read().decode("utf-8", errors="replace")
            except Exception:
                error_body = ""

            print(
                f"⚠️ [{mode_label} API] HTTP {e.code}: {e.reason} "
                f"(Attempt {attempt + 1}/{max_retries})"
            )

            if error_body:
                print(f"   Server response: {error_body[:1500]}")

            if attempt < max_retries - 1:
                time.sleep(2)

        except http.client.IncompleteRead:
            print(
                f"⚠️ [{mode_label} API] Network packet loss "
                f"(IncompleteRead): Attempt {attempt + 1}/{max_retries}..."
            )

            if attempt < max_retries - 1:
                time.sleep(2)

        except urllib.error.URLError as e:
            print(
                f"⚠️ [{mode_label} API] Connection error: {e.reason} "
                f"(Attempt {attempt + 1}/{max_retries})..."
            )

            if attempt < max_retries - 1:
                time.sleep(2)

        except json.JSONDecodeError as e:
            print(
                f"⚠️ [{mode_label} API] Invalid JSON response: {e} "
                f"(Attempt {attempt + 1}/{max_retries})..."
            )

            if attempt < max_retries - 1:
                time.sleep(2)

        except Exception as e:
            print(
                f"⚠️ [{mode_label} API] Error: {e} "
                f"(Attempt {attempt + 1}/{max_retries})..."
            )

            if attempt < max_retries - 1:
                time.sleep(2)

    raise RuntimeError(
        f"❌ Failed to fetch Tarkov {mode_label} API "
        f"after {max_retries} attempts."
    )

# ==============================================================================
# 3. Item Mapping Logic
# ==============================================================================

def robust_mapping_sync(raw_items):
    """Syncs items via Upsert and fetches all mapping rows using pagination."""
    print("🛡️  Syncing item mapping (Upsert Logic)...")
    
# Prepare payload for the 'items' table
    payload_dict = {
        item['id']: {  # 🌟 修复：使用塔科夫的底层 id 作为内存去重的键
            "original_id": item['id'],
            "name": item['name'],
            "short_name": item.get('shortName', 'Unknown')
        } for item in raw_items if item.get('id')
    }
    
    # 🌟 修复: 使用 original_id 作为冲突判定的唯一依据
    supabase.table("items").upsert(list(payload_dict.values()), on_conflict="original_id").execute()
    
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
    """Pushes raw snapshots to the prices_2d table with Item-Level Deduplication."""
    mode_label = "PvE" if is_pve else "PvP"
    
    # Extract timestamps to determine the actual update time of this batch
    api_timestamps = [parse_tarkov_time(item.get('updated')) for item in raw_items]
    max_api_ts = max(api_timestamps) if api_timestamps else 0

    # Fetch the latest timestamp currently stored in the database for this game mode
    res = supabase.table("prices_2d").select("ts_fetch").eq("is_pve", is_pve).limit(1).order("ts_fetch", desc=True).execute()
    latest_db_ts = res.data[0]['ts_fetch'] if res.data else 0

    # If the API hasn't updated since our last fetch, skip the database write entirely
    if max_api_ts <= latest_db_ts:
        print(f"⏩ [{mode_label}] Global API time not updated, skipping sync.")
        return
    
    print(f"🔍 [{mode_label}] Fetching previous snapshot for deduplication...")
    last_prices = {}
    
    # 🌟 Incremental deduplication: Paginate and fetch all data from the previous run
    # (latest_db_ts) to build an in-memory comparison dictionary
    if latest_db_ts > 0:
        page_size, current_page = 1000, 0
        while True:
            start_idx = current_page * page_size
            end_idx = start_idx + page_size - 1
            snap_res = supabase.table("prices_2d").select("item_ref, p_min, p_avg, p_count")\
                .eq("is_pve", is_pve).eq("ts_fetch", latest_db_ts).range(start_idx, end_idx).execute()
            
            if not snap_res.data: break
            
            for row in snap_res.data:
                last_prices[row['item_ref']] = (row['p_min'], row['p_avg'], row['p_count'])
                
            if len(snap_res.data) < page_size: break
            current_page += 1

    upload_payload = []
    for item in raw_items:
        ref = id_map.get(item['id'])
        if not ref: continue

        p_avg = int(item.get('avg24hPrice') or 0)
        p_min = int(item.get('lastLowPrice') or 0)
        p_low = int(item.get('low24hPrice') or 0)
        p_high = int(item.get('high24hPrice') or 0)
        p_count = min(32767, int(item.get('lastOfferCount') or 0))

        # 🌟 Core filtering logic: Compare minimum price, average price, and offer count
        if ref in last_prices:
            last_min, last_avg, last_count = last_prices[ref]
            # If the three core dimensions remain unchanged, consider it zombie data and discard it directly
            if p_min == last_min and p_avg == last_avg and p_count == last_count:
                continue 
        
        upload_payload.append({
            "ts_fetch": max_api_ts, 
            "p_avg": p_avg,
            "p_min": p_min,
            "p_low": p_low,
            "p_high": p_high,
            "item_ref": ref,
            "p_count": p_count,
            "is_pve": is_pve 
        })

    # Batch insert into Supabase with safety chunking
    if upload_payload:
        skipped_count = len(raw_items) - len(upload_payload)
        print(f"🚀 [{mode_label}] Found {len(upload_payload)} active assets, filtered out {skipped_count} unchanged zombie records.")
        
        # Chunked upload, 1000 records per batch, to prevent Supabase 'Payload Too Large' errors
        chunk_size = 1000
        for i in range(0, len(upload_payload), chunk_size):
            supabase.table("prices_2d").insert(upload_payload[i:i+chunk_size]).execute()
            
        print(f"✅ [{mode_label}] High-frequency snapshot successfully written to the database.")
    else:
        print(f"⏩ [{mode_label}] No price or market changes detected, no database write quota consumed.")

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
        # With traceback and sys imported, the actual error will no longer be masked
        print(f"❌ Critical Error in sync engine: {traceback.format_exc() if 'traceback' in sys.modules else str(e)}")
