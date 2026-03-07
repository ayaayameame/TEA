"""
Module: sync_items.py
Description: Fetches all item IDs and names from tarkov.dev GraphQL API
             to build a local mapping file for GUI search.
"""
import json
import requests
from pathlib import Path

def fetch_item_mapping():
    # Use tarkov.dev GraphQL API (Public)
    query = """
    {
        items {
            id
            name
            shortName
        }
    }
    """
    url = 'https://api.tarkov.dev/graphql'
    
    print("[*] Connecting to Tarkov API...")
    response = requests.post(url, json={'query': query})
    
    if response.status_code == 200:
        data = response.json()['data']['items']
        # Structure: { "Item Name": "item_id" }
        mapping = {item['name']: item['id'] for item in data}
        
        # Save to data directory
        output_path = Path(__file__).parent.parent.parent / "data" / "items_mapping.json"
        output_path.parent.mkdir(parents=True, exist_ok=True)
        
        with open(output_path, 'w', encoding='utf-8') as f:
            json.dump(mapping, f, indent=4, ensure_ascii=False)
        
        print(f"[+] Successfully synced {len(mapping)} items to {output_path}")
    else:
        print(f"[-] Failed to fetch items: {response.status_code}")

if __name__ == "__main__":
    fetch_item_mapping()