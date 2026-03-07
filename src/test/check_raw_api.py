import urllib.request
import json
import os
from pathlib import Path

def check_raw_api():
    print("⏳ 正在向 Tarkov API 发送全量探测请求...")
    
    # 我们把能想到的有用字段全都要了
    query = """
    {
        items {
            id
            name
            shortName
            width
            height
            basePrice
            updated
            sellFor {
                vendor { normalizedName }
                price
                currency
            }
            buyFor {
                vendor { normalizedName }
                price
                currency
            }
        }
    }
    """
    url = 'https://api.tarkov.dev/graphql'
    headers = {
        'Content-Type': 'application/json',
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
    }
    
    req = urllib.request.Request(url, data=json.dumps({'query': query}).encode('utf-8'), headers=headers)
    
    try:
        with urllib.request.urlopen(req) as response:
            raw_data = json.loads(response.read().decode('utf-8'))
            items = raw_data['data']['items']
            
            # 1. 保存全量数据到本地 JSON 文件，方便你慢慢看
            output_file = Path(__file__).parent / "raw_tarkov_data.json"
            with open(output_file, 'w', encoding='utf-8') as f:
                json.dump(items, f, indent=4, ensure_ascii=False)
            print(f"✅ 全量数据已保存至: {output_file}")
            
            # 2. 在控制台挑一个热门物品（比如显卡）详细打印出来分析
            print("\n" + "="*50)
            print("🔍 核心数据结构解剖 (以单个物品为例):")
            print("="*50)
            
            target_item = next((item for item in items if "Graphics card" in item['name']), items[0])
            
            print(f"🏷️ 物品名称: {target_item['name']} ({target_item['shortName']})")
            print(f"🆔 唯一 ID: {target_item['id']}")
            print(f"📦 占用格子: {target_item.get('width', 1)} x {target_item.get('height', 1)}")
            print(f"💰 游戏基础价: {target_item.get('basePrice')} 卢布")
            print(f"⏱️ 数据更新时间: {target_item.get('updated')}")
            
            print("\n🛒 【你买入】的价格 (buyFor):")
            if target_item.get('buyFor'):
                for offer in target_item['buyFor']:
                    vendor = offer['vendor']['normalizedName']
                    price = offer['price']
                    currency = offer['currency']
                    print(f"  - 渠道: {vendor:<15} | 价格: {price:,} {currency}")
            else:
                print("  - 无买入数据")

            print("\n🤝 【你卖出】的价格 (sellFor):")
            if target_item.get('sellFor'):
                for offer in target_item['sellFor']:
                    vendor = offer['vendor']['normalizedName']
                    price = offer['price']
                    currency = offer['currency']
                    print(f"  - 渠道: {vendor:<15} | 价格: {price:,} {currency}")
            else:
                print("  - 无卖出数据")
                
            print("="*50)
            print("💡 提示: 你可以打开刚才保存的 raw_tarkov_data.json 查看所有物品！")

    except Exception as e:
        print(f"❌ 请求失败: {e}")

if __name__ == "__main__":
    check_raw_api()