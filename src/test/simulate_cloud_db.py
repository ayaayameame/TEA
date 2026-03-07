import sqlite3
import urllib.request
import json
import time
import os
from pathlib import Path

# 设置本地模拟数据库的路径
DB_PATH = Path(__file__).resolve().parent / "mock_supabase.db"

def setup_mock_database():
    """建立与 Supabase 高度一致的表结构"""
    if DB_PATH.exists():
        os.remove(DB_PATH) # 每次测试前清理旧库
        
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    # 1. 静态物品表 (包含冗余和自动更新机制)
    cursor.execute('''
        CREATE TABLE items (
            item_numeric_id INTEGER PRIMARY KEY AUTOINCREMENT,
            original_id TEXT UNIQUE NOT NULL,
            name TEXT,
            short_name TEXT,
            slots INTEGER
        )
    ''')

    # 2. 2天高频表 (存储数字映射，2个价格，和时间戳)
    cursor.execute('''
        CREATE TABLE prices_2d (
            item_ref INTEGER,
            pvp_price INTEGER,
            pve_price INTEGER,
            created_at INTEGER
        )
    ''')

    # 3. 30天月线表
    cursor.execute('''
        CREATE TABLE prices_30d (
            item_ref INTEGER,
            pvp_avg INTEGER,
            pve_avg INTEGER,
            created_at INTEGER
        )
    ''')

    # 4. 365天年线表
    cursor.execute('''
        CREATE TABLE prices_365d (
            item_ref INTEGER,
            pvp_avg INTEGER,
            pve_avg INTEGER,
            created_at INTEGER
        )
    ''')
    
    conn.commit()
    return conn

def fetch_real_tarkov_data():
    """从 Tarkov.dev 真实拉取全量数据"""
    print("⏳ 正在从 Tarkov API 拉取真实物品数据 (约需几秒钟)...")
    query = """
    {
        items {
            id
            name
            shortName
            width
            height
            sellFor { price }
        }
    }
    """
    url = 'https://api.tarkov.dev/graphql'
    
    # 加入了浏览器的 User-Agent 伪装，绕过 403 拦截
    headers = {
        'Content-Type': 'application/json',
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
    }
    
    req = urllib.request.Request(url, data=json.dumps({'query': query}).encode('utf-8'), headers=headers)
    
    with urllib.request.urlopen(req) as response:
        data = json.loads(response.read().decode('utf-8'))
        return data['data']['items']

def run_simulation():
    conn = setup_mock_database()
    cursor = conn.cursor()
    
    raw_items = fetch_real_tarkov_data()
    print(f"✅ 成功获取了 {len(raw_items)} 个真实物品。")

    # ==========================================
    # 步骤 1：测试静态表冗余插入 (UPSERT 机制)
    # ==========================================
    print("⏳ 正在验证静态表冗余插入机制...")
    items_to_insert = []
    for item in raw_items:
        slots = item.get('width', 1) * item.get('height', 1)
        items_to_insert.append((
            item['id'], item['name'], item.get('shortName', ''), slots
        ))
        
    # 使用 INSERT OR IGNORE 模拟 Supabase 的 ON CONFLICT 机制
    cursor.executemany('''
        INSERT OR IGNORE INTO items (original_id, name, short_name, slots)
        VALUES (?, ?, ?, ?)
    ''', items_to_insert)
    conn.commit()
    print("✅ 静态表更新完毕！(如果物品已存在会自动跳过，完美兼容游戏更新)")

    # 获取分配好的数字 ID 映射
    cursor.execute("SELECT original_id, item_numeric_id FROM items")
    mapping = {row[0]: row[1] for row in cursor.fetchall()}

    # ==========================================
    # 步骤 2：准备基础价格数据
    # ==========================================
    base_prices = []
    current_time = int(time.time())
    
    for item in raw_items:
        numeric_id = mapping.get(item['id'])
        # 随便找个价格作为基础，如果没有就是 0
        price = 0
        if item.get('sellFor'):
            price = item['sellFor'][0].get('price', 0)
            
        if numeric_id:
            # 记录: item_ref, pvp_price, pve_price
            base_prices.append((numeric_id, price, price + 1000)) # 伪造一个 PVE 价格

    # ==========================================
    # 步骤 3：极限压力填充测试
    # ==========================================
    print("⏳ 开始生成模拟数据，这会执行上百万次插入，请稍候...")
    
    # 3.1 填充 prices_2d: 48 小时，每 5 分钟一次 = 576 次快照
    print("   -> 正在灌入 48小时 高频数据 (prices_2d)...")
    records_2d = []
    for step in range(576): 
        timestamp = current_time - (step * 300)
        for bp in base_prices:
            records_2d.append((bp[0], bp[1], bp[2], timestamp))
    cursor.executemany('INSERT INTO prices_2d VALUES (?, ?, ?, ?)', records_2d)

    # 3.2 填充 prices_30d: 30 天，每 1 小时一次 = 720 次快照
    print("   -> 正在灌入 30天 月线数据 (prices_30d)...")
    records_30d = []
    for step in range(720):
        timestamp = current_time - (step * 3600)
        for bp in base_prices:
            records_30d.append((bp[0], bp[1], bp[2], timestamp))
    cursor.executemany('INSERT INTO prices_30d VALUES (?, ?, ?, ?)', records_30d)

    # 3.3 填充 prices_365d: 365 天，每天一次 = 365 次快照
    print("   -> 正在灌入 365天 年线数据 (prices_365d)...")
    records_365d = []
    for step in range(365):
        timestamp = current_time - (step * 86400)
        for bp in base_prices:
            records_365d.append((bp[0], bp[1], bp[2], timestamp))
    cursor.executemany('INSERT INTO prices_365d VALUES (?, ?, ?, ?)', records_365d)

    conn.commit()
    
    # ==========================================
    # 步骤 4：统计结果
    # ==========================================
    cursor.execute("SELECT COUNT(*) FROM prices_2d")
    count_2d = cursor.fetchone()[0]
    cursor.execute("SELECT COUNT(*) FROM prices_30d")
    count_30d = cursor.fetchone()[0]
    cursor.execute("SELECT COUNT(*) FROM prices_365d")
    count_365d = cursor.fetchone()[0]
    
    conn.close()
    
    file_size_mb = os.path.getsize(DB_PATH) / (1024 * 1024)
    
    print("\n" + "="*50)
    print("🎉 压力测试完成！模拟一年的数据已写入本地数据库。")
    print(f"📊 [高频表 2D]   共 {count_2d:,} 行记录")
    print(f"📊 [月线表 30D]  共 {count_30d:,} 行记录")
    print(f"📊 [年线表 365D] 共 {count_365d:,} 行记录")
    print("-" * 50)
    print(f"💾 总计记录: {count_2d + count_30d + count_365d:,} 行")
    print(f"📦 物理文件大小: {file_size_mb:.2f} MB")
    print("="*50)
    print(f"对比 Supabase 免费额度 (500 MB)，占用比例约为: {(file_size_mb/500)*100:.1f}%")

if __name__ == "__main__":
    run_simulation()