import sqlite3
import urllib.request
import json
import time
import os
from pathlib import Path

DB_PATH = Path(__file__).resolve().parent / "tarkov_zero_loss.db"

def setup_zero_loss_schema():
    """建立零误差、全精度存储结构"""
    if DB_PATH.exists(): os.remove(DB_PATH)
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    
    # 1. 高频表 (2d): 7 维度全精度
    cursor.execute('''CREATE TABLE prices_2d (
        ts_fetch INTEGER, ts_api INTEGER,    -- 8 bytes
        p_avg INTEGER, p_min INTEGER,        -- 8 bytes
        p_low INTEGER, p_high INTEGER,       -- 8 bytes
        p_fee INTEGER,                       -- 4 bytes
        item_ref SMALLINT,                   -- 2 bytes
        p_count SMALLINT,                    -- 2 bytes
        p_change_s SMALLINT,                 -- 2 bytes
        is_pve BOOLEAN                       -- 1 byte
    )''')
    
    # 2. 月线表 (30d): 4 核心价格全精度 (零误差)
    cursor.execute('''CREATE TABLE prices_30d (
        ts_fetch INTEGER,                    -- 4 bytes
        p_avg INTEGER, p_min INTEGER,        -- 8 bytes
        p_low INTEGER, p_high INTEGER,       -- 8 bytes
        item_ref SMALLINT,                   -- 2 bytes
        is_pve BOOLEAN                       -- 1 byte
    )''')
    
    # 3. 年线表 (365d): 2 核心价格全精度 (零误差)
    cursor.execute('''CREATE TABLE prices_365d (
        ts_fetch INTEGER,                    -- 4 bytes
        p_avg INTEGER, p_min INTEGER,        -- 8 bytes
        item_ref SMALLINT,                   -- 2 bytes
        is_pve BOOLEAN                       -- 1 byte
    )''')
    
    conn.commit()
    return conn

def fetch_real_data():
    query = "{ items { avg24hPrice lastLowPrice low24hPrice high24hPrice changeLast48hPercent fleaMarketFee lastOfferCount } }"
    url = 'https://api.tarkov.dev/graphql'
    headers = {'Content-Type': 'application/json', 'User-Agent': 'Mozilla/5.0'}
    req = urllib.request.Request(url, data=json.dumps({'query': query}).encode('utf-8'), headers=headers)
    with urllib.request.urlopen(req) as response:
        return json.loads(response.read().decode('utf-8'))['data']['items']

def run_v3_test():
    conn = setup_zero_loss_schema()
    cursor = conn.cursor()
    
    raw_items = fetch_real_data()
    item_count = len(raw_items)
    print(f"✅ 获取到 {item_count} 个物品。开始【零误差】填充...")

    sample_2d, sample_30d, sample_365d = [], [], []
    ts_now = int(time.time())

    for i, item in enumerate(raw_items):
        ref = i + 1
        p_avg = int(item.get('avg24hPrice') or 0)
        p_min = int(item.get('lastLowPrice') or 0)
        
        # 2d 样本 (全量全精度)
        d2 = (ts_now, ts_now, p_avg, p_min, 
              item.get('low24hPrice') or 0, item.get('high24hPrice') or 0,
              item.get('fleaMarketFee') or 0, ref,
              min(32767, item.get('lastOfferCount') or 0),
              int((item.get('changeLast48hPercent') or 0) * 100), False)
        sample_2d.append(d2)
        sample_2d.append((*d2[:-1], True))

        # 30d 样本 (4价格全精度)
        d30 = (ts_now, p_avg, p_min, 
               item.get('low24hPrice') or 0, item.get('high24hPrice') or 0,
               ref, False)
        sample_30d.append(d30)
        sample_30d.append((*d30[:-1], True))

        # 365d 样本 (2价格全精度 - 不再除以1000)
        d365 = (ts_now, p_avg, p_min, ref, False)
        sample_365d.append(d365)
        sample_365d.append((*d365[:-1], True))

    # 执行模拟填充
    for _ in range(288): cursor.executemany('INSERT INTO prices_2d VALUES (?,?,?,?,?,?,?,?,?,?,?)', sample_2d)
    for _ in range(720): cursor.executemany('INSERT INTO prices_30d VALUES (?,?,?,?,?,?,?)', sample_30d)
    for _ in range(365): cursor.executemany('INSERT INTO prices_365d VALUES (?,?,?,?,?)', sample_365d)

    conn.commit()
    
    file_size_mb = os.path.getsize(DB_PATH) / (1024 * 1024)
    print("\n" + "="*50)
    print(f"💎 零误差压力测试报告")
    print("="*50)
    print(f"📦 物理文件大小: {file_size_mb:.2f} MB")
    print(f"📈 500MB 占用率: {(file_size_mb/500)*100:.1f}%")
    print("-" * 50)
    if file_size_mb < 500:
        print("✅ 成功：在不牺牲 1 卢布精度的情况下，500MB 依然够用！")
    else:
        print("⚠️ 风险：空间非常紧迫，建议开启‘数据未变动跳过’逻辑。")
    print("="*50)
    conn.close()

if __name__ == "__main__":
    run_v3_test()