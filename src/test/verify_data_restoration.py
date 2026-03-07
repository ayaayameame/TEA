import sqlite3
import random
from pathlib import Path

DB_PATH = Path(__file__).resolve().parent / "tarkov_tier_v2_optimized.db"

def verify_restoration():
    if not DB_PATH.exists():
        print("❌ 找不到测试数据库，请先运行 ultimate_tier_test_v2.py")
        return

    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    # 1. 随机挑选 10 个物品 ID (1 ~ 4848)
    test_ids = random.sample(range(1, 4849), 10)
    
    print(f"🔍 随机抽取 10 件物品进行数据还原验证...\n")
    print(f"{'Item ID':<8} | {'数据源':<10} | {'还原后的均价 (Avg)':<20} | {'精度损耗':<15}")
    print("-" * 65)

    for item_id in test_ids:
        # 从 prices_2d (高频无损) 读取
        cursor.execute("SELECT p_avg, p_change_s FROM prices_2d WHERE item_ref = ? LIMIT 1", (item_id,))
        row_2d = cursor.fetchone()
        
        # 从 prices_365d (年线压缩) 读取
        cursor.execute("SELECT p_avg_k FROM prices_365d WHERE item_ref = ? LIMIT 1", (item_id,))
        row_365 = cursor.fetchone()

        if row_2d and row_365:
            # --- 还原逻辑 ---
            # 2d 还原
            original_avg = row_2d[0]
            restored_change = row_2d[1] / 100.0  # 百分比还原
            
            # 365d 还原 (千卢布进位)
            restored_avg_365 = row_365[0] * 1000 
            
            # 计算损耗 (365d 相比 2d)
            loss = original_avg - restored_avg_365

            print(f"{item_id:<8} | {'2d (高频)':<10} | {original_avg:>12,} RUB | {'无损 (100%)':<15}")
            print(f"{'':<8} | {'365d (年线)':<10} | {restored_avg_365:>12,} RUB | {'误差: '+str(loss)+' R':<15}")
            print(f"{'':<8} | {'48h趋势':<10} | {restored_change:>13.2f} %     | {'无损':<15}")
            print("-" * 65)

    conn.close()

if __name__ == "__main__":
    verify_restoration()