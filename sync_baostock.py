"""
Baostock 同步脚本
"""
import sys
import os

# 添加项目路径
project_path = r'C:\Users\mubin\Desktop\amazing_data_system'
sys.path.insert(0, project_path)
os.chdir(project_path)

from src.collectors.baostock.client import BaostockClient
import time

def main():
    # 使用 baostock_full.duckdb
    db_path = r'C:\Users\mubin\Desktop\amazing_data_system\data\baostock_full.duckdb'
    
    client = BaostockClient(db_path=db_path)
    client.connect()
    
    # 同步股票列表（如果需要）
    # result = client.sync_stock_list()
    
    # 获取全部股票
    all_codes = client.get_all_codes()
    print(f'总股票: {len(all_codes)}')
    
    # 获取已同步的股票
    synced = set([r[0] for r in client.db.conn.execute('SELECT sec_code FROM daily_kline').fetchall()])
    
    # 过滤未同步的
    codes = [c for c in all_codes if c not in synced]
    print(f'已同步: {len(synced)}')
    print(f'待同步: {len(codes)}')
    
    # 增量同步（force=False）
    print('开始增量同步...')
    total = 0
    start = time.time()
    for i, code in enumerate(codes):
        result = client.sync_daily_kline(code, force=False)  # 增量同步
        total += result.get('record_count', 0)
        if (i + 1) % 100 == 0:
            elapsed = time.time() - start
            rate = (i + 1) / elapsed * 60
            remaining = (len(codes) - i - 1) / rate
            print(f'进度: {i+1}/{len(codes)}, 累计: {total}条, 预计剩余: {remaining:.0f}分钟')
    
    print(f'完成! 总耗时: {(time.time()-start)/60:.1f}分钟')
    print('最终状态:', client.get_sync_status())

if __name__ == '__main__':
    main()
