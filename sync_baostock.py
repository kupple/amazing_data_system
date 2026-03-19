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
    # 使用绝对路径
    db_path = r'C:\Users\mubin\Desktop\amazing_data_system\data\baostock_data.duckdb'
    
    client = BaostockClient(db_path=db_path)
    client.connect()
    
    # 同步股票列表
    print('=== 同步股票列表 ===')
    result = client.sync_stock_list()
    print(result)
    
    # 获取全部股票
    all_codes = client.get_all_codes()
    print(f'总股票: {len(all_codes)}')
    
    # 过滤只保留常规A股
    def is_regular_a_share(code):
        if code.startswith('600') or code.startswith('601') or code.startswith('603') or code.startswith('605'):
            return True
        if code.startswith('688') or code.startswith('689'):
            return True
        if code.startswith('000') and len(code) == 7:
            return True
        if code.startswith('001') and len(code) == 7:
            return True
        if code.startswith('002') and len(code) == 7:
            return True
        return False
    
    regular_a = [c for c in all_codes if is_regular_a_share(c)]
    print(f'常规A股: {len(regular_a)}')
    
    # 同步日线（后复权+全部字段）
    print('开始同步全部历史数据（后复权）...')
    total = 0
    start = time.time()
    for i, code in enumerate(regular_a):
        result = client.sync_daily_kline(code, force=True)
        total += result.get('record_count', 0)
        if (i + 1) % 100 == 0:
            elapsed = time.time() - start
            rate = (i + 1) / elapsed * 60
            remaining = (len(regular_a) - i - 1) / rate
            print(f'进度: {i+1}/{len(regular_a)}, 累计: {total}条, 预计剩余: {remaining:.0f}分钟')
    
    print(f'完成! 总耗时: {(time.time()-start)/60:.1f}分钟')
    print('最终状态:', client.get_sync_status())

if __name__ == '__main__':
    main()
