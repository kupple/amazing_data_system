"""
Baostock 同步脚本
"""
import sys
import os

# 添加项目路径
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from src.collectors.baostock.client import BaostockClient
import time


def main():
    client = BaostockClient()
    client.connect()

    # 获取全部股票
    all_codes = client.get_all_codes()
    print(f'总股票: {len(all_codes)}')

    # 获取已同步的股票
    result = client.db.client.query('SELECT DISTINCT sec_code FROM daily_kline')
    synced = set([r[0] for r in result.result_rows])

    # 过滤未同步的
    codes = [c for c in all_codes if c not in synced]
    print(f'已同步: {len(synced)}')
    print(f'待同步: {len(codes)}')

    # 增量同步
    print('开始增量同步...')
    total = 0
    start = time.time()
    for i, code in enumerate(codes):
        result = client.sync_daily_kline(code, force=False)
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
