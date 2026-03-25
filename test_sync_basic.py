# -*- coding: utf-8 -*-
"""
测试基础数据同步
"""
import sys
import os

# 添加项目路径
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from sync_starlight import StarlightSyncManager

def test_basic_sync():
    """测试基础数据同步"""
    try:
        manager = StarlightSyncManager()
        manager.connect()
        
        # 只测试基础数据同步
        manager.sync_basic_data()
        
        print("✓ 基础数据同步测试完成")
        
    except Exception as e:
        print(f"✗ 测试失败: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    test_basic_sync()