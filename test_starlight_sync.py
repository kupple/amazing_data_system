"""
Starlight 数据同步测试脚本
"""
import sys
from pathlib import Path

# 添加项目根目录到路径
sys.path.insert(0, str(Path(__file__).parent))

from src.collectors.starlight.scheduler import get_scheduler
from src.collectors.starlight.client import get_client
from src.common.database import get_db
from src.common.logger import logger


def test_connection():
    """测试连接"""
    logger.info("=" * 50)
    logger.info("测试 1: 连接测试")
    logger.info("=" * 50)
    
    # 测试客户端连接
    client = get_client()
    if client.connect():
        logger.info("✓ AmazingData 连接成功")
    else:
        logger.error("✗ AmazingData 连接失败")
        return False
    
    # 测试数据库连接
    try:
        db = get_db("starlight")
        tables = db.get_tables()
        logger.info(f"✓ ClickHouse 连接成功，当前有 {len(tables)} 个表")
    except Exception as e:
        logger.error(f"✗ ClickHouse 连接失败: {e}")
        return False
    
    return True


def test_basic_data():
    """测试基础数据获取"""
    logger.info("\n" + "=" * 50)
    logger.info("测试 2: 基础数据获取")
    logger.info("=" * 50)
    
    client = get_client()
    
    # 测试获取股票列表
    try:
        code_list = client.get_code_list("EXTRA_STOCK_A")
        logger.info(f"✓ 获取股票列表成功，共 {len(code_list)} 只")
        logger.info(f"  示例: {code_list[:5]}")
    except Exception as e:
        logger.error(f"✗ 获取股票列表失败: {e}")
        return False
    
    # 测试获取交易日历
    try:
        calendar = client.get_calendar()
        logger.info(f"✓ 获取交易日历成功，共 {len(calendar)} 个交易日")
        logger.info(f"  最近: {calendar[-5:]}")
    except Exception as e:
        logger.error(f"✗ 获取交易日历失败: {e}")
        return False
    
    return True


def test_sync_basic():
    """测试基础数据同步"""
    logger.info("\n" + "=" * 50)
    logger.info("测试 3: 基础数据同步")
    logger.info("=" * 50)
    
    scheduler = get_scheduler()
    
    try:
        result = scheduler.sync_basic_data()
        logger.info(f"✓ 基础数据同步完成")
        logger.info(f"  结果: {result}")
        
        # 检查数据库
        db = get_db("starlight")
        
        # 检查股票代码表
        if db.table_exists("stock_codes"):
            count = db.get_table_count("stock_codes")
            logger.info(f"  - stock_codes: {count} 条记录")
        
        # 检查交易日历
        if db.table_exists("trading_calendar"):
            count = db.get_table_count("trading_calendar")
            logger.info(f"  - trading_calendar: {count} 条记录")
        
        # 检查证券基础信息
        if db.table_exists("stock_basic"):
            count = db.get_table_count("stock_basic")
            logger.info(f"  - stock_basic: {count} 条记录")
        
        return True
        
    except Exception as e:
        logger.error(f"✗ 基础数据同步失败: {e}")
        import traceback
        traceback.print_exc()
        return False


def test_query_data():
    """测试数据查询"""
    logger.info("\n" + "=" * 50)
    logger.info("测试 4: 数据查询")
    logger.info("=" * 50)
    
    db = get_db("starlight")
    
    # 查询股票代码
    try:
        df = db.query("SELECT * FROM stock_codes LIMIT 5")
        logger.info(f"✓ 查询股票代码成功")
        logger.info(f"\n{df}")
    except Exception as e:
        logger.error(f"✗ 查询失败: {e}")
        return False
    
    # 查询同步状态
    try:
        status = db.get_sync_status()
        logger.info(f"✓ 查询同步状态成功")
        for s in status[:3]:
            logger.info(f"  - {s}")
    except Exception as e:
        logger.error(f"✗ 查询同步状态失败: {e}")
    
    return True


def test_incremental_update():
    """测试增量更新"""
    logger.info("\n" + "=" * 50)
    logger.info("测试 5: 增量更新")
    logger.info("=" * 50)
    
    import pandas as pd
    from datetime import datetime
    
    db = get_db("starlight")
    
    # 创建测试数据
    test_data = pd.DataFrame({
        "code": ["TEST001", "TEST002", "TEST003"],
        "name": ["测试1", "测试2", "测试3"],
        "trade_date": [datetime.now().strftime("%Y-%m-%d")] * 3,
        "price": [10.5, 20.3, 15.8]
    })
    
    try:
        # 第一次插入
        db.incremental_update(
            "test_incremental",
            test_data,
            key_columns=["code", "trade_date"],
            date_column="trade_date"
        )
        count1 = db.get_table_count("test_incremental")
        logger.info(f"✓ 第一次插入成功，共 {count1} 条")
        
        # 第二次插入（应该被去重）
        db.incremental_update(
            "test_incremental",
            test_data,
            key_columns=["code", "trade_date"],
            date_column="trade_date"
        )
        count2 = db.get_table_count("test_incremental")
        logger.info(f"✓ 第二次插入完成，共 {count2} 条（去重生效）")
        
        if count1 == count2:
            logger.info("✓ 增量去重功能正常")
        else:
            logger.warning(f"⚠ 去重可能未生效: {count1} -> {count2}")
        
        # 清理测试表
        db.execute("DROP TABLE IF EXISTS test_incremental")
        logger.info("✓ 测试表已清理")
        
        return True
        
    except Exception as e:
        logger.error(f"✗ 增量更新测试失败: {e}")
        import traceback
        traceback.print_exc()
        return False


def main():
    """主测试流程"""
    logger.info("开始 Starlight 数据同步测试")
    logger.info("=" * 50)
    
    tests = [
        ("连接测试", test_connection),
        ("基础数据获取", test_basic_data),
        ("基础数据同步", test_sync_basic),
        ("数据查询", test_query_data),
        ("增量更新", test_incremental_update),
    ]
    
    results = []
    for name, test_func in tests:
        try:
            success = test_func()
            results.append((name, success))
        except Exception as e:
            logger.error(f"测试 {name} 异常: {e}")
            results.append((name, False))
    
    # 汇总结果
    logger.info("\n" + "=" * 50)
    logger.info("测试结果汇总")
    logger.info("=" * 50)
    
    for name, success in results:
        status = "✓ 通过" if success else "✗ 失败"
        logger.info(f"{status} - {name}")
    
    passed = sum(1 for _, s in results if s)
    total = len(results)
    logger.info(f"\n总计: {passed}/{total} 通过")
    
    if passed == total:
        logger.info("🎉 所有测试通过！")
    else:
        logger.warning(f"⚠️  有 {total - passed} 个测试失败")


if __name__ == "__main__":
    main()
