"""
QMT 数据采集测试脚本
"""
import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.logger import logger


def test_qmt_client():
    """测试 QMT 客户端"""
    from src.qmt.client import QMTClient
    
    logger.info("=== 测试 QMT 客户端 ===")
    
    # 创建客户端（需要配置 QMT 路径和账号）
    client = QMTClient(
        qmt_path="C:/zhiyue/zqxtspeed/xmXtp",
        account_id="your_account_id"
    )
    
    # 尝试连接
    if client.connect():
        logger.info("QMT 连接成功")
        
        # 测试获取股票列表
        try:
            stocks = client.get_stock_list()
            logger.info(f"获取到 {len(stocks)} 只股票")
        except Exception as e:
            logger.warning(f"获取股票列表失败: {e}")
        
        # 测试获取ETF列表
        try:
            etfs = client.get_etf_list()
            logger.info(f"获取到 {len(etfs)} 只ETF")
        except Exception as e:
            logger.warning(f"获取ETF列表失败: {e}")
        
        client.disconnect()
    else:
        logger.warning("QMT 连接失败，请检查 QMT 路径和账号")


def test_qmt_database():
    """测试 QMT 数据库"""
    from src.qmt.database import QMTDatabase
    
    logger.info("=== 测试 QMT 数据库 ===")
    
    db = QMTDatabase(db_path="./data/qmt_test.duckdb")
    
    # 获取所有表
    tables = db.get_tables()
    logger.info(f"数据库表: {tables}")
    
    # 获取每个表的记录数
    for table in tables:
        count = db.get_table_count(table)
        logger.info(f"  {table}: {count} 条")
    
    db.close()


def test_qmt_scheduler():
    """测试 QMT 调度器"""
    from src.qmt.scheduler import QMTScheduler
    
    logger.info("=== 测试 QMT 调度器 ===")
    
    scheduler = QMTScheduler(
        qmt_path="C:/zhiyue/zqxtspeed/xmXtp",
        account_id="your_account_id",
        db_path="./data/qmt_test.duckdb"
    )
    
    # 获取同步状态
    status = scheduler.get_sync_status()
    logger.info(f"同步状态: {status}")
    
    scheduler.close()


if __name__ == "__main__":
    # 测试数据库（不需要 QMT 连接）
    test_qmt_database()
    
    # 测试客户端（需要 QMT 运行）
    # test_qmt_client()
    
    # 测试调度器
    # test_qmt_scheduler()
