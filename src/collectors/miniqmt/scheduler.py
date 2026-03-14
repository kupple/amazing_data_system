"""
QMT 数据同步调度器
"""
from datetime import datetime, timedelta
from typing import Optional, List
import pandas as pd

from src.collectors.miniqmt.client import QMTClient, get_qmt_client
from src.collectors.miniqmt.database import QMTDatabase, get_qmt_db
from src.common.logger import logger
from src.common.retry import retry


class QMTScheduler:
    """QMT 数据同步调度器"""
    
    def __init__(self, 
                 qmt_path: Optional[str] = None,
                 account_id: Optional[str] = None,
                 db_path: str = "./data/qmt_data.duckdb"):
        """
        初始化调度器
        
        Args:
            qmt_path: QMT 安装路径
            account_id: 资金账号
            db_path: 数据库路径
        """
        self.client = get_qmt_client(qmt_path, account_id)
        self.db = get_qmt_db(db_path)
        
        # 常用板块列表
        self.common_sectors = [
            "银行", "房地产", "医药生物", "电子", "计算机",
            "新能源", "汽车", "有色金属", "化工", "机械设备",
            "食品饮料", "家用电器", "电力设备", "建筑材料", "交通运输"
        ]
        
        # 常用指数
        self.common_indexes = [
            "000001.SH",  # 上证指数
            "399001.SH",  # 深证成指
            "399006.SH",  # 创业板指
            "000300.SH",  # 沪深300
            "000905.SH",  # 中证500
            "000016.SH",  # 上证50
        ]
    
    def connect(self) -> bool:
        """连接 QMT"""
        return self.client.connect()
    
    def sync_sector_list(self) -> dict:
        """同步板块列表"""
        logger.info("开始同步板块列表")
        
        try:
            df = self.client.get_sector_list()
            count = self.db.save_sector_list(df)
            self.db.save_sync_record("sector_list", True, count)
            return {"success": True, "record_count": count}
        except Exception as e:
            logger.error(f"同步板块列表失败: {e}")
            self.db.save_sync_record("sector_list", False, 0, str(e))
            return {"success": False, "error": str(e)}
    
    def sync_sector_stocks(self, sectors: Optional[List[str]] = None) -> dict:
        """同步板块股票"""
        if sectors is None:
            sectors = self.common_sectors
        
        logger.info(f"开始同步板块股票: {sectors}")
        
        total_count = 0
        errors = []
        
        for sector in sectors:
            try:
                df = self.client.get_stock_list_in_sector(sector)
                count = self.db.save_sector_stocks(df)
                total_count += count
            except Exception as e:
                errors.append(f"{sector}: {e}")
        
        if errors:
            self.db.save_sync_record("sector_stocks", False, total_count, "; ".join(errors))
            return {"success": False, "record_count": total_count, "errors": errors}
        else:
            self.db.save_sync_record("sector_stocks", True, total_count)
            return {"success": True, "record_count": total_count}
    
    def sync_stock_list(self) -> dict:
        """同步股票列表"""
        logger.info("开始同步股票列表")
        
        try:
            df = self.client.get_stock_list()
            count = self.db.save_stock_list(df)
            self.db.save_sync_record("stock_list", True, count)
            return {"success": True, "record_count": count}
        except Exception as e:
            logger.error(f"同步股票列表失败: {e}")
            self.db.save_sync_record("stock_list", False, 0, str(e))
            return {"success": False, "error": str(e)}
    
    def sync_etf_list(self) -> dict:
        """同步ETF列表"""
        logger.info("开始同步ETF列表")
        
        try:
            df = self.client.get_etf_list()
            count = self.db.save_etf_list(df)
            self.db.save_sync_record("etf_list", True, count)
            return {"success": True, "record_count": count}
        except Exception as e:
            logger.error(f"同步ETF列表失败: {e}")
            self.db.save_sync_record("etf_list", False, 0, str(e))
            return {"success": False, "error": str(e)}
    
    def sync_index_weight(self, indexes: Optional[List[str]] = None) -> dict:
        """同步指数成分股权重"""
        if indexes is None:
            indexes = self.common_indexes
        
        logger.info(f"开始同步指数成分股: {indexes}")
        
        total_count = 0
        errors = []
        
        for index in indexes:
            try:
                df = self.client.get_index_weight(index)
                count = self.db.save_index_weight(df, index)
                total_count += count
            except Exception as e:
                errors.append(f"{index}: {e}")
        
        if errors:
            self.db.save_sync_record("index_weight", False, total_count, "; ".join(errors))
            return {"success": False, "record_count": total_count, "errors": errors}
        else:
            self.db.save_sync_record("index_weight", True, total_count)
            return {"success": True, "record_count": total_count}
    
    def sync_kline(self, sec_codes: List[str], days: int = 30) -> dict:
        """同步K线数据"""
        logger.info(f"开始同步K线: {sec_codes}, 近{days}天")
        
        end_date = datetime.now().strftime("%Y%m%d")
        start_date = (datetime.now() - timedelta(days=days)).strftime("%Y%m%d")
        
        total_count = 0
        errors = []
        
        for code in sec_codes:
            try:
                df = self.client.get_full_kline(code, start_date, end_date, period="1d")
                count = self.db.save_kline(df, period="1d")
                total_count += count
            except Exception as e:
                errors.append(f"{code}: {e}")
        
        if errors:
            self.db.save_sync_record("kline", False, total_count, "; ".join(errors))
            return {"success": False, "record_count": total_count, "errors": errors}
        else:
            self.db.save_sync_record("kline", True, total_count)
            return {"success": True, "record_count": total_count}
    
    def sync_realtime_quote(self, sec_codes: List[str]) -> dict:
        """同步实时行情"""
        logger.info(f"开始同步实时行情: {sec_codes}")
        
        try:
            df = self.client.get_realtime_quote(sec_codes)
            count = self.db.save_realtime_quote(df)
            self.db.save_sync_record("realtime_quote", True, count)
            return {"success": True, "record_count": count}
        except Exception as e:
            logger.error(f"同步实时行情失败: {e}")
            self.db.save_sync_record("realtime_quote", False, 0, str(e))
            return {"success": False, "error": str(e)}
    
    def sync_all(self) -> List[dict]:
        """执行全量同步"""
        results = []
        
        results.append({"task": "sector_list", **self.sync_sector_list()})
        results.append({"task": "sector_stocks", **self.sync_sector_stocks()})
        results.append({"task": "stock_list", **self.sync_stock_list()})
        results.append({"task": "etf_list", **self.sync_etf_list()})
        results.append({"task": "index_weight", **self.sync_index_weight()})
        
        return results
    
    def get_sync_status(self) -> dict:
        """获取同步状态"""
        tables = self.db.get_tables()
        status = {}
        
        for table in tables:
            count = self.db.get_table_count(table)
            status[table] = count
        
        return status
    
    def close(self):
        """关闭连接"""
        from src.qmt.client import close_qmt_client
        from src.qmt.database import close_qmt_db
        close_qmt_client()
        close_qmt_db()


# 全局调度器实例
_qmt_scheduler_instance: Optional[QMTScheduler] = None


def get_qmt_scheduler(qmt_path: Optional[str] = None,
                     account_id: Optional[str] = None,
                     db_path: str = "./data/qmt_data.duckdb") -> QMTScheduler:
    """获取 QMT 调度器实例"""
    global _qmt_scheduler_instance
    if _qmt_scheduler_instance is None:
        _qmt_scheduler_instance = QMTScheduler(qmt_path, account_id, db_path)
    return _qmt_scheduler_instance
