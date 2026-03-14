"""
定时任务模块
"""
import time
import threading
from datetime import datetime, timedelta
from typing import Optional, Dict, Any, List, Callable
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.interval import IntervalTrigger
from apscheduler.triggers.cron import CronTrigger

from src.config import config
from src.logger import logger
from src.client import get_client, AmazingDataClient
from src.database import get_db, DuckDBManager
from src.retry import retry_manager
from src.models import DataSource


class DataFetcher:
    """数据获取器"""
    
    def __init__(self, client: Optional[AmazingDataClient] = None,
                 db: Optional[DuckDBManager] = None):
        self.client = client or get_client()
        self.db = db or get_db()
    
    def fetch_and_save(self, data_type: DataSource, 
                       table_name: Optional[str] = None,
                       **kwargs) -> Dict[str, Any]:
        """
        获取并保存数据
        
        Args:
            data_type: 数据类型
            table_name: 表名（默认使用 data_type）
            **kwargs: 其他参数
            
        Returns:
            结果字典
        """
        if table_name is None:
            table_name = data_type.value
        
        try:
            # 确保连接
            if not self.client.is_connected:
                self.client.login()
            
            # 获取数据
            logger.info(f"开始获取数据: {data_type}")
            df = self.client.fetch_data(data_type, **kwargs)
            
            if df.empty:
                logger.warning(f"数据为空: {data_type}")
                self.db.save_fetch_record(
                    data_type.value, 
                    success=True, 
                    record_count=0
                )
                logger.log_fetch(data_type.value, True, 0)
                return {"success": True, "record_count": 0}
            
            # 保存到数据库
            self.db.insert_dataframe(df, table_name)
            
            # 记录成功
            self.db.save_fetch_record(
                data_type.value,
                success=True,
                record_count=len(df),
                start_date=kwargs.get("start_date"),
                end_date=kwargs.get("end_date")
            )
            logger.log_fetch(data_type.value, True, len(df))
            
            return {"success": True, "record_count": len(df)}
            
        except Exception as e:
            error_msg = str(e)
            logger.error(f"获取数据失败 {data_type}: {error_msg}")
            
            # 记录失败
            self.db.save_fetch_record(
                data_type.value,
                success=False,
                error_message=error_msg
            )
            logger.log_fetch(data_type.value, False, 0, error_msg)
            
            # 添加到重试队列
            retry_manager.add_failed_task(
                f"{data_type.value}_{datetime.now().timestamp()}",
                {
                    "data_type": data_type.value,
                    "func": "fetch_and_save",
                    "args": (data_type, table_name),
                    "kwargs": kwargs,
                    "error": error_msg,
                    "max_attempts": config.scheduler.retry_times
                }
            )
            
            return {"success": False, "error": error_msg}
    
    def incremental_fetch(self, data_type: DataSource,
                          table_name: Optional[str] = None,
                          date_column: str = "trade_date",
                          **kwargs) -> Dict[str, Any]:
        """
        增量获取数据
        
        Args:
            data_type: 数据类型
            table_name: 表名
            date_column: 日期列名
            **kwargs: 其他参数
            
        Returns:
            结果字典
        """
        if table_name is None:
            table_name = data_type.value
        
        try:
            # 获取最新日期
            latest_date = self.db.get_latest_date(table_name, date_column)
            
            # 设置日期范围
            if latest_date:
                kwargs["start_date"] = latest_date
                logger.info(f"增量更新 {table_name}, 从 {latest_date} 开始")
            else:
                # 首次获取，获取过去30天数据
                start_date = (datetime.now() - timedelta(days=30)).strftime("%Y%m%d")
                kwargs["start_date"] = start_date
                logger.info(f"首次获取 {table_name}, 从 {start_date} 开始")
            
            # 获取数据
            return self.fetch_and_save(data_type, table_name, **kwargs)
            
        except Exception as e:
            return {"success": False, "error": str(e)}


class Scheduler:
    """定时任务调度器"""
    
    def __init__(self):
        self.scheduler = BackgroundScheduler()
        self.fetcher = DataFetcher()
        self._running = False
    
    def start(self):
        """启动调度器"""
        if self._running:
            logger.warning("调度器已在运行")
            return
        
        if not config.scheduler.enabled:
            logger.info("调度器未启用")
            return
        
        # 添加定时任务
        self._add_jobs()
        
        self.scheduler.start()
        self._running = True
        logger.info("调度器已启动")
    
    def stop(self):
        """停止调度器"""
        if not self._running:
            return
        
        self.scheduler.shutdown()
        self._running = False
        logger.info("调度器已停止")
    
    def _add_jobs(self):
        """添加定时任务"""
        
        # 每小时增量更新实时行情
        self.scheduler.add_job(
            self.sync_realtime_data,
            trigger=IntervalTrigger(hours=config.scheduler.interval_hours),
            id="sync_realtime",
            name="同步实时行情数据",
            replace_existing=True
        )
        
        # 每日收盘后更新历史行情
        self.scheduler.add_job(
            self.sync_historical_data,
            trigger=CronTrigger(hour=16, minute=0),
            id="sync_historical",
            name="同步历史行情数据",
            replace_existing=True
        )
        
        # 每日收盘后更新财务数据
        self.scheduler.add_job(
            self.sync_financial_data,
            trigger=CronTrigger(hour=17, minute=0),
            id="sync_financial",
            name="同步财务数据",
            replace_existing=True
        )
        
        # 每日更新基础数据
        self.scheduler.add_job(
            self.sync_basic_data,
            trigger=CronTrigger(hour=6, minute=0),
            id="sync_basic",
            name="同步基础数据",
            replace_existing=True
        )
        
        # 每日更新股东数据
        self.scheduler.add_job(
            self.sync_holder_data,
            trigger=CronTrigger(hour=18, minute=0),
            id="sync_holder",
            name="同步股东数据",
            replace_existing=True
        )
        
        logger.info("定时任务已添加")
    
    def sync_realtime_data(self):
        """同步实时行情数据"""
        logger.info("开始同步实时行情数据")
        
        data_types = [
            (DataSource.INDEX_SNAPSHOT, "index_snapshot"),
            (DataSource.STOCK_SNAPSHOT, "stock_snapshot"),
            (DataSource.ETF_SNAPSHOT, "etf_snapshot"),
            (DataSource.CB_SNAPSHOT, "cb_snapshot"),
        ]
        
        results = []
        for data_type, table_name in data_types:
            result = self.fetcher.fetch_and_save(data_type, table_name)
            results.append({"data_type": data_type.value, **result})
        
        return results
    
    def sync_historical_data(self):
        """同步历史行情数据"""
        logger.info("开始同步历史行情数据")
        
        # 获取交易日历
        calendar = self.fetcher.client.get_trading_calendar(
            (datetime.now() - timedelta(days=30)).strftime("%Y%m%d"),
            datetime.now().strftime("%Y%m%d")
        )
        
        # 获取所有股票代码
        sec_codes = self.fetcher.db.query("SELECT DISTINCT sec_code FROM stock_snapshot LIMIT 100")
        
        results = []
        for _, row in sec_codes.iterrows():
            sec_code = row["sec_code"]
            try:
                result = self.fetcher.fetch_and_save(
                    DataSource.HISTORICAL_KLINE,
                    table_name=f"kline_1D_{sec_code}",
                    sec_code=sec_code,
                    kline_type="1D",
                    count=100
                )
                results.append({"sec_code": sec_code, **result})
            except Exception as e:
                logger.error(f"获取 {sec_code} K线失败: {e}")
        
        return results
    
    def sync_financial_data(self):
        """同步财务数据"""
        logger.info("开始同步财务数据")
        
        data_types = [
            DataSource.BALANCE_SHEET,
            DataSource.CASH_FLOW,
            DataSource.INCOME,
            DataSource.EXPRESS_REPORT,
            DataSource.FORECAST_REPORT,
        ]
        
        results = []
        end_date = datetime.now().strftime("%Y%m%d")
        start_date = (datetime.now() - timedelta(days=90)).strftime("%Y%m%d")
        
        # 获取所有股票代码
        sec_codes = self.fetcher.db.query("SELECT DISTINCT sec_code FROM stock_snapshot LIMIT 50")
        
        for data_type in data_types:
            for _, row in sec_codes.iterrows():
                sec_code = row["sec_code"]
                try:
                    result = self.fetcher.fetch_and_save(
                        data_type,
                        sec_code=sec_code,
                        start_date=start_date,
                        end_date=end_date
                    )
                    results.append({"data_type": data_type.value, "sec_code": sec_code, **result})
                except Exception as e:
                    logger.error(f"获取 {sec_code} {data_type} 失败: {e}")
        
        return results
    
    def sync_basic_data(self):
        """同步基础数据"""
        logger.info("开始同步基础数据")
        
        data_types = [
            (DataSource.SECURITY_INFO, "security_info"),
            (DataSource.SECURITY_CODE, "security_code"),
            (DataSource.FUTURES_CODE, "futures_code"),
            (DataSource.SECURITY_BASIC, "security_basic"),
            (DataSource.TRADING_CALENDAR, "trading_calendar"),
        ]
        
        results = []
        for data_type, table_name in data_types:
            result = self.fetcher.fetch_and_save(data_type, table_name)
            results.append({"data_type": data_type.value, **result})
        
        return results
    
    def sync_holder_data(self):
        """同步股东数据"""
        logger.info("开始同步股东数据")
        
        data_types = [
            DataSource.TOP10_HOLDERS,
            DataSource.SHAREHOLDER_COUNT,
            DataSource.SHARE_STRUCTURE,
        ]
        
        results = []
        end_date = datetime.now().strftime("%Y%m%d")
        start_date = (datetime.now() - timedelta(days=365)).strftime("%Y%m%d")
        
        # 获取所有股票代码
        sec_codes = self.fetcher.db.query("SELECT DISTINCT sec_code FROM stock_snapshot LIMIT 50")
        
        for data_type in data_types:
            for _, row in sec_codes.iterrows():
                sec_code = row["sec_code"]
                try:
                    result = self.fetcher.fetch_and_save(
                        data_type,
                        sec_code=sec_code,
                        start_date=start_date,
                        end_date=end_date
                    )
                    results.append({"data_type": data_type.value, "sec_code": sec_code, **result})
                except Exception as e:
                    logger.error(f"获取 {sec_code} {data_type} 失败: {e}")
        
        return results
    
    def trigger_sync(self, sync_type: str) -> Dict[str, Any]:
        """
        手动触发同步
        
        Args:
            sync_type: 同步类型 (realtime, historical, financial, basic, holder)
        """
        sync_methods = {
            "realtime": self.sync_realtime_data,
            "historical": self.sync_historical_data,
            "financial": self.sync_financial_data,
            "basic": self.sync_basic_data,
            "holder": self.sync_holder_data,
        }
        
        method = sync_methods.get(sync_type)
        if method is None:
            return {"success": False, "error": f"未知的同步类型: {sync_type}"}
        
        try:
            result = method()
            return {"success": True, "result": result}
        except Exception as e:
            logger.error(f"同步失败: {e}")
            return {"success": False, "error": str(e)}


# 全局调度器实例
_scheduler: Optional[Scheduler] = None


def get_scheduler() -> Scheduler:
    """获取调度器实例"""
    global _scheduler
    if _scheduler is None:
        _scheduler = Scheduler()
    return _scheduler


def start_scheduler():
    """启动调度器"""
    scheduler = get_scheduler()
    scheduler.start()


def stop_scheduler():
    """停止调度器"""
    global _scheduler
    if _scheduler:
        _scheduler.stop()
        _scheduler = None
