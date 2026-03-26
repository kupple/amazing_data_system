"""
Starlight (AmazingData) 定时任务调度器
实现增量同步到 starlight 数据库
"""
import time
from datetime import datetime, timedelta
from typing import Optional, Dict, Any, List
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.interval import IntervalTrigger
from apscheduler.triggers.cron import CronTrigger
import pandas as pd

from src.common.config import config
from src.common.logger import logger
from src.collectors.starlight.client import get_client, AmazingDataClient
from src.collectors.starlight.sync_shared import StarlightSyncSupport
from src.common.database import get_db, ClickHouseManager


class StarlightScheduler(StarlightSyncSupport):
    """Starlight 数据同步调度器"""
    
    def __init__(self):
        self.scheduler = BackgroundScheduler()
        self.client: Optional[AmazingDataClient] = None
        self.db: Optional[ClickHouseManager] = None
        self._running = False
    
    def _ensure_connections(self):
        """确保连接"""
        if self.client is None:
            self.client = get_client()
            if not self.client.is_connected:
                self.client.connect()
        
        if self.db is None:
            # 使用 starlight 数据源的数据库
            self.db = get_db("starlight")

    
    def start(self):
        """启动调度器"""
        if self._running:
            logger.warning("Starlight 调度器已在运行")
            return
        
        if not config.scheduler.enabled:
            logger.info("调度器未启用")
            return
        
        self._ensure_connections()
        self._add_jobs()
        
        self.scheduler.start()
        self._running = True
        logger.info("Starlight 调度器已启动")
    
    def stop(self):
        """停止调度器"""
        if not self._running:
            return
        
        self.scheduler.shutdown()
        self._running = False
        logger.info("Starlight 调度器已停止")
    
    def _add_jobs(self):
        """添加定时任务"""
        
        # 每日早上 6:00 更新基础数据
        self.scheduler.add_job(
            self.sync_basic_data,
            trigger=CronTrigger(hour=6, minute=0),
            id="starlight_sync_basic",
            name="同步基础数据（代码表、日历等）",
            replace_existing=True
        )
        
        # 每日收盘后 16:30 更新行情数据
        self.scheduler.add_job(
            self.sync_market_data,
            trigger=CronTrigger(hour=16, minute=30),
            id="starlight_sync_market",
            name="同步历史行情数据",
            replace_existing=True
        )
        
        # 每日晚上 18:00 更新财务数据
        self.scheduler.add_job(
            self.sync_financial_data,
            trigger=CronTrigger(hour=18, minute=0),
            id="starlight_sync_financial",
            name="同步财务数据",
            replace_existing=True
        )
        
        # 每日晚上 19:00 更新股东数据
        self.scheduler.add_job(
            self.sync_holder_data,
            trigger=CronTrigger(hour=19, minute=0),
            id="starlight_sync_holder",
            name="同步股东数据",
            replace_existing=True
        )
        
        # 每日晚上 20:00 更新其他数据
        self.scheduler.add_job(
            self.sync_other_data,
            trigger=CronTrigger(hour=20, minute=0),
            id="starlight_sync_other",
            name="同步其他数据（融资融券、龙虎榜等）",
            replace_existing=True
        )
        
        logger.info("Starlight 定时任务已添加")
    
    # ========== 基础数据同步 ==========
    
    def sync_basic_data(self) -> Dict[str, Any]:
        """同步基础数据（每日更新）"""
        logger.info("开始同步基础数据")
        self._ensure_connections()
        
        results = []
        
        # 1. 股票代码表（全量替换）
        try:
            code_list = self._call_client_method(self.client.get_code_list, security_type="EXTRA_STOCK_A")
            df = pd.DataFrame({"code": code_list})
            df["update_time"] = datetime.now()
            
            # 全量替换
            self.db.execute(f"DROP TABLE IF EXISTS stock_codes")
            self.db.insert_dataframe(df, "stock_codes")
            self._mark_table_success("stock_codes")
            
            logger.info(f"股票代码表已更新，共 {len(df)} 条")
            results.append({"type": "stock_codes", "success": True, "count": len(df)})
        except Exception as e:
            self._mark_table_failed("stock_codes", e)
            logger.error(f"同步股票代码表失败: {e}")
            results.append({"type": "stock_codes", "success": False, "error": str(e)})
        
        # 2. 交易日历（增量）
        try:
            calendar = self._call_client_method(self.client.get_calendar)
            df = pd.DataFrame({"trade_date": calendar})
            df["trade_date"] = pd.to_datetime(df["trade_date"])
            
            # 增量插入
            self.db.incremental_update(
                "trading_calendar",
                df,
                key_columns=["trade_date"],
                date_column="trade_date"
            )
            self._mark_table_success("trading_calendar", date_column="trade_date")
            
            logger.info(f"交易日历已更新")
            results.append({"type": "trading_calendar", "success": True})
        except Exception as e:
            self._mark_table_failed("trading_calendar", e, date_column="trade_date")
            logger.error(f"同步交易日历失败: {e}")
            results.append({"type": "trading_calendar", "success": False, "error": str(e)})
        
        # 3. 证券基础信息（全量替换）
        try:
            code_list = self._call_client_method(self.client.get_code_list, security_type="EXTRA_STOCK_A")
            self.db.execute(f"DROP TABLE IF EXISTS stock_basic")
            total_rows = 0
            for batch_index, _, stock_basic in self._iter_code_batch_results(
                self.client.get_stock_basic,
                code_list,
                batch_size=50,
                sleep_seconds=0.02,
                checkpoint_key="scheduler.basic.stock_basic",
            ):
                if isinstance(stock_basic, pd.DataFrame) and not stock_basic.empty:
                    stock_basic = self._lowercase_columns(stock_basic)
                    self.db.insert_dataframe(stock_basic, "stock_basic")
                    total_rows += len(stock_basic)
                self._set_checkpoint("scheduler.basic.stock_basic", batch_index + 1)
            if total_rows:
                logger.info(f"证券基础信息已更新，共 {total_rows} 条")
                results.append({"type": "stock_basic", "success": True, "count": total_rows})
            self._mark_table_success("stock_basic")
            self._clear_checkpoint("scheduler.basic.stock_basic")
        except Exception as e:
            self._mark_table_failed("stock_basic", e)
            logger.error(f"同步证券基础信息失败: {e}")
            results.append({"type": "stock_basic", "success": False, "error": str(e)})
        
        # 4. 复权因子（增量）
        try:
            code_list = self._call_client_method(self.client.get_code_list, security_type="EXTRA_STOCK_A")
            
            # 后复权因子
            factor_frames = []
            for batch_index, batch_codes in self._iter_batches(
                code_list,
                batch_size=50,
                checkpoint_key="scheduler.basic.backward_factor",
            ):
                backward_factor = self.client.get_backward_factor(batch_codes, is_local=False)
                reshaped = self._reshape_factor_dataframe(backward_factor, "backward_factor")
                if not reshaped.empty:
                    factor_frames.append(reshaped)
                time.sleep(0.02)
                self._set_checkpoint("scheduler.basic.backward_factor", batch_index + 1)
            backward_factor = pd.concat(factor_frames, ignore_index=True) if factor_frames else pd.DataFrame()
            if not backward_factor.empty:
                self.db.incremental_update(
                    "backward_factor",
                    backward_factor,
                    key_columns=["code", "date"],
                    date_column="date"
                )
                logger.info(f"后复权因子已更新，共 {len(backward_factor)} 条")
                results.append({"type": "backward_factor", "success": True, "count": len(backward_factor)})
            self._mark_table_success("backward_factor", date_column="date")
            self._clear_checkpoint("scheduler.basic.backward_factor")
            
            # 前复权因子
            factor_frames = []
            for batch_index, batch_codes in self._iter_batches(
                code_list,
                batch_size=50,
                checkpoint_key="scheduler.basic.adj_factor",
            ):
                adj_factor = self.client.get_adj_factor(batch_codes, is_local=False)
                reshaped = self._reshape_factor_dataframe(adj_factor, "adj_factor")
                if not reshaped.empty:
                    factor_frames.append(reshaped)
                time.sleep(0.02)
                self._set_checkpoint("scheduler.basic.adj_factor", batch_index + 1)
            adj_factor = pd.concat(factor_frames, ignore_index=True) if factor_frames else pd.DataFrame()
            if not adj_factor.empty:
                self.db.incremental_update(
                    "adj_factor",
                    adj_factor,
                    key_columns=["code", "date"],
                    date_column="date"
                )
                logger.info(f"前复权因子已更新，共 {len(adj_factor)} 条")
                results.append({"type": "adj_factor", "success": True, "count": len(adj_factor)})
            self._mark_table_success("adj_factor", date_column="date")
            self._clear_checkpoint("scheduler.basic.adj_factor")
                
        except Exception as e:
            self._mark_table_failed("backward_factor", e, date_column="date")
            logger.error(f"同步复权因子失败: {e}")
            results.append({"type": "adj_factor", "success": False, "error": str(e)})
        
        return {"task": "sync_basic_data", "results": results}
    
    # ========== 行情数据同步 ==========
    
    def sync_market_data(self) -> Dict[str, Any]:
        """同步历史行情数据（智能增量）"""
        logger.info("开始同步历史行情数据")
        self._ensure_connections()
        
        results = []
        
        # 获取股票列表
        try:
            code_list = self._call_client_method(self.client.get_code_list, security_type="EXTRA_STOCK_A")
            logger.info(f"获取到 {len(code_list)} 只股票")
        except Exception as e:
            logger.error(f"获取股票列表失败: {e}")
            return {"task": "sync_market_data", "error": str(e)}
        
        # 智能判断时间范围
        # 检查K线表是否存在
        is_first_sync = not self.db.table_exists("kline_daily")
        
        end_date = datetime.now()
        if is_first_sync:
            # 首次同步：获取全部历史（从2010年开始）
            start_date = datetime(2010, 1, 1)
            logger.info("⚠ 检测到首次同步，将获取全部历史数据（从2010年开始）")
        else:
            # 增量同步：查询数据库中最新的日期
            latest_date = None
            for column in ["kline_time", "trade_time", "time"]:
                try:
                    latest_date = self.db.get_latest_date("kline_daily", column)
                except Exception:
                    latest_date = None
                if latest_date:
                    break
            
            if latest_date:
                # 从最新日期开始同步（往前推1天以防遗漏）
                start_date = datetime.strptime(latest_date[:10], "%Y-%m-%d") - timedelta(days=1)
                logger.info(f"✓ 增量同步模式，从最新日期 {latest_date[:10]} 开始")
            else:
                # 如果查不到最新日期，默认获取最近30天
                start_date = end_date - timedelta(days=30)
                logger.info("⚠ 未找到最新日期，默认获取最近30天数据")
        
        begin_date_int = int(start_date.strftime("%Y%m%d"))
        end_date_int = int(end_date.strftime("%Y%m%d"))
        
        logger.info(f"时间范围: {begin_date_int} - {end_date_int}")
        
        # 分批处理，每次50只股票（同步所有股票）
        batch_size = 50
        completed = True
        for batch_index, batch_codes in self._iter_batches(
            code_list,
            batch_size=batch_size,
            checkpoint_key="scheduler.market.kline",
        ):
            
            try:
                # 获取日K线
                kline_dict = self._call_client_method(
                    self.client.query_kline,
                    code_list=batch_codes,
                    begin_date=begin_date_int,
                    end_date=end_date_int
                )
                
                # 保存到统一的K线表
                for code, df in kline_dict.items():
                    if not df.empty:
                        # 添加 code 列
                        df = self._lowercase_columns(df)
                        if "kline_time" in df.columns and "trade_time" not in df.columns:
                            df = df.rename(columns={"kline_time": "trade_time"})
                        df['code'] = code
                        time_column = self._find_existing_column(df, ["trade_time", "kline_time", "time"])
                        
                        # 增量插入到统一表
                        self.db.incremental_update(
                            "kline_daily",
                            df,
                            key_columns=["code", time_column],
                            date_column=time_column
                        )
                
                logger.info(f"已同步 {(batch_index + 1) * batch_size}/{len(code_list)} 只股票的K线数据")
                results.append({"batch": batch_index, "success": True, "count": len(batch_codes)})
                self._set_checkpoint("scheduler.market.kline", batch_index + 1)
                
            except Exception as e:
                logger.error(f"同步第 {batch_index} 批K线数据失败: {e}")
                results.append({"batch": batch_index, "success": False, "error": str(e)})
                completed = False
                break
            
            # 避免请求过快
            time.sleep(1)
        
        if completed:
            self._mark_table_success("kline_daily", date_column="trade_time")
            self._clear_checkpoint("scheduler.market.kline")
        else:
            self._mark_table_failed("kline_daily", Exception("K线同步未完成"), date_column="trade_time")
        return {"task": "sync_market_data", "results": results}
    
    # ========== 财务数据同步 ==========
    
    def sync_financial_data(self) -> Dict[str, Any]:
        """同步财务数据（智能增量）"""
        logger.info("开始同步财务数据")
        self._ensure_connections()
        
        results = []
        
        # 获取股票列表
        try:
            code_list = self._call_client_method(self.client.get_code_list, security_type="EXTRA_STOCK_A")
        except Exception as e:
            logger.error(f"获取股票列表失败: {e}")
            return {"task": "sync_financial_data", "error": str(e)}
        
        # 检查是否首次同步
        is_first_sync = not self.db.table_exists("balance_sheet")
        
        if is_first_sync:
            logger.info("⚠ 检测到首次同步，将获取全部历史财务数据")
        else:
            logger.info("✓ 增量同步模式，使用本地缓存加速")
        
        # 财务数据类型
        financial_types = [
            ("balance_sheet", "资产负债表"),
            ("cash_flow", "现金流量表"),
            ("income", "利润表"),
            ("profit_express", "业绩快报"),
            ("profit_notice", "业绩预告"),
        ]
        
        for data_type, name in financial_types:
            try:
                # 获取方法
                method = getattr(self.client, f"get_{data_type}")
                
                # 首次同步强制从服务器获取，后续使用本地缓存
                total_rows = 0
                for batch_index, _, data in self._iter_code_batch_results(
                    method,
                    code_list,
                    batch_size=20,
                    sleep_seconds=0.02,
                    checkpoint_key=f"scheduler.financial.{data_type}",
                    is_local=(not is_first_sync),
                ):
                    if isinstance(data, dict):
                        all_data = []
                        for code, df in data.items():
                            if not df.empty:
                                df['market_code'] = code
                                all_data.append(df)
                        if all_data:
                            merged_df = pd.concat(all_data, ignore_index=True)
                            merged_df.columns = [col.lower() for col in merged_df.columns]
                            self.db.incremental_update(
                                data_type,
                                merged_df,
                                key_columns=["market_code", "reporting_period"],
                                date_column="reporting_period"
                            )
                            total_rows += len(merged_df)
                    elif isinstance(data, pd.DataFrame) and not data.empty:
                        data.columns = [col.lower() for col in data.columns]
                        self.db.incremental_update(
                            data_type,
                            data,
                            key_columns=["market_code", "reporting_period"],
                            date_column="reporting_period"
                        )
                        total_rows += len(data)
                    self._set_checkpoint(f"scheduler.financial.{data_type}", batch_index + 1)
                if total_rows:
                    logger.info(f"{name}已更新，共 {total_rows} 条")
                    results.append({"type": data_type, "success": True, "count": total_rows})
                self._mark_table_success(data_type, date_column="reporting_period")
                self._clear_checkpoint(f"scheduler.financial.{data_type}")
                
            except Exception as e:
                self._mark_table_failed(data_type, e, date_column="reporting_period")
                logger.error(f"同步{name}失败: {e}")
                results.append({"type": data_type, "success": False, "error": str(e)})
        
        return {"task": "sync_financial_data", "results": results}
    
    # ========== 股东数据同步 ==========
    
    def sync_holder_data(self) -> Dict[str, Any]:
        """同步股东数据（智能增量）"""
        logger.info("开始同步股东数据")
        self._ensure_connections()
        
        results = []
        
        # 获取股票列表
        try:
            code_list = self._call_client_method(self.client.get_code_list, security_type="EXTRA_STOCK_A")
        except Exception as e:
            logger.error(f"获取股票列表失败: {e}")
            return {"task": "sync_holder_data", "error": str(e)}
        
        # 检查是否首次同步
        is_first_sync = not self.db.table_exists("share_holder")
        
        if is_first_sync:
            logger.info("⚠ 检测到首次同步，将获取全部历史股东数据")
        else:
            logger.info("✓ 增量同步模式，使用本地缓存加速")
        
        # 股东数据类型
        holder_types = [
            ("share_holder", "十大股东", ["market_code", "holder_enddate", "qty_num"]),
            ("holder_num", "股东户数", ["market_code", "holder_enddate"]),
            ("equity_structure", "股本结构", ["market_code", "change_date"]),
        ]
        
        for data_type, name, key_columns in holder_types:
            try:
                method = getattr(self.client, f"get_{data_type}")
                total_rows = 0
                for batch_index, _, data in self._iter_code_batch_results(
                    method,
                    code_list,
                    batch_size=20,
                    sleep_seconds=0.02,
                    checkpoint_key=f"scheduler.holder.{data_type}",
                    is_local=(not is_first_sync),
                ):
                    if isinstance(data, pd.DataFrame) and not data.empty:
                        data.columns = [col.lower() for col in data.columns]
                        self.db.incremental_update(
                            data_type,
                            data,
                            key_columns=key_columns
                        )
                        total_rows += len(data)
                    self._set_checkpoint(f"scheduler.holder.{data_type}", batch_index + 1)
                if total_rows:
                    logger.info(f"{name}已更新，共 {total_rows} 条")
                    results.append({"type": data_type, "success": True, "count": total_rows})
                self._mark_table_success(data_type)
                self._clear_checkpoint(f"scheduler.holder.{data_type}")
                
            except Exception as e:
                self._mark_table_failed(data_type, e)
                logger.error(f"同步{name}失败: {e}")
                results.append({"type": data_type, "success": False, "error": str(e)})
        
        return {"task": "sync_holder_data", "results": results}
    
    # ========== 其他数据同步 ==========
    
    def sync_other_data(self) -> Dict[str, Any]:
        """同步其他数据（融资融券、龙虎榜等）"""
        logger.info("开始同步其他数据")
        self._ensure_connections()
        
        results = []
        
        # 获取股票列表
        try:
            code_list = self.client.get_code_list("EXTRA_STOCK_A")
        except Exception as e:
            logger.error(f"获取股票列表失败: {e}")
            return {"task": "sync_other_data", "error": str(e)}
        
        # 检查是否首次同步
        is_first_sync = not self.db.table_exists("margin_summary")
        
        if is_first_sync:
            logger.info("⚠ 检测到首次同步，将获取全部历史数据")
        else:
            logger.info("✓ 增量同步模式，使用本地缓存加速")
        
        # 1. 融资融券汇总
        try:
            margin_summary = self._call_client_method(self.client.get_margin_summary, is_local=(not is_first_sync))
            if not margin_summary.empty:
                # 重命名列为小写
                margin_summary.columns = [col.lower() for col in margin_summary.columns]
                
                self.db.incremental_update(
                    "margin_summary",
                    margin_summary,
                    key_columns=["trade_date"],
                    date_column="trade_date"
                )
                logger.info(f"融资融券汇总已更新，共 {len(margin_summary)} 条")
                results.append({"type": "margin_summary", "success": True, "count": len(margin_summary)})
            self._mark_table_success("margin_summary", date_column="trade_date")
        except Exception as e:
            self._mark_table_failed("margin_summary", e, date_column="trade_date")
            logger.error(f"同步融资融券汇总失败: {e}")
            results.append({"type": "margin_summary", "success": False, "error": str(e)})
        
        # 2. 龙虎榜
        try:
            total_rows = 0
            for batch_index, _, dragon_tiger in self._iter_code_batch_results(
                self.client.get_long_hu_bang,
                code_list,
                batch_size=20,
                sleep_seconds=0.02,
                checkpoint_key="scheduler.other.dragon_tiger",
                is_local=(not is_first_sync),
            ):
                if isinstance(dragon_tiger, pd.DataFrame) and not dragon_tiger.empty:
                    dragon_tiger.columns = [col.lower() for col in dragon_tiger.columns]
                    self.db.incremental_update(
                        "dragon_tiger",
                        dragon_tiger,
                        key_columns=["market_code", "trade_date", "trader_name"],
                        date_column="trade_date"
                    )
                    total_rows += len(dragon_tiger)
                self._set_checkpoint("scheduler.other.dragon_tiger", batch_index + 1)
            if total_rows:
                logger.info(f"龙虎榜已更新，共 {total_rows} 条")
                results.append({"type": "dragon_tiger", "success": True, "count": total_rows})
            self._mark_table_success("dragon_tiger", date_column="trade_date")
            self._clear_checkpoint("scheduler.other.dragon_tiger")
        except Exception as e:
            self._mark_table_failed("dragon_tiger", e, date_column="trade_date")
            logger.error(f"同步龙虎榜失败: {e}")
            results.append({"type": "dragon_tiger", "success": False, "error": str(e)})
        
        # 3. 大宗交易
        try:
            total_rows = 0
            for batch_index, _, block_trade in self._iter_code_batch_results(
                self.client.get_block_trading,
                code_list,
                batch_size=20,
                sleep_seconds=0.02,
                checkpoint_key="scheduler.other.block_trade",
                is_local=(not is_first_sync),
            ):
                if isinstance(block_trade, pd.DataFrame) and not block_trade.empty:
                    block_trade.columns = [col.lower() for col in block_trade.columns]
                    self.db.incremental_update(
                        "block_trade",
                        block_trade,
                        key_columns=["market_code", "trade_date"],
                        date_column="trade_date"
                    )
                    total_rows += len(block_trade)
                self._set_checkpoint("scheduler.other.block_trade", batch_index + 1)
            if total_rows:
                logger.info(f"大宗交易已更新，共 {total_rows} 条")
                results.append({"type": "block_trade", "success": True, "count": total_rows})
            self._mark_table_success("block_trade", date_column="trade_date")
            self._clear_checkpoint("scheduler.other.block_trade")
        except Exception as e:
            self._mark_table_failed("block_trade", e, date_column="trade_date")
            logger.error(f"同步大宗交易失败: {e}")
            results.append({"type": "block_trade", "success": False, "error": str(e)})
        
        return {"task": "sync_other_data", "results": results}
    
    # ========== 手动触发 ==========
    
    def trigger_sync(self, sync_type: str) -> Dict[str, Any]:
        """
        手动触发同步
        
        Args:
            sync_type: 同步类型 ('basic', 'market', 'financial', 'holder', 'other', 'all')
        """
        self._ensure_connections()
        
        sync_methods = {
            "basic": self.sync_basic_data,
            "market": self.sync_market_data,
            "financial": self.sync_financial_data,
            "holder": self.sync_holder_data,
            "other": self.sync_other_data,
        }
        
        if sync_type == "all":
            results = []
            for name, method in sync_methods.items():
                try:
                    result = method()
                    results.append(result)
                except Exception as e:
                    logger.error(f"同步 {name} 失败: {e}")
                    results.append({"task": name, "success": False, "error": str(e)})
            return {"sync_type": "all", "results": results}
        
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
_scheduler: Optional[StarlightScheduler] = None


def get_scheduler() -> StarlightScheduler:
    """获取调度器实例"""
    global _scheduler
    if _scheduler is None:
        _scheduler = StarlightScheduler()
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
