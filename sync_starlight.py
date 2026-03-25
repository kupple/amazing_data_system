# -*- coding: utf-8 -*-
"""
Starlight (AmazingData) 完整数据同步脚本
支持增量同步、进度显示、断点续传
"""
import sys
import os
import time
import pandas as pd
from datetime import datetime, timedelta
from typing import List, Set, Dict

# 添加项目路径
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from src.collectors.starlight.client import get_client
from src.collectors.starlight.sync_shared import StarlightSyncSupport
from src.common.database import get_db
from src.common.logger import logger


class StarlightSyncManager(StarlightSyncSupport):
    """Starlight 数据同步管理器"""
    
    def __init__(self):
        self.client = get_client()
        self.db = get_db("starlight")
        self.total_records = 0
        self.start_time = None
    
    def connect(self):
        """连接到服务"""
        if not self.client.is_connected:
            self.client.connect()
        logger.info("✓ 已连接到 AmazingData")
    
    def get_all_codes(self) -> List[str]:
        """获取所有股票代码"""
        logger.info("获取股票列表...")
        
        # 先尝试从数据库获取
        if self.db.table_exists("stock_codes"):
            result = self.db.client.query("SELECT code FROM stock_codes")
            codes = [row[0] for row in result.result_rows]
            if codes:
                logger.info(f"✓ 从数据库获取到 {len(codes)} 只股票")
                return codes
        
        # 从 API 获取
        codes = self._call_client_method(self.client.get_code_list, security_type="EXTRA_STOCK_A")
        logger.info(f"✓ 从 API 获取到 {len(codes)} 只股票")
        return codes
    
    def get_synced_codes(self, table_name: str) -> Set[str]:
        """获取已同步的股票代码"""
        synced = set()
        
        # 从统一表中查询已有的股票代码
        if self.db.table_exists(table_name):
            try:
                result = self.db.client.query(f"SELECT DISTINCT code FROM {table_name}")
                synced = set([row[0] for row in result.result_rows])
            except Exception as e:
                logger.warning(f"查询已同步代码失败: {e}")
        
        return synced

    
    def sync_basic_data(self):
        """同步基础数据"""
        logger.info("\n" + "=" * 60)
        logger.info("1. 同步基础数据")
        logger.info("=" * 60)
        
        # 1.1 股票代码表
        logger.info("1.1 同步股票代码表...")
        if not self._should_skip_table_sync("stock_codes"):
            try:
                code_list = self._call_client_method(self.client.get_code_list, security_type="EXTRA_STOCK_A")
                df = pd.DataFrame({"code": code_list})
                df["update_time"] = datetime.now()
                self.db.execute("DROP TABLE IF EXISTS stock_codes")
                self.db.insert_dataframe(df, "stock_codes")
                logger.info(f"✓ 股票代码表已更新: {len(df)} 条")
            except Exception as e:
                logger.error(f"✗ 同步股票代码表失败: {e}")
        
        # 1.2 交易日历
        logger.info("1.2 同步交易日历...")
        if not self._should_skip_table_sync("trading_calendar"):
            try:
                calendar = self._call_client_method(self.client.get_calendar, data_type="str", market="SH")
                if calendar:
                    logger.info(f"原始数据: {calendar[:5]}, 类型: {[type(x).__name__ for x in calendar[:5]]}")
                    df = pd.DataFrame({"trade_date": calendar})
                    formats = ["%Y%m%d", "%Y-%m-%d", "%Y/%m/%d", None]
                    for fmt in formats:
                        try:
                            df["trade_date"] = pd.to_datetime(df["trade_date"], format=fmt)
                            logger.info(f"使用格式 {fmt} 解析成功")
                            break
                        except Exception as e:
                            logger.warning(f"格式 {fmt} 解析失败: {e}")
                            continue
                    logger.info(f"解析后数据: {df['trade_date'].head().tolist()}")
                    df = df.dropna(subset=["trade_date"])
                    self.db.incremental_update(
                        "trading_calendar",
                        df,
                        key_columns=["trade_date"],
                        date_column="trade_date"
                    )
                    logger.info(f"✓ 交易日历已更新: {len(df)} 条")
                else:
                    logger.warning("交易日历为空，跳过")
            except Exception as e:
                logger.error(f"✗ 同步交易日历失败: {e}")
                import traceback
                traceback.print_exc()
        
        # 1.3 证券基础信息
        logger.info("1.3 同步证券基础信息...")
        if not self._should_skip_table_sync("stock_basic", checkpoint_keys=["sync_basic.stock_basic"]):
            try:
                code_list = self.get_all_codes()
                self.db.execute("DROP TABLE IF EXISTS stock_basic")
                total_rows = 0
                for batch_index, _, stock_basic in self._iter_code_batch_results(
                    self.client.get_stock_basic,
                    code_list,
                    batch_size=1050,
                    sleep_seconds=0.02,
                    checkpoint_key="sync_basic.stock_basic",
                ):
                    if isinstance(stock_basic, pd.DataFrame) and not stock_basic.empty:
                        stock_basic = self._lowercase_columns(stock_basic)
                        self.db.insert_dataframe(stock_basic, "stock_basic")
                        total_rows += len(stock_basic)
                    self._set_checkpoint("sync_basic.stock_basic", batch_index + 1)
                if total_rows:
                    logger.info(f"✓ 证券基础信息已更新: {total_rows} 条")
                self._clear_checkpoint("sync_basic.stock_basic")
            except Exception as e:
                logger.error(f"✗ 同步证券基础信息失败: {e}")
        
        # 1.4 复权因子
        logger.info("1.4 同步复权因子...")
        if self._should_skip_table_sync("backward_factor", checkpoint_keys=["sync_basic.backward_factor"]) and self._should_skip_table_sync("adj_factor", checkpoint_keys=["sync_basic.adj_factor"]):
            return
        try:
            code_list = self.get_all_codes()
            
            # 后复权因子 - 当前 SDK 返回 index=日期、columns=代码 的 DataFrame
            if not self._should_skip_table_sync("backward_factor", checkpoint_keys=["sync_basic.backward_factor"]):
                logger.info("  同步后复权因子...")
                factor_frames = []
                for batch_index, batch_codes in self._iter_batches(
                    code_list,
                    batch_size=1050,
                    checkpoint_key="sync_basic.backward_factor",
                ):
                    backward_factor = self._call_client_method(
                        self.client.get_backward_factor,
                        code_list=batch_codes,
                        is_local=False,
                    )
                    reshaped = self._reshape_factor_dataframe(backward_factor, "backward_factor")
                    if not reshaped.empty:
                        factor_frames.append(reshaped)
                    time.sleep(0.02)
                    self._set_checkpoint("sync_basic.backward_factor", batch_index + 1)
                merged_df = pd.concat(factor_frames, ignore_index=True) if factor_frames else pd.DataFrame()
                if not merged_df.empty:
                    self.db.incremental_update(
                        "backward_factor",
                        merged_df,
                        key_columns=["code", "date"],
                        date_column="date"
                    )
                    logger.info(f"  ✓ 后复权因子已更新: {len(merged_df)} 条")
                self._clear_checkpoint("sync_basic.backward_factor")
            
            # 前复权因子 - 当前 SDK 返回 index=日期、columns=代码 的 DataFrame
            if not self._should_skip_table_sync("adj_factor", checkpoint_keys=["sync_basic.adj_factor"]):
                logger.info("  同步前复权因子...")
                factor_frames = []
                for batch_index, batch_codes in self._iter_batches(
                    code_list,
                    batch_size=1050,
                    checkpoint_key="sync_basic.adj_factor",
                ):
                    adj_factor = self._call_client_method(
                        self.client.get_adj_factor,
                        code_list=batch_codes,
                        is_local=False,
                    )
                    reshaped = self._reshape_factor_dataframe(adj_factor, "adj_factor")
                    if not reshaped.empty:
                        factor_frames.append(reshaped)
                    time.sleep(0.02)
                    self._set_checkpoint("sync_basic.adj_factor", batch_index + 1)
                merged_df = pd.concat(factor_frames, ignore_index=True) if factor_frames else pd.DataFrame()
                if not merged_df.empty:
                    self.db.incremental_update(
                        "adj_factor",
                        merged_df,
                        key_columns=["code", "date"],
                        date_column="date"
                    )
                    logger.info(f"  ✓ 前复权因子已更新: {len(merged_df)} 条")
                self._clear_checkpoint("sync_basic.adj_factor")
                
        except Exception as e:
            logger.error(f"✗ 同步复权因子失败: {e}")
            import traceback
            traceback.print_exc()
    
    def sync_kline_data(self, force: bool = False):
        """同步K线数据（智能增量）"""
        logger.info("\n" + "=" * 60)
        logger.info("2. 同步K线数据")
        logger.info("=" * 60)
        
        # 获取所有股票
        all_codes = self.get_all_codes()
        logger.info(f"总股票数: {len(all_codes)}")
        
        # 获取已同步的股票
        synced = self.get_synced_codes("kline_daily")
        logger.info(f"已同步: {len(synced)}")
        
        # 判断是否首次同步
        is_first_sync = len(synced) < len(all_codes) * 0.5
        
        if force:
            # 强制全量同步所有股票
            codes_to_sync = all_codes
            logger.info(f"强制全量同步: {len(codes_to_sync)}")
        else:
            # 智能同步：新股票 + 已有股票的增量更新
            codes_to_sync = all_codes
            logger.info(f"智能同步模式: {len(codes_to_sync)} 只股票")
        
        # 智能判断时间范围
        end_date = datetime.now()
        if is_first_sync:
            # 首次同步：获取全部历史（从2010年开始）
            default_start_date = datetime(2010, 1, 1)
            logger.info("⚠ 检测到首次同步，将获取全部历史数据（从2010年开始）")
        else:
            # 增量同步：查询数据库中最新的日期
            latest_date = self._get_latest_date_with_fallback("kline_daily", ["kline_time", "trade_time", "time"])
            
            if latest_date and latest_date != "None":
                # 从最新日期开始同步（往前推1天以防遗漏）
                try:
                    default_start_date = datetime.strptime(latest_date[:10], "%Y-%m-%d") - timedelta(days=1)
                    logger.info(f"✓ 增量同步模式，从最新日期 {latest_date[:10]} 开始")
                except (ValueError, TypeError):
                    # 如果日期格式有问题，默认获取最近30天
                    default_start_date = end_date - timedelta(days=30)
                    logger.info("⚠ 最新日期格式异常，默认获取最近30天数据")
            else:
                # 如果查不到最新日期，默认获取最近30天
                default_start_date = end_date - timedelta(days=30)
                logger.info("⚠ 未找到最新日期，默认获取最近30天数据")
        
        begin_date_int = int(default_start_date.strftime("%Y%m%d"))
        end_date_int = int(end_date.strftime("%Y%m%d"))
        
        # 验证日期参数
        if not begin_date_int or not end_date_int:
            logger.error(f"K线同步日期参数无效: begin_date_int={begin_date_int}, end_date_int={end_date_int}")
            return
        
        logger.info(f"时间范围: {begin_date_int} - {end_date_int}")
        
        # 分批同步
        batch_size = 50
        total_batches = (len(codes_to_sync) + batch_size - 1) // batch_size
        self.total_records = 0
        self.start_time = time.time()
        completed = True
        
        for batch_idx, batch_codes in self._iter_batches(
            codes_to_sync,
            batch_size=batch_size,
            checkpoint_key="sync_kline_data",
        ):
            
            try:
                # 获取K线数据
                kline_dict = self._call_client_method(
                    self.client.query_kline,
                    code_list=batch_codes,
                    begin_date=begin_date_int,
                    end_date=end_date_int
                )
                
                # 保存到统一的K线表
                batch_records = 0
                if isinstance(kline_dict, dict):
                    for code, df in kline_dict.items():
                        if df is not None and not df.empty:
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
                            batch_records += len(df)
                
                self.total_records += batch_records
                
                # 显示进度
                self._print_progress(batch_idx + 1, total_batches, len(codes_to_sync))
                self._set_checkpoint("sync_kline_data", batch_idx + 1)
                
            except Exception as e:
                logger.error(f"✗ 批次 {batch_idx + 1} 同步失败: {e}")
                import traceback
                traceback.print_exc()
                completed = False
                break
            
            # 避免请求过快
            time.sleep(1)
        
        logger.info(f"\n✓ K线数据同步完成，共 {self.total_records} 条记录")
        if completed:
            self._clear_checkpoint("sync_kline_data")
    
    def sync_snapshot_data(self, force: bool = False):
        """同步快照数据（智能增量）"""
        logger.info("\n" + "=" * 60)
        logger.info("2.5 同步快照数据")
        logger.info("=" * 60)
        
        # 获取所有股票
        all_codes = self.get_all_codes()
        logger.info(f"总股票数: {len(all_codes)}")
        
        # 获取已同步的股票
        synced = self.get_synced_codes("snapshot")
        logger.info(f"已同步: {len(synced)}")
        
        # 判断是否首次同步
        is_first_sync = len(synced) < len(all_codes) * 0.5
        
        # 智能判断时间范围
        end_date = datetime.now()
        if is_first_sync:
            # 首次同步：获取最近1年数据（快照数据量大）
            default_start_date = end_date - timedelta(days=365)
            logger.info("⚠ 检测到首次同步，将获取最近1年快照数据")
        else:
            # 增量同步：查询数据库中最新的日期
            latest_date = self._get_latest_date_with_fallback("snapshot", ["time", "snapshot_time"])
            
            if latest_date and latest_date != "None":
                try:
                    default_start_date = datetime.strptime(latest_date[:10], "%Y-%m-%d") - timedelta(days=1)
                    logger.info(f"✓ 增量同步模式，从最新日期 {latest_date[:10]} 开始")
                except (ValueError, TypeError):
                    default_start_date = end_date - timedelta(days=30)
                    logger.info("⚠ 最新日期格式异常，默认获取最近30天数据")
            else:
                default_start_date = end_date - timedelta(days=30)
                logger.info("⚠ 未找到最新日期，默认获取最近30天数据")
        
        begin_date_int = int(default_start_date.strftime("%Y%m%d"))
        end_date_int = int(end_date.strftime("%Y%m%d"))
        
        # 验证日期参数
        if not begin_date_int or not end_date_int:
            logger.error(f"快照同步日期参数无效: begin_date_int={begin_date_int}, end_date_int={end_date_int}")
            return
        
        logger.info(f"时间范围: {begin_date_int} - {end_date_int}")
        
        # 分批同步
        batch_size = 20  # 快照数据量大，减小批次
        codes_to_sync = all_codes
        total_batches = (len(codes_to_sync) + batch_size - 1) // batch_size
        self.total_records = 0
        self.start_time = time.time()
        completed = True
        
        for batch_idx, batch_codes in self._iter_batches(
            codes_to_sync,
            batch_size=batch_size,
            checkpoint_key="sync_snapshot_data",
        ):
            
            try:
                # 获取快照数据
                snapshot_dict = self._call_client_method(
                    self.client.query_snapshot,
                    code_list=batch_codes,
                    begin_date=begin_date_int,
                    end_date=end_date_int
                )
                
                # 保存到统一的快照表
                batch_records = 0
                if isinstance(snapshot_dict, dict):
                    for code, df in snapshot_dict.items():
                        if df is not None and not df.empty:
                            # 添加 code 列
                            df = self._lowercase_columns(df)
                            df['code'] = code
                            time_column = self._find_existing_column(df, ["time", "snapshot_time"])
                            
                            # 增量插入到统一表
                            self.db.incremental_update(
                                "snapshot",
                                df,
                                key_columns=["code", time_column],
                                date_column=time_column
                            )
                            batch_records += len(df)
                
                self.total_records += batch_records
                
                # 显示进度
                self._print_progress(batch_idx + 1, total_batches, len(codes_to_sync))
                self._set_checkpoint("sync_snapshot_data", batch_idx + 1)
                
            except Exception as e:
                logger.error(f"✗ 批次 {batch_idx + 1} 同步失败: {e}")
                completed = False
                break
            
            # 避免请求过快
            time.sleep(2)
        
        logger.info(f"\n✓ 快照数据同步完成，共 {self.total_records} 条记录")
        if completed:
            self._clear_checkpoint("sync_snapshot_data")
    
    def sync_financial_data(self, force: bool = False):
        """同步财务数据（智能增量）"""
        logger.info("\n" + "=" * 60)
        logger.info("3. 同步财务数据")
        logger.info("=" * 60)
        
        all_codes = self.get_all_codes()
        
        # 检查是否首次同步
        is_first_sync = not self.db.table_exists("balance_sheet")
        
        if is_first_sync:
            logger.info("⚠ 检测到首次同步，将获取全部历史财务数据")
        else:
            logger.info("✓ 增量同步模式，使用本地缓存加速")
        
        financial_types = [
            ("balance_sheet", "资产负债表"),
            ("cash_flow", "现金流量表"),
            ("income", "利润表"),
            ("profit_express", "业绩快报"),
            ("profit_notice", "业绩预告"),
        ]
        
        for data_type, name in financial_types:
            logger.info(f"\n3.{financial_types.index((data_type, name)) + 1} 同步{name}...")
            
            try:
                method = getattr(self.client, f"get_{data_type}")
                # 首次同步强制从服务器获取，后续使用本地缓存
                total_rows = 0
                for batch_index, _, data in self._iter_code_batch_results(
                    method,
                    all_codes,
                    batch_size=20,
                    sleep_seconds=0.02,
                    checkpoint_key=f"sync_financial.{data_type}",
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
                    self._set_checkpoint(f"sync_financial.{data_type}", batch_index + 1)
                if total_rows:
                    logger.info(f"✓ {name}已更新: {total_rows} 条")
                self._clear_checkpoint(f"sync_financial.{data_type}")
                
            except Exception as e:
                logger.error(f"✗ 同步{name}失败: {e}")
    
    def sync_holder_data(self):
        """同步股东数据（智能增量）"""
        logger.info("\n" + "=" * 60)
        logger.info("4. 同步股东数据")
        logger.info("=" * 60)
        
        all_codes = self.get_all_codes()
        
        # 检查是否首次同步
        is_first_sync = not self.db.table_exists("share_holder")
        
        if is_first_sync:
            logger.info("⚠ 检测到首次同步，将获取全部历史股东数据")
        else:
            logger.info("✓ 增量同步模式，使用本地缓存加速")
        
        holder_types = [
            ("share_holder", "十大股东", ["market_code", "holder_enddate", "qty_num"]),
            ("holder_num", "股东户数", ["market_code", "holder_enddate"]),
            ("equity_structure", "股本结构", ["market_code", "change_date"]),
        ]
        
        for data_type, name, key_columns in holder_types:
            logger.info(f"\n4.{holder_types.index((data_type, name, key_columns)) + 1} 同步{name}...")
            
            try:
                method = getattr(self.client, f"get_{data_type}")
                total_rows = 0
                for batch_index, _, data in self._iter_code_batch_results(
                    method,
                    all_codes,
                    batch_size=20,
                    sleep_seconds=0.02,
                    checkpoint_key=f"sync_holder.{data_type}",
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
                    self._set_checkpoint(f"sync_holder.{data_type}", batch_index + 1)
                if total_rows:
                    logger.info(f"✓ {name}已更新: {total_rows} 条")
                self._clear_checkpoint(f"sync_holder.{data_type}")
                
            except Exception as e:
                logger.error(f"✗ 同步{name}失败: {e}")
    
    def sync_other_data(self):
        """同步其他数据（智能增量）"""
        logger.info("\n" + "=" * 60)
        logger.info("5. 同步其他数据")
        logger.info("=" * 60)
        
        all_codes = self.get_all_codes()
        
        # 检查是否首次同步
        is_first_sync = not self.db.table_exists("margin_summary")
        
        if is_first_sync:
            logger.info("⚠ 检测到首次同步，将获取全部历史数据")
        else:
            logger.info("✓ 增量同步模式，使用本地缓存加速")
        
        # 5.1 融资融券汇总
        logger.info("\n5.1 同步融资融券汇总...")
        try:
            margin_summary = self._call_client_method(self.client.get_margin_summary, is_local=(not is_first_sync))
            if not margin_summary.empty:
                margin_summary.columns = [col.lower() for col in margin_summary.columns]
                self.db.incremental_update(
                    "margin_summary",
                    margin_summary,
                    key_columns=["trade_date"],
                    date_column="trade_date"
                )
                logger.info(f"✓ 融资融券汇总已更新: {len(margin_summary)} 条")
        except Exception as e:
            logger.error(f"✗ 同步融资融券汇总失败: {e}")
        
        # 5.2 融资融券明细
        logger.info("\n5.2 同步融资融券明细...")
        try:
            total_rows = 0
            for batch_index, _, margin_detail in self._iter_code_batch_results(
                self.client.get_margin_detail,
                all_codes,
                batch_size=20,
                sleep_seconds=0.02,
                checkpoint_key="sync_other.margin_detail",
                is_local=(not is_first_sync),
            ):
                if isinstance(margin_detail, dict):
                    all_data = []
                    for code, df in margin_detail.items():
                        if not df.empty:
                            df['market_code'] = code
                            all_data.append(df)
                    if all_data:
                        merged_df = pd.concat(all_data, ignore_index=True)
                        merged_df.columns = [col.lower() for col in merged_df.columns]
                        self.db.incremental_update(
                            "margin_detail",
                            merged_df,
                            key_columns=["market_code", "trade_date"],
                            date_column="trade_date"
                        )
                        total_rows += len(merged_df)
                self._set_checkpoint("sync_other.margin_detail", batch_index + 1)
            if total_rows:
                logger.info(f"✓ 融资融券明细已更新: {total_rows} 条")
            self._clear_checkpoint("sync_other.margin_detail")
        except Exception as e:
            logger.error(f"✗ 同步融资融券明细失败: {e}")
        
        # 5.3 龙虎榜
        logger.info("\n5.3 同步龙虎榜...")
        try:
            total_rows = 0
            for batch_index, _, dragon_tiger in self._iter_code_batch_results(
                self.client.get_long_hu_bang,
                all_codes,
                batch_size=20,
                sleep_seconds=0.02,
                checkpoint_key="sync_other.dragon_tiger",
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
                self._set_checkpoint("sync_other.dragon_tiger", batch_index + 1)
            if total_rows:
                logger.info(f"✓ 龙虎榜已更新: {total_rows} 条")
            self._clear_checkpoint("sync_other.dragon_tiger")
        except Exception as e:
            logger.error(f"✗ 同步龙虎榜失败: {e}")
        
        # 5.4 大宗交易
        logger.info("\n5.4 同步大宗交易...")
        try:
            total_rows = 0
            for batch_index, _, block_trade in self._iter_code_batch_results(
                self.client.get_block_trading,
                all_codes,
                batch_size=20,
                sleep_seconds=0.02,
                checkpoint_key="sync_other.block_trade",
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
                self._set_checkpoint("sync_other.block_trade", batch_index + 1)
            if total_rows:
                logger.info(f"✓ 大宗交易已更新: {total_rows} 条")
            self._clear_checkpoint("sync_other.block_trade")
        except Exception as e:
            logger.error(f"✗ 同步大宗交易失败: {e}")
        
        # 5.5 股权质押冻结
        logger.info("\n5.5 同步股权质押冻结...")
        try:
            total_rows = 0
            for batch_index, _, equity_pledge in self._iter_code_batch_results(
                self.client.get_equity_pledge_freeze,
                all_codes,
                batch_size=20,
                sleep_seconds=0.02,
                checkpoint_key="sync_other.equity_pledge_freeze",
                is_local=(not is_first_sync),
            ):
                if isinstance(equity_pledge, dict):
                    all_data = []
                    for code, df in equity_pledge.items():
                        if not df.empty:
                            df['market_code'] = code
                            all_data.append(df)
                    if all_data:
                        merged_df = pd.concat(all_data, ignore_index=True)
                        merged_df.columns = [col.lower() for col in merged_df.columns]
                        self.db.incremental_update(
                            "equity_pledge_freeze",
                            merged_df,
                            key_columns=["market_code", "announce_date", "holder_name"]
                        )
                        total_rows += len(merged_df)
                self._set_checkpoint("sync_other.equity_pledge_freeze", batch_index + 1)
            if total_rows:
                logger.info(f"✓ 股权质押冻结已更新: {total_rows} 条")
            self._clear_checkpoint("sync_other.equity_pledge_freeze")
        except Exception as e:
            logger.error(f"✗ 同步股权质押冻结失败: {e}")
        
        # 5.6 限售股解禁
        logger.info("\n5.6 同步限售股解禁...")
        try:
            total_rows = 0
            for batch_index, _, equity_restricted in self._iter_code_batch_results(
                self.client.get_equity_restricted,
                all_codes,
                batch_size=20,
                sleep_seconds=0.02,
                checkpoint_key="sync_other.equity_restricted",
                is_local=(not is_first_sync),
            ):
                if isinstance(equity_restricted, dict):
                    all_data = []
                    for code, df in equity_restricted.items():
                        if not df.empty:
                            df['market_code'] = code
                            all_data.append(df)
                    if all_data:
                        merged_df = pd.concat(all_data, ignore_index=True)
                        merged_df.columns = [col.lower() for col in merged_df.columns]
                        self.db.incremental_update(
                            "equity_restricted",
                            merged_df,
                            key_columns=["market_code", "lift_date"]
                        )
                        total_rows += len(merged_df)
                self._set_checkpoint("sync_other.equity_restricted", batch_index + 1)
            if total_rows:
                logger.info(f"✓ 限售股解禁已更新: {total_rows} 条")
            self._clear_checkpoint("sync_other.equity_restricted")
        except Exception as e:
            logger.error(f"✗ 同步限售股解禁失败: {e}")
        
        # 5.7 分红送股
        logger.info("\n5.7 同步分红送股...")
        try:
            total_rows = 0
            for batch_index, _, dividend in self._iter_code_batch_results(
                self.client.get_dividend,
                all_codes,
                batch_size=20,
                sleep_seconds=0.02,
                checkpoint_key="sync_other.dividend",
                is_local=(not is_first_sync),
            ):
                if isinstance(dividend, pd.DataFrame) and not dividend.empty:
                    dividend.columns = [col.lower() for col in dividend.columns]
                    self.db.incremental_update(
                        "dividend",
                        dividend,
                        key_columns=["market_code", "announce_date"]
                    )
                    total_rows += len(dividend)
                self._set_checkpoint("sync_other.dividend", batch_index + 1)
            if total_rows:
                logger.info(f"✓ 分红送股已更新: {total_rows} 条")
            self._clear_checkpoint("sync_other.dividend")
        except Exception as e:
            logger.error(f"✗ 同步分红送股失败: {e}")
        
        # 5.8 配股
        logger.info("\n5.8 同步配股...")
        try:
            total_rows = 0
            for batch_index, _, right_issue in self._iter_code_batch_results(
                self.client.get_right_issue,
                all_codes,
                batch_size=20,
                sleep_seconds=0.02,
                checkpoint_key="sync_other.right_issue",
                is_local=(not is_first_sync),
            ):
                if isinstance(right_issue, pd.DataFrame) and not right_issue.empty:
                    right_issue.columns = [col.lower() for col in right_issue.columns]
                    self.db.incremental_update(
                        "right_issue",
                        right_issue,
                        key_columns=["market_code", "announce_date"]
                    )
                    total_rows += len(right_issue)
                self._set_checkpoint("sync_other.right_issue", batch_index + 1)
            if total_rows:
                logger.info(f"✓ 配股已更新: {total_rows} 条")
            self._clear_checkpoint("sync_other.right_issue")
        except Exception as e:
            logger.error(f"✗ 同步配股失败: {e}")
    
    def _print_progress(self, current: int, total: int, total_codes: int):
        """打印进度信息"""
        elapsed = time.time() - self.start_time
        rate = current / elapsed * 60  # 批次/分钟
        remaining = (total - current) / rate if rate > 0 else 0
        
        progress = current / total * 100
        logger.info(
            f"进度: {current}/{total} ({progress:.1f}%), "
            f"累计: {self.total_records} 条, "
            f"预计剩余: {remaining:.1f} 分钟"
        )
    
    def sync_index_data(self):
        """同步指数数据"""
        logger.info("\n" + "=" * 60)
        logger.info("6. 同步指数数据")
        logger.info("=" * 60)
        
        # 获取指数代码列表
        logger.info("获取指数代码列表...")
        try:
            index_codes = self._call_client_method(self.client.get_code_list, security_type="EXTRA_INDEX_A")
            logger.info(f"✓ 获取到 {len(index_codes)} 个指数")
        except Exception as e:
            logger.error(f"✗ 获取指数代码失败: {e}")
            return
        
        is_first_sync = not self.db.table_exists("index_constituent")
        
        # 6.1 指数成分股
        logger.info("\n6.1 同步指数成分股...")
        try:
            index_constituent = self._call_client_method(
                self.client.get_index_constituent,
                code_list=index_codes,
                is_local=(not is_first_sync),
            )
            if isinstance(index_constituent, dict):
                all_data = []
                for code, df in index_constituent.items():
                    if not df.empty:
                        df = self._lowercase_columns(df)
                        if "index_code" not in df.columns:
                            df["index_code"] = code
                        all_data.append(df)
                if all_data:
                    merged_df = pd.concat(all_data, ignore_index=True)
                    self.db.incremental_update(
                        "index_constituent",
                        merged_df,
                        key_columns=[col for col in ["index_code", "con_code", "indate"] if col in merged_df.columns]
                    )
                    logger.info(f"✓ 指数成分股已更新: {len(merged_df)} 条")
        except Exception as e:
            logger.error(f"✗ 同步指数成分股失败: {e}")
        
        # 6.2 指数权重
        logger.info("\n6.2 同步指数权重...")
        try:
            supported_weight_codes = [
                "000016.SH",
                "000300.SH",
                "000905.SH",
                "000906.SH",
                "000852.SH",
            ]
            weight_codes = [code for code in supported_weight_codes if code in index_codes]
            if not weight_codes:
                logger.warning("未获取到文档支持的指数权重代码，跳过指数权重同步")
                return
            date_range = self._build_incremental_date_range(
                "index_weight",
                ["trade_date"],
                first_sync_days=365,
                fallback_days=30,
            )
            index_weight = self._call_client_method(
                self.client.get_index_weight,
                code_list=weight_codes,
                is_local=(not is_first_sync),
                begin_date=date_range["begin_date"],
                end_date=date_range["end_date"],
            )
            if isinstance(index_weight, dict):
                all_data = []
                for code, df in index_weight.items():
                    if not df.empty:
                        df = self._lowercase_columns(df)
                        if "index_code" not in df.columns:
                            df["index_code"] = code
                        all_data.append(df)
                if all_data:
                    merged_df = pd.concat(all_data, ignore_index=True)
                    self.db.incremental_update(
                        "index_weight",
                        merged_df,
                        key_columns=[col for col in ["index_code", "con_code", "trade_date"] if col in merged_df.columns],
                        date_column="trade_date"
                    )
                    logger.info(f"✓ 指数权重已更新: {len(merged_df)} 条")
        except Exception as e:
            logger.error(f"✗ 同步指数权重失败: {e}")
    
    def sync_industry_data(self):
        """同步行业数据"""
        logger.info("\n" + "=" * 60)
        logger.info("7. 同步行业数据")
        logger.info("=" * 60)
        
        is_first_sync = not self.db.table_exists("industry_base_info")
        
        # 7.1 行业基本信息
        logger.info("\n7.1 同步行业基本信息...")
        try:
            industry_info = self._call_client_method(self.client.get_industry_base_info, is_local=(not is_first_sync))
            if not industry_info.empty:
                industry_info = self._lowercase_columns(industry_info)
                self.db.execute("DROP TABLE IF EXISTS industry_base_info")
                self.db.insert_dataframe(industry_info, "industry_base_info")
                logger.info(f"✓ 行业基本信息已更新: {len(industry_info)} 条")
                
                # 获取行业代码列表用于后续查询
                industry_codes = industry_info['index_code'].tolist() if 'index_code' in industry_info.columns else []
            else:
                industry_codes = []
        except Exception as e:
            logger.error(f"✗ 同步行业基本信息失败: {e}")
            industry_codes = []
        
        if not industry_codes:
            logger.warning("未获取到行业代码，跳过后续行业数据同步")
            return
        
        # 7.2 行业成分股
        logger.info("\n7.2 同步行业成分股...")
        try:
            industry_constituent = self._call_client_method(
                self.client.get_industry_constituent,
                code_list=industry_codes,
                is_local=(not is_first_sync),
            )
            if isinstance(industry_constituent, dict):
                all_data = []
                for code, df in industry_constituent.items():
                    if not df.empty:
                        df = self._lowercase_columns(df)
                        if "index_code" not in df.columns:
                            df["index_code"] = code
                        all_data.append(df)
                if all_data:
                    merged_df = pd.concat(all_data, ignore_index=True)
                    self.db.incremental_update(
                        "industry_constituent",
                        merged_df,
                        key_columns=[col for col in ["index_code", "con_code", "indate"] if col in merged_df.columns]
                    )
                    logger.info(f"✓ 行业成分股已更新: {len(merged_df)} 条")
        except Exception as e:
            logger.error(f"✗ 同步行业成分股失败: {e}")
        
        # 7.3 行业权重
        logger.info("\n7.3 同步行业权重...")
        try:
            date_range = self._build_incremental_date_range(
                "industry_weight",
                ["trade_date"],
                first_sync_days=365,
                fallback_days=30,
            )
            industry_weight = self._call_client_method(
                self.client.get_industry_weight,
                code_list=industry_codes,
                is_local=(not is_first_sync),
                begin_date=date_range["begin_date"],
                end_date=date_range["end_date"],
            )
            if isinstance(industry_weight, dict):
                all_data = []
                for code, df in industry_weight.items():
                    if not df.empty:
                        df = self._lowercase_columns(df)
                        if "index_code" not in df.columns:
                            df["index_code"] = code
                        all_data.append(df)
                if all_data:
                    merged_df = pd.concat(all_data, ignore_index=True)
                    self.db.incremental_update(
                        "industry_weight",
                        merged_df,
                        key_columns=[col for col in ["index_code", "con_code", "trade_date"] if col in merged_df.columns],
                        date_column="trade_date"
                    )
                    logger.info(f"✓ 行业权重已更新: {len(merged_df)} 条")
        except Exception as e:
            logger.error(f"✗ 同步行业权重失败: {e}")
        
        # 7.4 行业日线
        logger.info("\n7.4 同步行业日线...")
        try:
            date_range = self._build_incremental_date_range(
                "industry_daily",
                ["trade_date"],
                first_sync_days=365,
                fallback_days=30,
            )
            industry_daily = self._call_client_method(
                self.client.get_industry_daily,
                code_list=industry_codes,
                is_local=(not is_first_sync),
                begin_date=date_range["begin_date"],
                end_date=date_range["end_date"],
            )
            if isinstance(industry_daily, dict):
                all_data = []
                for code, df in industry_daily.items():
                    if not df.empty:
                        df = self._lowercase_columns(df)
                        if "index_code" not in df.columns:
                            df["index_code"] = code
                        all_data.append(df)
                if all_data:
                    merged_df = pd.concat(all_data, ignore_index=True)
                    self.db.incremental_update(
                        "industry_daily",
                        merged_df,
                        key_columns=[col for col in ["index_code", "trade_date"] if col in merged_df.columns],
                        date_column="trade_date"
                    )
                    logger.info(f"✓ 行业日线已更新: {len(merged_df)} 条")
        except Exception as e:
            logger.error(f"✗ 同步行业日线失败: {e}")
    
    def sync_convertible_bond_data(self):
        """同步可转债数据"""
        logger.info("\n" + "=" * 60)
        logger.info("8. 同步可转债数据")
        logger.info("=" * 60)
        
        # 获取可转债代码列表
        logger.info("获取可转债代码列表...")
        try:
            kzz_codes = self._call_client_method(self.client.get_code_list, security_type="EXTRA_KZZ")
            logger.info(f"✓ 获取到 {len(kzz_codes)} 个可转债")
        except Exception as e:
            logger.error(f"✗ 获取可转债代码失败: {e}")
            return
        
        is_first_sync = not self.db.table_exists("kzz_issuance")
        
        kzz_types = [
            ("kzz_issuance", "可转债发行", ["market_code"]),
            ("kzz_share", "可转债份额", ["market_code", "trade_date"]),
            ("kzz_conv", "可转债转股", ["market_code", "trade_date"]),
            ("kzz_conv_change", "可转债转股变动", ["market_code", "change_date"]),
            ("kzz_corr", "可转债修正", ["market_code", "trade_date"]),
            ("kzz_call", "可转债赎回", ["market_code", "announce_date"]),
            ("kzz_put", "可转债回售", ["market_code", "announce_date"]),
            ("kzz_suspend", "可转债停复牌", ["market_code", "suspend_date"]),
        ]
        
        for data_type, name, key_columns in kzz_types:
            logger.info(f"\n8.{kzz_types.index((data_type, name, key_columns)) + 1} 同步{name}...")
            try:
                method = getattr(self.client, f"get_{data_type}")
                data = method(code_list=kzz_codes, is_local=(not is_first_sync))
                
                if not data.empty:
                    data.columns = [col.lower() for col in data.columns]
                    self.db.incremental_update(
                        data_type,
                        data,
                        key_columns=key_columns
                    )
                    logger.info(f"✓ {name}已更新: {len(data)} 条")
            except Exception as e:
                logger.error(f"✗ 同步{name}失败: {e}")
    
    def sync_etf_data(self):
        """同步ETF数据"""
        logger.info("\n" + "=" * 60)
        logger.info("9. 同步ETF数据")
        logger.info("=" * 60)
        
        # 获取ETF代码列表
        logger.info("获取ETF代码列表...")
        try:
            etf_codes = self._call_client_method(self.client.get_code_list, security_type="EXTRA_ETF")
            logger.info(f"✓ 获取到 {len(etf_codes)} 个ETF")
        except Exception as e:
            logger.error(f"✗ 获取ETF代码失败: {e}")
            return
        
        is_first_sync = not self.db.table_exists("etf_pcf")
        
        # 9.1 ETF申赎数据
        logger.info("\n9.1 同步ETF申赎数据...")
        try:
            etf_pcf_data = self.client.get_etf_pcf(code_list=etf_codes)
            if isinstance(etf_pcf_data, tuple) and len(etf_pcf_data) > 0:
                df = etf_pcf_data[0] if isinstance(etf_pcf_data[0], pd.DataFrame) else None
                if df is not None and not df.empty:
                    df.columns = [col.lower() for col in df.columns]
                    self.db.incremental_update(
                        "etf_pcf",
                        df,
                        key_columns=["etf_code", "trade_date"],
                        date_column="trade_date"
                    )
                    logger.info(f"✓ ETF申赎数据已更新: {len(df)} 条")
        except Exception as e:
            logger.error(f"✗ 同步ETF申赎数据失败: {e}")
        
        # 9.2 基金份额
        logger.info("\n9.2 同步基金份额...")
        try:
            fund_share = self.client.get_fund_share(code_list=etf_codes, is_local=(not is_first_sync))
            if isinstance(fund_share, dict):
                all_data = []
                for code, df in fund_share.items():
                    if not df.empty:
                        df['fund_code'] = code
                        all_data.append(df)
                if all_data:
                    merged_df = pd.concat(all_data, ignore_index=True)
                    merged_df.columns = [col.lower() for col in merged_df.columns]
                    self.db.incremental_update(
                        "fund_share",
                        merged_df,
                        key_columns=["fund_code", "trade_date"],
                        date_column="trade_date"
                    )
                    logger.info(f"✓ 基金份额已更新: {len(merged_df)} 条")
        except Exception as e:
            logger.error(f"✗ 同步基金份额失败: {e}")
        
        # 9.3 基金IOPV
        logger.info("\n9.3 同步基金IOPV...")
        try:
            fund_iopv = self.client.get_fund_iopv(code_list=etf_codes, is_local=(not is_first_sync))
            if isinstance(fund_iopv, dict):
                all_data = []
                for code, df in fund_iopv.items():
                    if not df.empty:
                        df['fund_code'] = code
                        all_data.append(df)
                if all_data:
                    merged_df = pd.concat(all_data, ignore_index=True)
                    merged_df.columns = [col.lower() for col in merged_df.columns]
                    self.db.incremental_update(
                        "fund_iopv",
                        merged_df,
                        key_columns=["fund_code", "trade_time"]
                    )
                    logger.info(f"✓ 基金IOPV已更新: {len(merged_df)} 条")
        except Exception as e:
            logger.error(f"✗ 同步基金IOPV失败: {e}")
    
    def sync_option_data(self):
        """同步期权数据"""
        logger.info("\n" + "=" * 60)
        logger.info("10. 同步期权数据")
        logger.info("=" * 60)
        
        # 获取期权代码列表
        logger.info("获取期权代码列表...")
        try:
            option_codes = self._call_client_method(self.client.get_option_code_list, security_type="EXTRA_ETF_OP")
            logger.info(f"✓ 获取到 {len(option_codes)} 个期权")
        except Exception as e:
            logger.error(f"✗ 获取期权代码失败: {e}")
            return
        
        is_first_sync = not self.db.table_exists("option_basic_info")
        
        option_types = [
            ("option_basic_info", "期权基本资料", ["option_code"]),
            ("option_std_ctr_specs", "期权标准合约", ["option_code"]),
            ("option_mon_ctr_specs", "期权月合约", ["option_code", "contract_month"]),
        ]
        
        for data_type, name, key_columns in option_types:
            logger.info(f"\n10.{option_types.index((data_type, name, key_columns)) + 1} 同步{name}...")
            try:
                method = getattr(self.client, f"get_{data_type}")
                data = method(code_list=option_codes, is_local=(not is_first_sync))
                
                if not data.empty:
                    data.columns = [col.lower() for col in data.columns]
                    self.db.incremental_update(
                        data_type,
                        data,
                        key_columns=key_columns
                    )
                    logger.info(f"✓ {name}已更新: {len(data)} 条")
            except Exception as e:
                logger.error(f"✗ 同步{name}失败: {e}")
    
    def sync_treasury_data(self):
        """同步国债收益率数据"""
        logger.info("\n" + "=" * 60)
        logger.info("11. 同步国债收益率")
        logger.info("=" * 60)
        
        is_first_sync = not self.db.table_exists("treasury_yield")
        
        # 国债期限列表
        term_list = ["1M", "3M", "6M", "1Y", "2Y", "3Y", "5Y", "7Y", "10Y", "30Y"]
        
        logger.info(f"同步 {len(term_list)} 个期限的国债收益率...")
        try:
            treasury_data = self.client.get_treasury_yield(term_list=term_list, is_local=(not is_first_sync))
            if isinstance(treasury_data, dict):
                all_data = []
                for term, df in treasury_data.items():
                    if not df.empty:
                        df['term'] = term
                        all_data.append(df)
                if all_data:
                    merged_df = pd.concat(all_data, ignore_index=True)
                    merged_df.columns = [col.lower() for col in merged_df.columns]
                    self.db.incremental_update(
                        "treasury_yield",
                        merged_df,
                        key_columns=["trade_date", "term"],
                        date_column="trade_date"
                    )
                    logger.info(f"✓ 国债收益率已更新: {len(merged_df)} 条")
        except Exception as e:
            logger.error(f"✗ 同步国债收益率失败: {e}")
    
    def get_sync_status(self):
        """获取同步状态"""
        logger.info("\n" + "=" * 60)
        logger.info("同步状态汇总")
        logger.info("=" * 60)

        raw_status = self.db.get_table_sync_status()
        status_map = {}
        if isinstance(raw_status, list):
            for item in raw_status:
                table_name = item.get("table_name")
                if table_name and table_name not in status_map:
                    status_map[table_name] = item
        
        # 按类型分组显示
        categories = {
            "基础数据": ["stock_codes", "trading_calendar", "stock_basic", "backward_factor", "adj_factor"],
            "K线数据": ["kline_daily"],
            "快照数据": ["snapshot"],
            "财务数据": ["balance_sheet", "cash_flow", "income", "profit_express", "profit_notice"],
            "股东数据": ["share_holder", "holder_num", "equity_structure"],
            "其他数据": ["margin_summary", "margin_detail", "dragon_tiger", "block_trade", 
                       "equity_pledge_freeze", "equity_restricted", "dividend", "right_issue"],
            "指数数据": ["index_constituent", "index_weight"],
            "行业数据": ["industry_base_info", "industry_constituent", "industry_weight", "industry_daily"],
            "可转债数据": ["kzz_issuance", "kzz_share", "kzz_conv", "kzz_conv_change", 
                         "kzz_corr", "kzz_call", "kzz_put", "kzz_suspend"],
            "ETF数据": ["etf_pcf", "fund_share", "fund_iopv"],
            "期权数据": ["option_basic_info", "option_std_ctr_specs", "option_mon_ctr_specs"],
            "国债数据": ["treasury_yield"],
        }
        
        for category, table_list in categories.items():
            logger.info(f"\n{category}:")
            category_tables = [t for t in table_list if t in status_map]
            if category_tables:
                for table in category_tables:
                    item = status_map[table]
                    logger.info(
                        f"  - {table}: {item.get('record_count', 0):,} 条, "
                        f"状态={item.get('status')}, 最新日期={item.get('latest_date')}"
                    )
            else:
                logger.info("  (无数据)")
        
        return status_map


def main():
    """主函数"""
    import argparse
    
    parser = argparse.ArgumentParser(description="Starlight 数据同步脚本")
    parser.add_argument("--force", action="store_true", help="强制全量同步")
    parser.add_argument("--basic", action="store_true", help="只同步基础数据")
    parser.add_argument("--kline", action="store_true", help="只同步K线数据")
    parser.add_argument("--snapshot", action="store_true", help="只同步快照数据")
    parser.add_argument("--financial", action="store_true", help="只同步财务数据")
    parser.add_argument("--holder", action="store_true", help="只同步股东数据")
    parser.add_argument("--other", action="store_true", help="只同步其他数据")
    parser.add_argument("--index", action="store_true", help="只同步指数数据")
    parser.add_argument("--industry", action="store_true", help="只同步行业数据")
    parser.add_argument("--kzz", action="store_true", help="只同步可转债数据")
    parser.add_argument("--etf", action="store_true", help="只同步ETF数据")
    parser.add_argument("--option", action="store_true", help="只同步期权数据")
    parser.add_argument("--treasury", action="store_true", help="只同步国债数据")
    args = parser.parse_args()
    
    # 创建同步管理器
    manager = StarlightSyncManager()
    manager.connect()
    
    start_time = time.time()
    
    try:
        # 根据参数选择同步内容
        if args.basic:
            manager.sync_basic_data()
        elif args.kline:
            manager.sync_kline_data(force=args.force)
        elif args.snapshot:
            manager.sync_snapshot_data(force=args.force)
        elif args.financial:
            manager.sync_financial_data(force=args.force)
        elif args.holder:
            manager.sync_holder_data()
        elif args.other:
            manager.sync_other_data()
        elif args.index:
            manager.sync_index_data()
        elif args.industry:
            manager.sync_industry_data()
        elif args.kzz:
            manager.sync_convertible_bond_data()
        elif args.etf:
            manager.sync_etf_data()
        elif args.option:
            manager.sync_option_data()
        elif args.treasury:
            manager.sync_treasury_data()
        else:
            # 全量同步所有数据
            manager.sync_basic_data()
            manager.sync_kline_data(force=args.force)
            # manager.sync_snapshot_data(force=args.force)  # 快照数据量大，默认不同步
            manager.sync_financial_data(force=args.force)
            manager.sync_holder_data()
            manager.sync_other_data()
            manager.sync_index_data()
            manager.sync_industry_data()
            manager.sync_convertible_bond_data()
            manager.sync_etf_data()
            manager.sync_option_data()
            manager.sync_treasury_data()
        
        # 显示最终状态
        elapsed = time.time() - start_time
        logger.info(f"\n{'=' * 60}")
        logger.info(f"✓ 同步完成！总耗时: {elapsed / 60:.1f} 分钟")
        logger.info(f"{'=' * 60}")
        
        manager.get_sync_status()
        
    except KeyboardInterrupt:
        logger.warning("\n用户中断同步")
    except Exception as e:
        logger.error(f"\n同步失败: {e}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    main()
