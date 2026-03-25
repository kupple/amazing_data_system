"""
Starlight (AmazingData) 完整数据同步脚本
支持增量同步、进度显示、断点续传
"""
import sys
import os
import time
from datetime import datetime, timedelta
from typing import List, Set

# 添加项目路径
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from src.collectors.starlight.client import get_client
from src.common.database import get_db
from src.common.logger import logger


class StarlightSyncManager:
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
        codes = self.client.get_code_list("EXTRA_STOCK_A")
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
        try:
            code_list = self.client.get_code_list("EXTRA_STOCK_A")
            import pandas as pd
            df = pd.DataFrame({"code": code_list})
            df["update_time"] = datetime.now()
            
            self.db.execute("DROP TABLE IF EXISTS stock_codes")
            self.db.insert_dataframe(df, "stock_codes")
            logger.info(f"✓ 股票代码表已更新: {len(df)} 条")
        except Exception as e:
            logger.error(f"✗ 同步股票代码表失败: {e}")
        
        # 1.2 交易日历
        logger.info("1.2 同步交易日历...")
        try:
            calendar = self.client.get_calendar()
            import pandas as pd
            df = pd.DataFrame({"trade_date": calendar})
            df["trade_date"] = pd.to_datetime(df["trade_date"])
            
            self.db.incremental_update(
                "trading_calendar",
                df,
                key_columns=["trade_date"],
                date_column="trade_date"
            )
            logger.info(f"✓ 交易日历已更新")
        except Exception as e:
            logger.error(f"✗ 同步交易日历失败: {e}")
        
        # 1.3 证券基础信息
        logger.info("1.3 同步证券基础信息...")
        try:
            code_list = self.get_all_codes()
            stock_basic = self.client.get_stock_basic(code_list)
            
            if not stock_basic.empty:
                self.db.execute("DROP TABLE IF EXISTS stock_basic")
                self.db.insert_dataframe(stock_basic, "stock_basic")
                logger.info(f"✓ 证券基础信息已更新: {len(stock_basic)} 条")
        except Exception as e:
            logger.error(f"✗ 同步证券基础信息失败: {e}")
        
        # 1.4 复权因子
        logger.info("1.4 同步复权因子...")
        try:
            code_list = self.get_all_codes()
            
            # 后复权因子
            backward_factor = self.client.get_backward_factor(code_list, is_local=True)
            if not backward_factor.empty:
                self.db.incremental_update(
                    "backward_factor",
                    backward_factor,
                    key_columns=["code", "date"]
                )
                logger.info(f"✓ 后复权因子已更新: {len(backward_factor)} 条")
            
            # 前复权因子
            adj_factor = self.client.get_adj_factor(code_list, is_local=True)
            if not adj_factor.empty:
                self.db.incremental_update(
                    "adj_factor",
                    adj_factor,
                    key_columns=["code", "date"]
                )
                logger.info(f"✓ 前复权因子已更新: {len(adj_factor)} 条")
                
        except Exception as e:
            logger.error(f"✗ 同步复权因子失败: {e}")
    
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
            latest_date = self.db.get_latest_date("kline_daily", "trade_time")
            
            if latest_date:
                # 从最新日期开始同步（往前推1天以防遗漏）
                default_start_date = datetime.strptime(latest_date[:10], "%Y-%m-%d") - timedelta(days=1)
                logger.info(f"✓ 增量同步模式，从最新日期 {latest_date[:10]} 开始")
            else:
                # 如果查不到最新日期，默认获取最近30天
                default_start_date = end_date - timedelta(days=30)
                logger.info("⚠ 未找到最新日期，默认获取最近30天数据")
        
        default_begin_date_int = int(default_start_date.strftime("%Y%m%d"))
        end_date_int = int(end_date.strftime("%Y%m%d"))
        
        logger.info(f"默认时间范围: {default_begin_date_int} - {end_date_int}")
        
        # 分批同步
        batch_size = 50
        total_batches = (len(codes_to_sync) + batch_size - 1) // batch_size
        self.start_time = time.time()
        
        for batch_idx in range(total_batches):
            start_idx = batch_idx * batch_size
            end_idx = min(start_idx + batch_size, len(codes_to_sync))
            batch_codes = codes_to_sync[start_idx:end_idx]
            
            try:
                # 获取K线数据（使用默认时间范围）
                kline_dict = self.client.query_kline(
                    code_list=batch_codes,
                    begin_date=default_begin_date_int,
                    end_date=end_date_int,
                    period=1440  # 日线
                )
                
                # 保存到统一的K线表
                batch_records = 0
                for code, df in kline_dict.items():
                    if not df.empty:
                        # 添加 code 列
                        df['code'] = code
                        # 重命名列为小写
                        df.columns = [col.lower() for col in df.columns]
                        
                        # 增量插入到统一表
                        self.db.incremental_update(
                            "kline_daily",
                            df,
                            key_columns=["code", "trade_time"],
                            date_column="trade_time"
                        )
                        batch_records += len(df)
                
                self.total_records += batch_records
                
                # 显示进度
                self._print_progress(batch_idx + 1, total_batches, len(codes_to_sync))
                
            except Exception as e:
                logger.error(f"✗ 批次 {batch_idx + 1} 同步失败: {e}")
            
            # 避免请求过快
            time.sleep(1)
        
        logger.info(f"\n✓ K线数据同步完成，共 {self.total_records} 条记录")
    
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
            latest_date = self.db.get_latest_date("snapshot", "time")
            
            if latest_date:
                default_start_date = datetime.strptime(latest_date[:10], "%Y-%m-%d") - timedelta(days=1)
                logger.info(f"✓ 增量同步模式，从最新日期 {latest_date[:10]} 开始")
            else:
                default_start_date = end_date - timedelta(days=30)
                logger.info("⚠ 未找到最新日期，默认获取最近30天数据")
        
        default_begin_date_int = int(default_start_date.strftime("%Y%m%d"))
        end_date_int = int(end_date.strftime("%Y%m%d"))
        
        logger.info(f"时间范围: {default_begin_date_int} - {end_date_int}")
        
        # 分批同步
        batch_size = 20  # 快照数据量大，减小批次
        codes_to_sync = all_codes
        total_batches = (len(codes_to_sync) + batch_size - 1) // batch_size
        self.start_time = time.time()
        
        for batch_idx in range(total_batches):
            start_idx = batch_idx * batch_size
            end_idx = min(start_idx + batch_size, len(codes_to_sync))
            batch_codes = codes_to_sync[start_idx:end_idx]
            
            try:
                # 获取快照数据
                snapshot_dict = self.client.query_snapshot(
                    code_list=batch_codes,
                    begin_date=default_begin_date_int,
                    end_date=end_date_int
                )
                
                # 保存到统一的快照表
                batch_records = 0
                for code, df in snapshot_dict.items():
                    if not df.empty:
                        # 添加 code 列
                        df['code'] = code
                        # 重命名列为小写
                        df.columns = [col.lower() for col in df.columns]
                        
                        # 增量插入到统一表
                        self.db.incremental_update(
                            "snapshot",
                            df,
                            key_columns=["code", "time"],
                            date_column="time"
                        )
                        batch_records += len(df)
                
                self.total_records += batch_records
                
                # 显示进度
                self._print_progress(batch_idx + 1, total_batches, len(codes_to_sync))
                
            except Exception as e:
                logger.error(f"✗ 批次 {batch_idx + 1} 同步失败: {e}")
            
            # 避免请求过快
            time.sleep(2)
        
        logger.info(f"\n✓ 快照数据同步完成，共 {self.total_records} 条记录")
    
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
                data = method(code_list=all_codes, is_local=(not is_first_sync))
                
                if isinstance(data, dict):
                    # 按股票分组的数据，合并到统一表
                    all_data = []
                    for code, df in data.items():
                        if not df.empty:
                            df['market_code'] = code
                            all_data.append(df)
                    
                    if all_data:
                        import pandas as pd
                        merged_df = pd.concat(all_data, ignore_index=True)
                        # 重命名列为小写
                        merged_df.columns = [col.lower() for col in merged_df.columns]
                        
                        self.db.incremental_update(
                            data_type,
                            merged_df,
                            key_columns=["market_code", "reporting_period"],
                            date_column="reporting_period"
                        )
                        logger.info(f"✓ {name}已更新: {len(merged_df)} 条")
                    
                elif isinstance(data, __import__('pandas').DataFrame) and not data.empty:
                    # 统一表格式
                    # 重命名列为小写
                    data.columns = [col.lower() for col in data.columns]
                    
                    self.db.incremental_update(
                        data_type,
                        data,
                        key_columns=["market_code", "reporting_period"],
                        date_column="reporting_period"
                    )
                    logger.info(f"✓ {name}已更新: {len(data)} 条")
                
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
                data = method(code_list=all_codes, is_local=(not is_first_sync))
                
                if isinstance(data, __import__('pandas').DataFrame) and not data.empty:
                    # 重命名列为小写
                    data.columns = [col.lower() for col in data.columns]
                    
                    self.db.incremental_update(
                        data_type,
                        data,
                        key_columns=key_columns
                    )
                    logger.info(f"✓ {name}已更新: {len(data)} 条")
                
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
            margin_summary = self.client.get_margin_summary(is_local=(not is_first_sync))
            if not margin_summary.empty:
                # 重命名列为小写
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
        
        # 5.2 龙虎榜
        logger.info("\n5.2 同步龙虎榜...")
        try:
            dragon_tiger = self.client.get_long_hu_bang(code_list=all_codes, is_local=(not is_first_sync))
            if not dragon_tiger.empty:
                # 重命名列为小写
                dragon_tiger.columns = [col.lower() for col in dragon_tiger.columns]
                
                self.db.incremental_update(
                    "dragon_tiger",
                    dragon_tiger,
                    key_columns=["market_code", "trade_date", "trader_name"],
                    date_column="trade_date"
                )
                logger.info(f"✓ 龙虎榜已更新: {len(dragon_tiger)} 条")
        except Exception as e:
            logger.error(f"✗ 同步龙虎榜失败: {e}")
        
        # 5.3 大宗交易
        logger.info("\n5.3 同步大宗交易...")
        try:
            block_trade = self.client.get_block_trading(code_list=all_codes, is_local=(not is_first_sync))
            if not block_trade.empty:
                # 重命名列为小写
                block_trade.columns = [col.lower() for col in block_trade.columns]
                
                self.db.incremental_update(
                    "block_trade",
                    block_trade,
                    key_columns=["market_code", "trade_date"],
                    date_column="trade_date"
                )
                logger.info(f"✓ 大宗交易已更新: {len(block_trade)} 条")
        except Exception as e:
            logger.error(f"✗ 同步大宗交易失败: {e}")
    
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
    
    def get_sync_status(self):
        """获取同步状态"""
        logger.info("\n" + "=" * 60)
        logger.info("同步状态汇总")
        logger.info("=" * 60)
        
        tables = self.db.get_tables()
        
        status = {}
        for table in tables:
            count = self.db.get_table_count(table)
            if count > 0:
                status[table] = count
        
        # 按类型分组显示
        categories = {
            "基础数据": ["stock_codes", "trading_calendar", "stock_basic", "backward_factor", "adj_factor"],
            "K线数据": ["kline_daily"],
            "快照数据": ["snapshot"],
            "财务数据": ["balance_sheet", "cash_flow", "income", "profit_express", "profit_notice"],
            "股东数据": ["share_holder", "holder_num", "equity_structure"],
            "其他数据": ["margin_summary", "margin_detail", "dragon_tiger", "block_trade"],
        }
        
        for category, table_list in categories.items():
            logger.info(f"\n{category}:")
            category_tables = [t for t in table_list if t in status]
            if category_tables:
                for table in category_tables:
                    logger.info(f"  - {table}: {status[table]:,} 条")
            else:
                logger.info("  (无数据)")
        
        return status


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
        else:
            # 全量同步
            manager.sync_basic_data()
            manager.sync_kline_data(force=args.force)
            # manager.sync_snapshot_data(force=args.force)  # 快照数据量大，默认不同步
            manager.sync_financial_data(force=args.force)
            manager.sync_holder_data()
            manager.sync_other_data()
        
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
