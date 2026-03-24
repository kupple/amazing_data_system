"""
Akshare 数据采集客户端
免费接口：股票列表、个股信息、日线后复权
支持增量同步
"""
import time
import os
import akshare as ak
import pandas as pd
from datetime import datetime, timedelta
from typing import Optional, List
from src.common.logger import logger
from src.common.database import ClickHouseManager
from src.collectors import BaseCollector


class AkshareClient(BaseCollector):
    """Akshare 客户端"""

    def __init__(self):
        super().__init__("akshare")
        self._last_request_time = 0
        self._min_request_interval = 2  # 最小请求间隔（秒）
        
        # 数据库
        self._db = None

    @property
    def db(self) -> ClickHouseManager:
        """获取数据库实例"""
        if self._db is None:
            from src.common.database import get_db
            self._db = get_db()
            self._init_db()
        return self._db

    @property
    def is_connected(self) -> bool:
        return self._connected

    def connect(self) -> bool:
        """连接（无需登录）"""
        self._connected = True
        logger.info("Akshare 连接成功")
        return True

    def disconnect(self):
        """断开连接"""
        self._connected = False
        logger.info("Akshare 已断开")

    def _wait_rate_limit(self):
        """等待速率限制"""
        elapsed = time.time() - self._last_request_time
        if elapsed < self._min_request_interval:
            time.sleep(self._min_request_interval - elapsed)
        self._last_request_time = time.time()

    def _init_db(self):
        """初始化 ClickHouse 表结构"""
        # 股票列表
        self.db.client.command("""
            CREATE TABLE IF NOT EXISTS stock_list (
                sec_code String,
                name String,
                update_time DateTime DEFAULT now()
            ) ENGINE = ReplacingMergeTree(update_time)
            ORDER BY sec_code
        """)
        
        # 日线数据
        self.db.client.command("""
            CREATE TABLE IF NOT EXISTS daily_kline (
                id UInt64,
                sec_code String,
                trade_date Date,
                open Nullable(Float64),
                high Nullable(Float64),
                low Nullable(Float64),
                close Nullable(Float64),
                volume Int64,
                amount Nullable(Float64),
                amplitude Nullable(Float64),
                pct_change Nullable(Float64),
                change_value Nullable(Float64),
                turnover Nullable(Float64),
                update_time DateTime DEFAULT now()
            ) ENGINE = ReplacingMergeTree(update_time)
            ORDER BY (sec_code, trade_date)
        """)
        
        logger.info("Akshare ClickHouse 数据库初始化完成")

    def get_latest_date(self, sec_code: str) -> Optional[str]:
        """获取本地最新日期"""
        try:
            result = self.db.conn.execute("""
                SELECT MAX(trade_date) as latest_date 
                FROM daily_kline 
                WHERE sec_code = ?
            """, [sec_code]).fetchone()
            
            if result and result[0]:
                return result[0].strftime('%Y%m%d')
            return None
        except:
            return None

    def get_all_codes(self) -> List[str]:
        """获取本地所有股票代码"""
        try:
            result = self.db.conn.execute("SELECT sec_code FROM stock_list").fetchall()
            return [r[0] for r in result]
        except:
            return []

    def sync_stock_list(self, force: bool = False) -> dict:
        """同步股票列表"""
        start_time = datetime.now()
        logger.info("开始同步股票列表")
        
        try:
            # 检查是否需要更新
            if not force:
                result = self.db.conn.execute("""
                    SELECT MAX(update_time) FROM stock_list
                """).fetchone()
                
                if result and result[0]:
                    if (datetime.now() - result[0]).total_seconds() < 3600:
                        logger.info("股票列表1小时内已更新，跳过")
                        return {'success': True, 'record_count': 0, 'message': '已更新'}
            
            # 获取新数据
            self._wait_rate_limit()
            df = ak.stock_info_a_code_name()
            df.columns = ['sec_code', 'name']
            
            # 保存到数据库
            count = 0
            for _, row in df.iterrows():
                try:
                    self.db.conn.execute("""
                        INSERT OR REPLACE INTO stock_list (sec_code, name, update_time)
                        VALUES (?, ?, ?)
                    """, [row['sec_code'], row['name'], datetime.now()])
                    count += 1
                except Exception as e:
                    logger.debug(f"插入失败: {e}")
            
            self.db.conn.commit()
            
            logger.info(f"股票列表同步完成: {count} 条")
            return {'success': True, 'record_count': count}
            
        except Exception as e:
            error_msg = str(e)
            logger.error(f"股票列表同步失败: {error_msg}")
            return {'success': False, 'error': error_msg}

    def sync_daily_kline(self, sec_code: str = None, force: bool = False) -> dict:
        """同步日线数据（增量）"""
        start_time = datetime.now()
        
        # 获取要同步的股票列表
        if sec_code:
            codes = [sec_code]
        else:
            codes = self.get_all_codes()
            if not codes:
                self.sync_stock_list()
                codes = self.get_all_codes()
        
        if not codes:
            logger.warning("没有股票需要同步")
            return {'success': True, 'record_count': 0}
        
        total_count = 0
        success_count = 0
        
        logger.info(f"开始同步日线数据: {len(codes)} 只股票")
        
        for code in codes:
            try:
                # 获取增量起始日期
                if not force:
                    latest_date = self.get_latest_date(code)
                    start_date = latest_date
                else:
                    start_date = (datetime.now() - timedelta(days=365)).strftime('%Y%m%d')
                
                end_date = datetime.now().strftime('%Y%m%d')
                
                if start_date == end_date:
                    logger.debug(f"{code} 已是最新，跳过")
                    continue
                
                self._wait_rate_limit()
                
                # 获取数据
                code_raw = code.replace('.SH', '').replace('.SZ', '')
                df = ak.stock_zh_a_hist(
                    symbol=code_raw,
                    start_date=start_date,
                    end_date=end_date,
                    adjust="qfq"
                )
                
                if df is None or df.empty:
                    continue
                
                # 重命名列
                df = df.rename(columns={
                    '日期': 'trade_date',
                    '股票代码': 'sec_code',
                    '开盘': 'open',
                    '收盘': 'close',
                    '最高': 'high',
                    '最低': 'low',
                    '成交量': 'volume',
                    '成交额': 'amount',
                    '振幅': 'amplitude',
                    '涨跌幅': 'pct_change',
                    '涨跌额': 'change_value',
                    '换手率': 'turnover'
                })
                
                df['sec_code'] = code
                df['update_time'] = datetime.now()
                
                # 插入数据
                count = 0
                for _, row in df.iterrows():
                    try:
                        self.db.conn.execute("""
                            INSERT OR IGNORE INTO daily_kline 
                            (sec_code, trade_date, open, high, low, close, volume, amount, 
                             amplitude, pct_change, change_value, turnover, update_time)
                            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                        """, [
                            row['sec_code'], row['trade_date'], row['open'], row['high'],
                            row['low'], row['close'], row['volume'], row['amount'],
                            row['amplitude'], row['pct_change'], row['change_value'], 
                            row['turnover'], row['update_time']
                        ])
                        count += 1
                    except:
                        pass
                
                self.db.conn.commit()
                total_count += count
                success_count += 1
                
                logger.debug(f"{code}: {count} 条")
                
            except Exception as e:
                logger.debug(f"{code} 失败: {e}")
        
        logger.info(f"日线同步完成: {success_count}/{len(codes)} 只成功, {total_count} 条数据")
        
        return {
            'success': True, 
            'record_count': total_count,
            'success_count': success_count
        }

    def sync_all(self, force: bool = False) -> dict:
        """同步所有数据"""
        results = {}
        results['stock_list'] = self.sync_stock_list(force=force)
        results['daily_kline'] = self.sync_daily_kline(force=force)
        return results

    def get_stock_list(self) -> pd.DataFrame:
        """获取股票列表 (从数据库)"""
        return self.db.conn.execute("SELECT * FROM stock_list").df()

    def get_daily_kline(self, 
                       sec_code: str, 
                       start_date: str = None, 
                       end_date: str = None,
                       adjust: str = "qfq") -> pd.DataFrame:
        """获取日线数据 (从数据库)"""
        query = "SELECT * FROM daily_kline WHERE sec_code = ?"
        params = [sec_code]
        
        if start_date:
            query += " AND trade_date >= ?"
            params.append(start_date)
        if end_date:
            query += " AND trade_date <= ?"
            params.append(end_date)
        
        query += " ORDER BY trade_date"
        
        return self.db.conn.execute(query, params).df()

    def get_kline(self, sec_code: str, start_date: str = None, end_date: str = None, **kwargs) -> pd.DataFrame:
        """获取K线 (从数据库)"""
        return self.get_daily_kline(sec_code, start_date, end_date)

    def get_sync_status(self) -> dict:
        """获取同步状态"""
        try:
            stock_count = self.db.conn.execute("SELECT COUNT(*) FROM stock_list").fetchone()[0]
            kline_count = self.db.conn.execute("SELECT COUNT(*) FROM daily_kline").fetchone()[0]
            
            return {
                'stock_count': stock_count,
                'kline_count': kline_count
            }
        except Exception as e:
            logger.error(f"获取同步状态失败: {e}")
            return {}

    def get_realtime_quote(self, sec_codes: List[str]) -> pd.DataFrame:
        """获取实时行情"""
        try:
            self._wait_rate_limit()
            codes = [c.replace('.SH', '').replace('.SZ', '') for c in sec_codes]
            df = ak.stock_zh_a_spot_em()
            df = df[df['代码'].isin(codes)]
            return df
        except Exception as e:
            logger.error(f"获取实时行情失败: {e}")
            raise


# 全局客户端
_client = None

def get_client() -> AkshareClient:
    """获取客户端实例"""
    global _client
    if _client is None:
        _client = AkshareClient()
        _client.connect()
    return _client

def close_client():
    """关闭客户端"""
    global _client
    if _client:
        _client.disconnect()
        _client = None
