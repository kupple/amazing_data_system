"""
Baostock 数据采集客户端
免费接口：日线后复权数据
支持增量同步
"""
import time
import baostock as bs
import pandas as pd
from datetime import datetime, timedelta
from typing import Optional, List
from src.common.logger import logger
from src.common.database import DuckDBManager
from src.collectors import BaseCollector


class BaostockClient(BaseCollector):
    """Baostock 客户端"""

    def __init__(self, db_path: str = "./data/baostock_data.duckdb"):
        super().__init__("baostock")
        self._last_request_time = 0
        self._min_request_interval = 1  # 最小请求间隔（秒）
        
        # 数据库
        self._db = None
        self._db_path = db_path

    @property
    def db(self) -> DuckDBManager:
        """获取数据库实例"""
        if self._db is None:
            self._db = DuckDBManager(self._db_path)
            self._init_db()
        return self._db

    @property
    def is_connected(self) -> bool:
        return self._connected

    def connect(self) -> bool:
        """登录 Baostock"""
        try:
            lg = bs.login()
            if lg.error_code == '0':
                self._connected = True
                logger.info("Baostock 登录成功")
                return True
            else:
                logger.error(f"Baostock 登录失败: {lg.error_msg}")
                return False
        except Exception as e:
            logger.error(f"Baostock 连接失败: {e}")
            return False

    def disconnect(self):
        """登出"""
        if self._connected:
            bs.logout()
            self._connected = False
            logger.info("Baostock 已登出")

    def _wait_rate_limit(self):
        """等待速率限制"""
        elapsed = time.time() - self._last_request_time
        if elapsed < self._min_request_interval:
            time.sleep(self._min_request_interval - elapsed)
        self._last_request_time = time.time()

    def _init_db(self):
        """初始化数据库表"""
        # 股票列表 (沪市sh. 深市sz.)
        self._db.conn.execute("""
            CREATE TABLE IF NOT EXISTS stock_list (
                sec_code VARCHAR PRIMARY KEY,
                code_name VARCHAR,
                update_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        
        # 日线数据
        self._db.conn.execute("""
            CREATE TABLE IF NOT EXISTS daily_kline (
                id INTEGER PRIMARY KEY,
                sec_code VARCHAR,
                trade_date DATE,
                open DOUBLE,
                high DOUBLE,
                low DOUBLE,
                close DOUBLE,
                volume BIGINT,
                amount DOUBLE,
                turn DOUBLE,
                update_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(sec_code, trade_date)
            )
        """)
        
        # 创建索引
        self._db.conn.execute("CREATE INDEX IF NOT EXISTS idx_kline_code ON daily_kline(sec_code)")
        self._db.conn.execute("CREATE INDEX IF NOT EXISTS idx_kline_date ON daily_kline(trade_date)")
        
        self._db.conn.commit()
        logger.info("Baostock 数据库初始化完成")

    def get_latest_date(self, sec_code: str) -> Optional[str]:
        """获取本地最新日期"""
        try:
            result = self.db.conn.execute("""
                SELECT MAX(trade_date) as latest_date 
                FROM daily_kline 
                WHERE sec_code = ?
            """, [sec_code]).fetchone()
            
            if result and result[0]:
                return result[0].strftime('%Y-%m-%d')
            return None
        except:
            return None
    
    def get_all_codes(self) -> List[str]:
        """获取本地所有股票代码 - 如果为空返回常用股票"""
        try:
            result = self.db.conn.execute("SELECT sec_code FROM stock_list").fetchall()
            codes = [r[0] for r in result]
            if not codes:
                # 返回常用股票代码
                return ['600000.SH', '000001.SH', '000002.SH', '600519.SH', '300750.SH']
            return codes
        except:
            return ['600000.SH', '000001.SH', '000002.SH', '600519.SH', '300750.SH']

    def get_stock_list(self) -> pd.DataFrame:
        """获取股票列表 (从数据库)"""
        return self.db.conn.execute("SELECT * FROM stock_list").df()

    def sync_stock_list(self) -> dict:
        """同步股票列表"""
        logger.info("开始同步股票列表")
        
        try:
            rs = bs.query_stock_basic()
            
            count = 0
            while (rs.error_code == '0') and rs.next():
                row = rs.get_row_data()
                if len(row) >= 2:
                    code = row[0]
                    name = row[1]
                    # 转换代码格式
                    if code.startswith('sh.'):
                        sec_code = code.replace('sh.', '') + '.SH'
                    elif code.startswith('sz.'):
                        sec_code = code.replace('sz.', '') + '.SZ'
                    else:
                        sec_code = code
                    
                    try:
                        self.db.conn.execute("""
                            INSERT OR REPLACE INTO stock_list (sec_code, code_name, update_time)
                            VALUES (?, ?, ?)
                        """, [sec_code, name, datetime.now()])
                        count += 1
                    except:
                        pass
            
            self.db.conn.commit()
            logger.info(f"股票列表同步完成: {count} 条")
            return {'success': True, 'record_count': count}
            
        except Exception as e:
            logger.error(f"股票列表同步失败: {e}")
            return {'success': False, 'error': str(e)}

    def sync_daily_kline(self, sec_code: str = None, force: bool = False) -> dict:
        """同步日线数据（增量）"""
        
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
                # 转换代码格式 baostock 需要 sh.xxxxxx 或 sz.xxxxxx
                if code.endswith('.SH'):
                    bs_code = 'sh.' + code.replace('.SH', '')
                elif code.endswith('.SZ'):
                    bs_code = 'sz.' + code.replace('.SZ', '')
                else:
                    bs_code = code
                
                # 获取增量起始日期
                if not force:
                    latest_date = self.get_latest_date(code)
                    if latest_date:
                        start_date = (datetime.strptime(latest_date, '%Y-%m-%d') + timedelta(days=1)).strftime('%Y-%m-%d')
                    else:
                        start_date = (datetime.now() - timedelta(days=365)).strftime('%Y-%m-%d')
                else:
                    start_date = (datetime.now() - timedelta(days=365)).strftime('%Y-%m-%d')
                
                end_date = datetime.now().strftime('%Y-%m-%d')
                
                if start_date >= end_date:
                    logger.debug(f"{code} 已是最新，跳过")
                    continue
                
                self._wait_rate_limit()
                
                # 获取数据 (adjustflag='2' 表示后复权)
                rs = bs.query_history_k_data_plus(
                    bs_code,
                    'date,code,open,high,low,close,volume,amount,turn',
                    start_date=start_date,
                    end_date=end_date,
                    frequency='d',
                    adjustflag='2'
                )
                
                if rs.error_code != '0':
                    logger.debug(f"{code} 获取失败: {rs.error_msg}")
                    continue
                
                count = 0
                while rs.next():
                    row = rs.get_row_data()
                    if len(row) >= 9:
                        try:
                            self.db.conn.execute("""
                                INSERT OR IGNORE INTO daily_kline 
                                (sec_code, trade_date, open, high, low, close, volume, amount, turn, update_time)
                                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                            """, [
                                code, row[0], 
                                float(row[2]) if row[2] else None,
                                float(row[3]) if row[3] else None,
                                float(row[4]) if row[4] else None,
                                float(row[5]) if row[5] else None,
                                int(float(row[6])) if row[6] else 0,
                                float(row[7]) if row[7] else None,
                                float(row[8]) if row[8] else None,
                                datetime.now()
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
        results['stock_list'] = self.sync_stock_list()
        results['daily_kline'] = self.sync_daily_kline(force=force)
        return results

    def get_daily_kline(self, 
                       sec_code: str, 
                       start_date: str = None, 
                       end_date: str = None,
                       adjust: str = "hfq") -> pd.DataFrame:
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
        """Baostock 不支持实时行情"""
        logger.warning("Baostock 不支持实时行情")
        return pd.DataFrame()


# 全局客户端
_client = None

def get_client(db_path: str = "./data/baostock_data.duckdb") -> BaostockClient:
    """获取客户端实例"""
    global _client
    if _client is None:
        _client = BaostockClient(db_path)
        _client.connect()
    return _client

def close_client():
    """关闭客户端"""
    global _client
    if _client:
        _client.disconnect()
        _client = None
