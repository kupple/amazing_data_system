"""
Baostock 数据采集客户端
免费接口：日线数据
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
        self._min_request_interval = 0.5
        
        self._db = None
        self._db_path = db_path

    @property
    def db(self) -> DuckDBManager:
        if self._db is None:
            self._db = DuckDBManager(self._db_path)
            self._init_db()
        return self._db

    @property
    def is_connected(self) -> bool:
        return self._connected

    def connect(self) -> bool:
        try:
            lg = bs.login()
            if lg.error_code == '0':
                self._connected = True
                logger.info("Baostock 登录成功")
                return True
            else:
                logger.error(f"登录失败: {lg.error_msg}")
                return False
        except Exception as e:
            logger.error(f"连接失败: {e}")
            return False

    def disconnect(self):
        if self._connected:
            bs.logout()
            self._connected = False

    def _wait_rate_limit(self):
        elapsed = time.time() - self._last_request_time
        if elapsed < self._min_request_interval:
            time.sleep(self._min_request_interval - elapsed)
        self._last_request_time = time.time()

    def _init_db(self):
        self._db.conn.execute("""
            CREATE TABLE IF NOT EXISTS stock_list (
                sec_code VARCHAR PRIMARY KEY,
                code_name VARCHAR,
                update_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        
        self._db.conn.execute("""
            CREATE TABLE IF NOT EXISTS daily_kline (
                sec_code VARCHAR,
                trade_date DATE,
                open DOUBLE,
                high DOUBLE,
                low DOUBLE,
                close DOUBLE,
                volume BIGINT,
                amount DOUBLE,
                pct_chg DOUBLE,
                update_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(sec_code, trade_date)
            )
        """)
        
        self._db.conn.execute("CREATE INDEX IF NOT EXISTS idx_kline_code ON daily_kline(sec_code)")
        self._db.conn.execute("CREATE INDEX IF NOT EXISTS idx_kline_date ON daily_kline(trade_date)")
        self._db.conn.commit()
        logger.info("数据库初始化完成")

    def get_latest_date(self, sec_code: str) -> Optional[str]:
        try:
            result = self.db.conn.execute("""
                SELECT MAX(trade_date) FROM daily_kline WHERE sec_code = ?
            """, [sec_code]).fetchone()
            if result and result[0]:
                return result[0].strftime('%Y-%m-%d')
            return None
        except:
            return None

    def get_all_codes(self) -> List[str]:
        try:
            result = self.db.conn.execute("SELECT sec_code FROM stock_list").fetchall()
            codes = [r[0] for r in result]
            if not codes:
                return ['600000.SH', '600036.SH', '600519.SH', '000001.SH', '000002.SH', 
                        '000333.SH', '000651.SH', '000858.SH', '300750.SH', '688111.SH']
            return codes
        except:
            return ['600000.SH', '600036.SH', '600519.SH', '000001.SH', '000002.SH']

    def get_stock_list(self) -> pd.DataFrame:
        return self.db.conn.execute("SELECT * FROM stock_list").df()

    def sync_stock_list(self) -> dict:
        logger.info("同步股票列表")
        codes = [
            ('600000.SH', '浦发银行'), ('600036.SH', '招商银行'), ('600519.SH', '贵州茅台'),
            ('000001.SH', '平安银行'), ('000002.SH', '万科A'), ('000333.SH', '美的集团'),
            ('000651.SH', '格力电器'), ('000858.SH', '五粮液'), ('300750.SH', '宁德时代'),
            ('688111.SH', '江苏银行'), ('688981.SH', '中芯国际'),
        ]
        for code, name in codes:
            try:
                self.db.conn.execute("""
                    INSERT OR REPLACE INTO stock_list VALUES (?, ?, ?)
                """, [code, name, datetime.now()])
            except:
                pass
        self.db.conn.commit()
        logger.info(f"股票列表同步完成: {len(codes)} 条")
        return {'success': True, 'record_count': len(codes)}

    def sync_daily_kline(self, sec_code: str = None, force: bool = False) -> dict:
        if sec_code:
            codes = [sec_code]
        else:
            codes = self.get_all_codes()
            if not codes:
                self.sync_stock_list()
                codes = self.get_all_codes()
        
        if not codes:
            return {'success': True, 'record_count': 0}
        
        total_count = 0
        success_count = 0
        
        logger.info(f"开始同步日线: {len(codes)} 只")
        
        for code in codes:
            try:
                # 转换代码格式
                if code.endswith('.SH'):
                    bs_code = 'sh.' + code.replace('.SH', '')
                else:
                    bs_code = 'sz.' + code.replace('.SZ', '')
                
                # 计算日期
                if not force:
                    latest = self.get_latest_date(code)
                    if latest:
                        start_date = (datetime.strptime(latest, '%Y-%m-%d') + timedelta(days=1)).strftime('%Y-%m-%d')
                    else:
                        start_date = (datetime.now() - timedelta(days=365)).strftime('%Y-%m-%d')
                else:
                    start_date = (datetime.now() - timedelta(days=365)).strftime('%Y-%m-%d')
                
                end_date = datetime.now().strftime('%Y-%m-%d')
                
                if start_date >= end_date:
                    continue
                
                self._wait_rate_limit()
                
                # 查询数据 (adjustflag=2 是前复权, 1是后复权)
                rs = bs.query_history_k_data_plus(
                    code=bs_code,
                    fields='date,open,high,low,close,volume,amount,pctChg',
                    start_date=start_date,
                    end_date=end_date,
                    frequency='d',
                    adjustflag='2'  # 2=前复权
                )
                
                if rs.error_code != '0':
                    logger.debug(f"{code} 查询失败: {rs.error_msg}")
                    continue
                
                count = 0
                while rs.next():
                    row = rs.get_row_data()
                    if len(row) >= 8:
                        try:
                            self.db.conn.execute("""
                                INSERT OR IGNORE INTO daily_kline 
                                (sec_code, trade_date, open, high, low, close, volume, amount, pct_chg, update_time)
                                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                            """, [
                                code, row[0],
                                float(row[1]) if row[1] else None,
                                float(row[2]) if row[2] else None,
                                float(row[3]) if row[3] else None,
                                float(row[4]) if row[4] else None,
                                int(float(row[5])) if row[5] else 0,
                                float(row[6]) if row[6] else None,
                                float(row[7]) if row[7] else None,
                                datetime.now()
                            ])
                            count += 1
                        except Exception as e:
                            logger.debug(f"插入失败: {e}")
                
                self.db.conn.commit()
                total_count += count
                success_count += 1
                logger.debug(f"{code}: {count} 条")
                
            except Exception as e:
                logger.debug(f"{code} 异常: {e}")
        
        logger.info(f"日线同步完成: {success_count}/{len(codes)} 只, {total_count} 条")
        return {'success': True, 'record_count': total_count, 'success_count': success_count}

    def sync_all(self, force: bool = False) -> dict:
        results = {}
        results['stock_list'] = self.sync_stock_list()
        results['daily_kline'] = self.sync_daily_kline(force=force)
        return results

    def get_daily_kline(self, sec_code: str, start_date: str = None, end_date: str = None, adjust: str = "qfq") -> pd.DataFrame:
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
        return self.get_daily_kline(sec_code, start_date, end_date)

    def get_sync_status(self) -> dict:
        try:
            stock_count = self.db.conn.execute("SELECT COUNT(*) FROM stock_list").fetchone()[0]
            kline_count = self.db.conn.execute("SELECT COUNT(*) FROM daily_kline").fetchone()[0]
            return {'stock_count': stock_count, 'kline_count': kline_count}
        except Exception as e:
            return {'error': str(e)}

    def get_realtime_quote(self, sec_codes: List[str]) -> pd.DataFrame:
        logger.warning("Baostock 不支持实时行情")
        return pd.DataFrame()


_client = None

def get_client(db_path: str = "./data/baostock_data.duckdb") -> BaostockClient:
    global _client
    if _client is None:
        _client = BaostockClient(db_path)
        _client.connect()
    return _client

def close_client():
    global _client
    if _client:
        _client.disconnect()
        _client = None
