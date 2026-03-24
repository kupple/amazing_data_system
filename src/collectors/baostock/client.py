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
                ipo_date DATE,
                out_date DATE,
                stock_type VARCHAR,
                trade_status VARCHAR,
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
                turn DOUBLE,
                trade_status VARCHAR,
                is_st VARCHAR,
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
            result = self.db.conn.execute(
                "SELECT MAX(trade_date) FROM daily_kline WHERE sec_code = ?", [sec_code]
            ).fetchone()
            if result and result[0]:
                return result[0].strftime('%Y-%m-%d')
            return None
        except:
            return None

    def get_all_codes(self) -> List[str]:
        try:
            result = self.db.conn.execute("SELECT sec_code FROM stock_list where stock_type = '1'").fetchall()
            print(result)
            codes = [r[0] for r in result]
            if not codes:
                return []
            return codes
        except:
            return ['600000.SH', '600036.SH', '600519.SH', '000001.SH', '000002.SH']

    def get_stock_list(self) -> pd.DataFrame:
        return self.db.conn.execute("SELECT * FROM stock_list").df()

    def sync_stock_list(self) -> dict:
        logger.info("同步股票列表...")
        rs = bs.query_stock_basic()
        if rs.error_code != '0':
            logger.error(f"获取股票列表失败: {rs.error_msg}")
            return {'success': False, 'error': rs.error_msg}

        count = 0
        while rs.next():
            row = rs.get_row_data()
            code = row[0]
            if code and (code.startswith('sh.') or code.startswith('sz.')):
                sec_code = code.replace('sh.', '') + '.SH' if code.startswith('sh.') else code.replace('sz.', '') + '.SZ'
                try:
                    self.db.conn.execute("""
                        INSERT OR REPLACE INTO stock_list
                        (sec_code, code_name, ipo_date, out_date, stock_type, trade_status, update_time)
                        VALUES (?, ?, ?, ?, ?, ?, ?)
                    """, [sec_code, row[1], row[2] or None, row[3] or None, row[4], row[5], datetime.now()])
                    count += 1
                except:
                    pass

        self.db.conn.commit()
        logger.info(f"股票列表同步完成: {count} 条")
        return {'success': True, 'record_count': count}

    def sync_daily_kline(self, sec_code: str = None, force: bool = False) -> dict:
        codes = [sec_code] if sec_code else self.get_all_codes()
        if not codes:
            return {'success': True, 'record_count': 0}

        total_count = 0
        success_count = 0
        logger.info(f"开始同步日线: {len(codes)} 只")

        for code in codes:
            try:
                bs_code = 'sh.' + code.replace('.SH', '') if code.endswith('.SH') else 'sz.' + code.replace('.SZ', '')

                if not force:
                    latest = self.get_latest_date(code)
                    start_date = (datetime.strptime(latest, '%Y-%m-%d') + timedelta(days=1)).strftime('%Y-%m-%d') if latest else '1990-01-01'
                else:
                    start_date = '1990-01-01'

                end_date = datetime.now().strftime('%Y-%m-%d')
                if start_date >= end_date:
                    continue

                self._wait_rate_limit()

                rs = bs.query_history_k_data_plus(
                    code=bs_code,
                    fields='date,open,high,low,close,volume,amount,pctChg,turn,tradestatus,isST',
                    start_date=start_date, end_date=end_date,
                    frequency='d', adjustflag='1'
                )

                if rs.error_code != '0':
                    continue

                count = 0
                while rs.next():
                    row = rs.get_row_data()
                    if len(row) >= 8:
                        try:
                            self.db.conn.execute("""
                                INSERT OR IGNORE INTO daily_kline
                                (sec_code, trade_date, open, high, low, close, volume, amount, pct_chg, turn, trade_status, is_st, update_time)
                                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                            """, [
                                code, row[0],
                                float(row[1]) if row[1] else None,
                                float(row[2]) if row[2] else None,
                                float(row[3]) if row[3] else None,
                                float(row[4]) if row[4] else None,
                                int(float(row[5])) if row[5] else 0,
                                float(row[6]) if row[6] else None,
                                float(row[7]) if row[7] else None,
                                float(row[8]) if row[8] else None,
                                row[9] if len(row) > 9 else None,
                                row[10] if len(row) > 10 else None,
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
                # Skip sync_log for now

        logger.info(f"日线同步完成: {success_count}/{len(codes)} 只, {total_count} 条")
        return {'success': True, 'record_count': total_count, 'success_count': success_count}

    def sync_all(self, force: bool = False) -> dict:
        return {
            'stock_list': self.sync_stock_list(),
            'daily_kline': self.sync_daily_kline(force=force)
        }

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

def get_client(db_path: str = "./data/baostock_full.duckdb") -> BaostockClient:
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
