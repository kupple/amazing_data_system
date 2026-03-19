"""
Baostock 数据采集客户端
免费接口：日线数据
"""
import time
import baostock as bs
import pandas as pd
import psycopg2
from datetime import datetime, timedelta
from typing import Optional, List
from src.common.logger import logger
from src.collectors import BaseCollector


class BaostockClient(BaseCollector):
    """Baostock 客户端"""

    def __init__(self, dsn: str = "host=localhost port=5433 dbname=main"):
        super().__init__("baostock")
        self._last_request_time = 0
        self._min_request_interval = 0.5
        self._conn = None
        self._dsn = dsn

    @property
    def conn(self):
        if self._conn is None or self._conn.closed:
            self._conn = psycopg2.connect(self._dsn)
            self._conn.autocommit = False
            self._init_db()
        return self._conn

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
        if self._conn and not self._conn.closed:
            self._conn.close()

    def _wait_rate_limit(self):
        elapsed = time.time() - self._last_request_time
        if elapsed < self._min_request_interval:
            time.sleep(self._min_request_interval - elapsed)
        self._last_request_time = time.time()

    def _init_db(self):
        cur = self.conn.cursor()
        cur.execute("""
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
        cur.execute("""
            CREATE TABLE IF NOT EXISTS daily_kline (
                sec_code VARCHAR,
                trade_date DATE,
                open DOUBLE PRECISION,
                high DOUBLE PRECISION,
                low DOUBLE PRECISION,
                close DOUBLE PRECISION,
                volume BIGINT,
                amount DOUBLE PRECISION,
                pct_chg DOUBLE PRECISION,
                turn DOUBLE PRECISION,
                trade_status VARCHAR,
                is_st VARCHAR,
                update_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(sec_code, trade_date)
            )
        """)
        cur.execute("""
            CREATE TABLE IF NOT EXISTS sync_log (
                id SERIAL PRIMARY KEY,
                sec_code VARCHAR,
                data_type VARCHAR,
                success INTEGER,
                record_count INTEGER,
                error_message TEXT,
                update_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        cur.execute("CREATE INDEX IF NOT EXISTS idx_kline_code ON daily_kline(sec_code)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_kline_date ON daily_kline(trade_date)")
        self.conn.commit()
        cur.close()
        logger.info("数据库初始化完成")

    def get_latest_date(self, sec_code: str) -> Optional[str]:
        try:
            cur = self.conn.cursor()
            cur.execute("SELECT MAX(trade_date) FROM daily_kline WHERE sec_code = %s", (sec_code,))
            result = cur.fetchone()
            cur.close()
            if result and result[0]:
                return result[0].strftime('%Y-%m-%d')
            return None
        except:
            return None

    def get_all_codes(self) -> List[str]:
        try:
            cur = self.conn.cursor()
            cur.execute("SELECT sec_code FROM stock_list")
            result = cur.fetchall()
            cur.close()
            codes = [r[0] for r in result]
            if not codes:
                return ['600000.SH', '600036.SH', '600519.SH', '000001.SH', '000002.SH',
                        '000333.SH', '000651.SH', '000858.SH', '300750.SH', '688111.SH']
            return codes
        except:
            return ['600000.SH', '600036.SH', '600519.SH', '000001.SH', '000002.SH']

    def get_stock_list(self) -> pd.DataFrame:
        return pd.read_sql("SELECT * FROM stock_list", self.conn)

    def sync_stock_list(self) -> dict:
        logger.info("同步股票列表（从 Baostock 获取全部A股）...")
        rs = bs.query_stock_basic()
        if rs.error_code != '0':
            logger.error(f"获取股票列表失败: {rs.error_msg}")
            return {'success': False, 'error': rs.error_msg}

        cur = self.conn.cursor()
        count = 0
        while rs.next():
            row = rs.get_row_data()
            code = row[0]
            if code and (code.startswith('sh.') or code.startswith('sz.')):
                if code.startswith('sh.'):
                    sec_code = code.replace('sh.', '') + '.SH'
                else:
                    sec_code = code.replace('sz.', '') + '.SZ'
                try:
                    cur.execute("""
                        INSERT INTO stock_list
                        (sec_code, code_name, ipo_date, out_date, stock_type, trade_status, update_time)
                        VALUES (%s, %s, %s, %s, %s, %s, %s)
                        ON CONFLICT (sec_code) DO UPDATE SET
                            code_name = EXCLUDED.code_name,
                            ipo_date = EXCLUDED.ipo_date,
                            out_date = EXCLUDED.out_date,
                            stock_type = EXCLUDED.stock_type,
                            trade_status = EXCLUDED.trade_status,
                            update_time = EXCLUDED.update_time
                    """, (
                        sec_code, row[1],
                        row[2] if row[2] else None,
                        row[3] if row[3] else None,
                        row[4], row[5], datetime.now()
                    ))
                    count += 1
                except:
                    pass
        self.conn.commit()
        cur.close()
        logger.info(f"股票列表同步完成: {count} 条")
        return {'success': True, 'record_count': count}

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
        cur = self.conn.cursor()

        for code in codes:
            try:
                if code.endswith('.SH'):
                    bs_code = 'sh.' + code.replace('.SH', '')
                else:
                    bs_code = 'sz.' + code.replace('.SZ', '')

                if not force:
                    latest = self.get_latest_date(code)
                    if latest:
                        start_date = (datetime.strptime(latest, '%Y-%m-%d') + timedelta(days=1)).strftime('%Y-%m-%d')
                    else:
                        start_date = '1990-01-01'
                else:
                    start_date = '1990-01-01'

                end_date = datetime.now().strftime('%Y-%m-%d')
                if start_date >= end_date:
                    continue

                self._wait_rate_limit()

                rs = bs.query_history_k_data_plus(
                    code=bs_code,
                    fields='date,open,high,low,close,volume,amount,pctChg,turn,tradestatus,isST',
                    start_date=start_date,
                    end_date=end_date,
                    frequency='d',
                    adjustflag='1'
                )

                if rs.error_code != '0':
                    logger.debug(f"{code} 查询失败: {rs.error_msg}")
                    continue

                count = 0
                while rs.next():
                    row = rs.get_row_data()
                    if len(row) >= 8:
                        try:
                            cur.execute("""
                                INSERT INTO daily_kline
                                (sec_code, trade_date, open, high, low, close, volume, amount, pct_chg, turn, trade_status, is_st, update_time)
                                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                                ON CONFLICT (sec_code, trade_date) DO NOTHING
                            """, (
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
                            ))
                            count += 1
                        except Exception as e:
                            logger.debug(f"插入失败: {e}")

                self.conn.commit()
                total_count += count
                success_count += 1

                cur.execute("""
                    INSERT INTO sync_log (sec_code, data_type, success, record_count, error_message, update_time)
                    VALUES (%s, %s, %s, %s, %s, %s)
                """, (code, 'daily_kline', 1, count, '', datetime.now()))
                self.conn.commit()
                logger.debug(f"{code}: {count} 条")

            except Exception as e:
                logger.debug(f"{code} 异常: {e}")
                cur.execute("""
                    INSERT INTO sync_log (sec_code, data_type, success, record_count, error_message, update_time)
                    VALUES (%s, %s, %s, %s, %s, %s)
                """, (code, 'daily_kline', 0, 0, str(e), datetime.now()))
                self.conn.commit()

        cur.close()
        logger.info(f"日线同步完成: {success_count}/{len(codes)} 只, {total_count} 条")
        return {'success': True, 'record_count': total_count, 'success_count': success_count}

    def sync_all(self, force: bool = False) -> dict:
        results = {}
        results['stock_list'] = self.sync_stock_list()
        results['daily_kline'] = self.sync_daily_kline(force=force)
        return results

    def get_daily_kline(self, sec_code: str, start_date: str = None, end_date: str = None, adjust: str = "qfq") -> pd.DataFrame:
        query = "SELECT * FROM daily_kline WHERE sec_code = %s"
        params = [sec_code]
        if start_date:
            query += " AND trade_date >= %s"
            params.append(start_date)
        if end_date:
            query += " AND trade_date <= %s"
            params.append(end_date)
        query += " ORDER BY trade_date"
        return pd.read_sql(query, self.conn, params=params)

    def get_kline(self, sec_code: str, start_date: str = None, end_date: str = None, **kwargs) -> pd.DataFrame:
        return self.get_daily_kline(sec_code, start_date, end_date)

    def get_sync_status(self) -> dict:
        try:
            cur = self.conn.cursor()
            cur.execute("SELECT COUNT(*) FROM stock_list")
            stock_count = cur.fetchone()[0]
            cur.execute("SELECT COUNT(*) FROM daily_kline")
            kline_count = cur.fetchone()[0]
            cur.close()
            return {'stock_count': stock_count, 'kline_count': kline_count}
        except Exception as e:
            return {'error': str(e)}

    def get_realtime_quote(self, sec_codes: List[str]) -> pd.DataFrame:
        logger.warning("Baostock 不支持实时行情")
        return pd.DataFrame()


_client = None

def get_client(dsn: str = "host=localhost port=5433 dbname=main") -> BaostockClient:
    global _client
    if _client is None:
        _client = BaostockClient(dsn)
        _client.connect()
    return _client

def close_client():
    global _client
    if _client:
        _client.disconnect()
        _client = None
