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
from src.common.database import ClickHouseManager
from src.collectors import BaseCollector


class BaostockClient(BaseCollector):
    """Baostock 客户端"""

    def __init__(self):
        super().__init__("baostock")
        self._last_request_time = 0
        self._min_request_interval = 0.5
        self._db = None

    @property
    def db(self) -> ClickHouseManager:
        if self._db is None:
            from src.common.database import get_db
            self._db = get_db()
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
        """初始化 ClickHouse 表结构"""
        self.db.client.command("""
            CREATE TABLE IF NOT EXISTS stock_list (
                sec_code String,
                code_name String,
                ipo_date Nullable(Date),
                out_date Nullable(Date),
                stock_type String,
                trade_status String,
                update_time DateTime DEFAULT now()
            ) ENGINE = ReplacingMergeTree(update_time)
            ORDER BY sec_code
        """)
        
        self.db.client.command("""
            CREATE TABLE IF NOT EXISTS daily_kline (
                sec_code String,
                trade_date Date,
                open Nullable(Float64),
                high Nullable(Float64),
                low Nullable(Float64),
                close Nullable(Float64),
                volume Int64,
                amount Nullable(Float64),
                pct_chg Nullable(Float64),
                turn Nullable(Float64),
                trade_status Nullable(String),
                is_st Nullable(String),
                update_time DateTime DEFAULT now()
            ) ENGINE = ReplacingMergeTree(update_time)
            ORDER BY (sec_code, trade_date)
        """)
        
        self.db.client.command("""
            CREATE TABLE IF NOT EXISTS sync_log (
                id UInt64,
                sec_code String,
                data_type String,
                success UInt8,
                record_count UInt32,
                error_message String,
                update_time DateTime DEFAULT now()
            ) ENGINE = MergeTree()
            ORDER BY (sec_code, update_time)
        """)
        
        logger.info("ClickHouse 数据库初始化完成")

    def get_latest_date(self, sec_code: str) -> Optional[str]:
        try:
            result = self.db.client.query(
                f"SELECT MAX(trade_date) FROM daily_kline WHERE sec_code = '{sec_code}'"
            )
            if result.result_rows and result.result_rows[0][0]:
                return str(result.result_rows[0][0])
            return None
        except:
            return None

    def get_all_codes(self) -> List[str]:
        try:
            # 先尝试从数据库获取
            result = self.db.client.query("SELECT sec_code FROM stock_list")
            codes = [r[0] for r in result.result_rows]
            if codes:
                return codes
        except:
            pass
        
        # 数据库没有则使用常用股票列表（A股主要股票）
        logger.info("使用常用A股列表...")
        codes = [
          
        ]
        logger.info(f"使用股票列表: {len(codes)} 只")
        return codes

    def get_stock_list(self) -> pd.DataFrame:
        return self.db.query("SELECT * FROM stock_list")

    def sync_stock_list(self) -> dict:
        logger.info("同步股票列表...")
        rs = bs.query_stock_basic()
        if rs.error_code != '0':
            logger.error(f"获取股票列表失败: {rs.error_msg}")
            return {'success': False, 'error': rs.error_msg}

        records = []
        while rs.next():
            row = rs.get_row_data()
            code = row[0]
            if code and (code.startswith('sh.') or code.startswith('sz.')):
                sec_code = code.replace('sh.', '') + '.SH' if code.startswith('sh.') else code.replace('sz.', '') + '.SZ'
                records.append({
                    'sec_code': sec_code,
                    'code_name': row[1],
                    'ipo_date': row[2] if row[2] else None,
                    'out_date': row[3] if row[3] else None,
                    'stock_type': row[4],
                    'trade_status': row[5],
                    'update_time': datetime.now()
                })

        if records:
            df = pd.DataFrame(records)
            self.db.insert_dataframe(df, 'stock_list', if_exists='append')
            logger.info(f"股票列表同步完成: {len(records)} 条")
            return {'success': True, 'record_count': len(records)}
        
        return {'success': True, 'record_count': 0}

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

                records = []
                while rs.next():
                    row = rs.get_row_data()
                    if len(row) >= 8:
                        # 将日期字符串转换为date对象
                        trade_date = datetime.strptime(row[0], '%Y-%m-%d').date() if row[0] else None
                        records.append({
                            'sec_code': code,
                            'trade_date': trade_date,
                            'open': float(row[1]) if row[1] else None,
                            'high': float(row[2]) if row[2] else None,
                            'low': float(row[3]) if row[3] else None,
                            'close': float(row[4]) if row[4] else None,
                            'volume': int(float(row[5])) if row[5] else 0,
                            'amount': float(row[6]) if row[6] else None,
                            'pct_chg': float(row[7]) if row[7] else None,
                            'turn': float(row[8]) if row[8] else None,
                            'trade_status': row[9] if len(row) > 9 else None,
                            'is_st': row[10] if len(row) > 10 else None,
                            'update_time': datetime.now()
                        })

                if records:
                    df = pd.DataFrame(records)
                    self.db.insert_dataframe(df, 'daily_kline', if_exists='append', unique_keys=['sec_code', 'trade_date'])
                    count = len(records)
                    total_count += count
                    success_count += 1
                    
                    # 记录同步日志
                    max_id_result = self.db.client.query("SELECT max(id) FROM sync_log")
                    max_id = max_id_result.result_rows[0][0] if max_id_result.result_rows[0][0] else 0
                    self.db.client.insert('sync_log', [[
                        max_id + 1, code, 'daily_kline', 1, count, '', datetime.now()
                    ]], column_names=['id', 'sec_code', 'data_type', 'success', 'record_count', 'error_message', 'update_time'])
                    
                    logger.debug(f"{code}: {count} 条")

            except Exception as e:
                logger.debug(f"{code} 异常: {e}")
                max_id_result = self.db.client.query("SELECT max(id) FROM sync_log")
                max_id = max_id_result.result_rows[0][0] if max_id_result.result_rows[0][0] else 0
                self.db.client.insert('sync_log', [[
                    max_id + 1, code, 'daily_kline', 0, 0, str(e), datetime.now()
                ]], column_names=['id', 'sec_code', 'data_type', 'success', 'record_count', 'error_message', 'update_time'])

        logger.info(f"日线同步完成: {success_count}/{len(codes)} 只, {total_count} 条")
        return {'success': True, 'record_count': total_count, 'success_count': success_count}

    def sync_all(self, force: bool = False) -> dict:
        return {
            'stock_list': self.sync_stock_list(),
            'daily_kline': self.sync_daily_kline(force=force)
        }

    def get_daily_kline(self, sec_code: str, start_date: str = None, end_date: str = None, adjust: str = "qfq") -> pd.DataFrame:
        query = f"SELECT * FROM daily_kline WHERE sec_code = '{sec_code}'"
        if start_date:
            query += f" AND trade_date >= '{start_date}'"
        if end_date:
            query += f" AND trade_date <= '{end_date}'"
        query += " ORDER BY trade_date"
        return self.db.query(query)

    def get_kline(self, sec_code: str, start_date: str = None, end_date: str = None, **kwargs) -> pd.DataFrame:
        return self.get_daily_kline(sec_code, start_date, end_date)

    def get_sync_status(self) -> dict:
        try:
            stock_result = self.db.client.query("SELECT COUNT(*) FROM stock_list")
            kline_result = self.db.client.query("SELECT COUNT(*) FROM daily_kline")
            stock_count = stock_result.result_rows[0][0] if stock_result.result_rows else 0
            kline_count = kline_result.result_rows[0][0] if kline_result.result_rows else 0
            return {'stock_count': stock_count, 'kline_count': kline_count}
        except Exception as e:
            return {'error': str(e)}

    def get_realtime_quote(self, sec_codes: List[str]) -> pd.DataFrame:
        logger.warning("Baostock 不支持实时行情")
        return pd.DataFrame()


_client = None

def get_client() -> BaostockClient:
    global _client
    if _client is None:
        _client = BaostockClient()
        _client.connect()
    return _client

def close_client():
    global _client
    if _client:
        _client.disconnect()
        _client = None
