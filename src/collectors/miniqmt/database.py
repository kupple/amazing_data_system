"""
QMT 数据库管理模块
单独使用 qmt_data.duckdb
"""
import os
from typing import Optional, List
from datetime import datetime
import pandas as pd
import duckdb

from src.common.logger import logger


class QMTDatabase:
    """QMT 数据数据库管理"""
    
    def __init__(self, db_path: str = "./data/qmt_data.duckdb"):
        """
        初始化 QMT 数据库
        
        Args:
            db_path: 数据库文件路径
        """
        self.db_path = db_path
        
        # 确保目录存在
        os.makedirs(os.path.dirname(db_path) if os.path.dirname(db_path) else "./data", exist_ok=True)
        
        # 连接数据库
        self.conn = duckdb.connect(db_path)
        
        # 初始化表结构
        self._init_tables()
        
        logger.info(f"QMT 数据库初始化完成: {db_path}")
    
    def _init_tables(self):
        """初始化表结构"""
        # 板块列表
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS sector_list (
                id INTEGER PRIMARY KEY,
                sector_name VARCHAR,
                create_time TIMESTAMP
            )
        """)
        
        # 板块股票列表
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS sector_stocks (
                id INTEGER PRIMARY KEY,
                sec_code VARCHAR,
                sector_name VARCHAR,
                list_time TIMESTAMP,
                UNIQUE(sec_code, sector_name)
            )
        """)
        
        # 股票列表
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS stock_list (
                id INTEGER PRIMARY KEY,
                sec_code VARCHAR UNIQUE,
                list_time TIMESTAMP
            )
        """)
        
        # K线数据
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS kline (
                id INTEGER PRIMARY KEY,
                sec_code VARCHAR,
                trade_time TIMESTAMP,
                open DOUBLE,
                high DOUBLE,
                low DOUBLE,
                close DOUBLE,
                volume BIGINT,
                amount DOUBLE,
                period VARCHAR,
                UNIQUE(sec_code, trade_time, period)
            )
        """)
        
        # 实时行情
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS realtime_quote (
                id INTEGER PRIMARY KEY,
                sec_code VARCHAR UNIQUE,
                last DOUBLE,
                open DOUBLE,
                high DOUBLE,
                low DOUBLE,
                close DOUBLE,
                volume BIGINT,
                amount DOUBLE,
                update_time TIMESTAMP
            )
        """)
        
        # 财务数据
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS financial_data (
                id INTEGER PRIMARY KEY,
                sec_code VARCHAR,
                report_date VARCHAR,
                data_type VARCHAR,
                create_time TIMESTAMP,
                data JSON,
                UNIQUE(sec_code, report_date, data_type)
            )
        """)
        
        # ETF列表
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS etf_list (
                id INTEGER PRIMARY KEY,
                sec_code VARCHAR UNIQUE,
                list_time TIMESTAMP
            )
        """)
        
        # 指数成分股权重
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS index_weight (
                id INTEGER PRIMARY KEY,
                index_code VARCHAR,
                sec_code VARCHAR,
                weight DOUBLE,
                update_time TIMESTAMP,
                UNIQUE(index_code, sec_code)
            )
        """)
        
        # 同步记录
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS sync_records (
                id INTEGER PRIMARY KEY,
                data_type VARCHAR,
                success BOOLEAN,
                record_count INTEGER,
                error_message VARCHAR,
                create_time TIMESTAMP
            )
        """)
        
        # 创建索引
        self.conn.execute("CREATE INDEX IF NOT EXISTS idx_kline_code ON kline(sec_code)")
        self.conn.execute("CREATE INDEX IF NOT EXISTS idx_kline_time ON kline(trade_time)")
        self.conn.execute("CREATE INDEX IF NOT EXISTS idx_quote_code ON realtime_quote(sec_code)")
        self.conn.execute("CREATE INDEX IF NOT EXISTS idx_financial_code ON financial_data(sec_code)")
        
        self.conn.commit()
    
    def save_sector_list(self, df: pd.DataFrame) -> int:
        """保存板块列表"""
        if df.empty:
            return 0
        
        df = df.copy()
        df['create_time'] = datetime.now()
        
        self.conn.execute("DELETE FROM sector_list")
        self.conn.execute("INSERT INTO sector_list (sector_name, create_time) VALUES (?, ?)",
                        [df['sector_name'].tolist(), df['create_time'].tolist()])
        self.conn.commit()
        
        count = len(df)
        logger.info(f"保存板块列表: {count} 条")
        return count
    
    def save_sector_stocks(self, df: pd.DataFrame) -> int:
        """保存板块股票"""
        if df.empty:
            return 0
        
        df = df.copy()
        df['list_time'] = datetime.now()
        
        # 使用 INSERT OR IGNORE 避免重复
        for _, row in df.iterrows():
            self.conn.execute("""
                INSERT OR IGNORE INTO sector_stocks (sec_code, sector_name, list_time)
                VALUES (?, ?, ?)
            """, [row['sec_code'], row['sector_name'], row['list_time']])
        
        self.conn.commit()
        
        count = len(df)
        logger.info(f"保存板块股票: {count} 条")
        return count
    
    def save_stock_list(self, df: pd.DataFrame) -> int:
        """保存股票列表"""
        if df.empty:
            return 0
        
        df = df.copy()
        df['list_time'] = datetime.now()
        
        for _, row in df.iterrows():
            self.conn.execute("""
                INSERT OR REPLACE INTO stock_list (sec_code, list_time)
                VALUES (?, ?)
            """, [row['sec_code'], row['list_time']])
        
        self.conn.commit()
        
        count = len(df)
        logger.info(f"保存股票列表: {count} 条")
        return count
    
    def save_kline(self, df: pd.DataFrame, period: str = "1d") -> int:
        """保存K线数据"""
        if df.empty:
            return 0
        
        df = df.copy()
        if 'time' in df.columns:
            df['trade_time'] = pd.to_datetime(df['time'], unit='s')
        df['period'] = period
        
        count = 0
        for _, row in df.iterrows():
            try:
                self.conn.execute("""
                    INSERT OR IGNORE INTO kline 
                    (sec_code, trade_time, open, high, low, close, volume, amount, period)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, [
                    row.get('sec_code', ''),
                    row.get('trade_time', datetime.now()),
                    row.get('open', 0),
                    row.get('high', 0),
                    row.get('low', 0),
                    row.get('close', 0),
                    row.get('volume', 0),
                    row.get('amount', 0),
                    period
                ])
                count += 1
            except Exception as e:
                pass
        
        self.conn.commit()
        logger.info(f"保存K线数据: {count} 条")
        return count
    
    def save_realtime_quote(self, df: pd.DataFrame) -> int:
        """保存实时行情"""
        if df.empty:
            return 0
        
        df = df.copy()
        df['update_time'] = datetime.now()
        
        count = 0
        for _, row in df.iterrows():
            try:
                self.conn.execute("""
                    INSERT OR REPLACE INTO realtime_quote
                    (sec_code, last, open, high, low, close, volume, amount, update_time)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, [
                    row.get('code', row.get('sec_code', '')),
                    row.get('last', 0),
                    row.get('open', 0),
                    row.get('high', 0),
                    row.get('low', 0),
                    row.get('close', 0),
                    row.get('volume', 0),
                    row.get('amount', 0),
                    row.get('update_time')
                ])
                count += 1
            except Exception as e:
                pass
        
        self.conn.commit()
        logger.info(f"保存实时行情: {count} 条")
        return count
    
    def save_etf_list(self, df: pd.DataFrame) -> int:
        """保存ETF列表"""
        if df.empty:
            return 0
        
        df = df.copy()
        df['list_time'] = datetime.now()
        
        for _, row in df.iterrows():
            self.conn.execute("""
                INSERT OR REPLACE INTO etf_list (sec_code, list_time)
                VALUES (?, ?)
            """, [row['sec_code'], row['list_time']])
        
        self.conn.commit()
        
        count = len(df)
        logger.info(f"保存ETF列表: {count} 条")
        return count
    
    def save_index_weight(self, df: pd.DataFrame, index_code: str) -> int:
        """保存指数成分股权重"""
        if df.empty:
            return 0
        
        df = df.copy()
        df['update_time'] = datetime.now()
        df['index_code'] = index_code
        
        for _, row in df.iterrows():
            self.conn.execute("""
                INSERT OR REPLACE INTO index_weight
                (index_code, sec_code, weight, update_time)
                VALUES (?, ?, ?, ?)
            """, [
                index_code,
                row.get('code', row.get('sec_code', '')),
                row.get('weight', 0),
                row.get('update_time')
            ])
        
        self.conn.commit()
        
        count = len(df)
        logger.info(f"保存指数成分股: {count} 条")
        return count
    
    def save_sync_record(self, data_type: str, success: bool, 
                        record_count: int, error_message: str = "") -> None:
        """保存同步记录"""
        self.conn.execute("""
            INSERT INTO sync_records (data_type, success, record_count, error_message, create_time)
            VALUES (?, ?, ?, ?, ?)
        """, [data_type, success, record_count, error_message, datetime.now()])
        self.conn.commit()
    
    def get_tables(self) -> List[str]:
        """获取所有表"""
        result = self.conn.execute("""
            SELECT table_name FROM information_schema.tables 
            WHERE table_schema = 'main'
        """).fetchall()
        return [r[0] for r in result]
    
    def get_table_count(self, table: str) -> int:
        """获取表记录数"""
        try:
            result = self.conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()
            return result[0] if result else 0
        except:
            return 0
    
    def query(self, sql: str) -> pd.DataFrame:
        """执行查询"""
        return self.conn.execute(sql).df()
    
    def close(self):
        """关闭连接"""
        if self.conn:
            self.conn.close()


# 全局数据库实例
_qmt_db_instance: Optional[QMTDatabase] = None


def get_qmt_db(db_path: str = "./data/qmt_data.duckdb") -> QMTDatabase:
    """获取 QMT 数据库实例（单例）"""
    global _qmt_db_instance
    if _qmt_db_instance is None:
        _qmt_db_instance = QMTDatabase(db_path)
    return _qmt_db_instance


def close_qmt_db():
    """关闭 QMT 数据库"""
    global _qmt_db_instance
    if _qmt_db_instance:
        _qmt_db_instance.close()
        _qmt_db_instance = None
