"""
DuckDB 数据库模块
"""
import os
import json
from datetime import datetime, date, timedelta
from pathlib import Path
from typing import Optional, List, Dict, Any, Union
import duckdb
import pandas as pd
from src.config import config
from src.logger import logger
from src.models import DataSource


class DuckDBManager:
    """DuckDB 数据库管理器"""
    
    def __init__(self, db_path: Optional[str] = None):
        self.db_path = db_path or config.database.db_path
        
        # 确保目录存在
        Path(self.db_path).parent.mkdir(parents=True, exist_ok=True)
        
        # 连接数据库
        self.conn = duckdb.connect(self.db_path)
        self.conn.execute("PRAGMA threads=4")
        
        # 初始化表结构
        self._init_tables()
    
    def _init_tables(self):
        """初始化表结构"""
        # 数据获取记录表
        self.conn.execute("""
            CREATE SEQUENCE IF NOT EXISTS fetch_records_id_seq
        """)
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS fetch_records (
                id INTEGER PRIMARY KEY DEFAULT NEXTVAL('fetch_records_id_seq'),
                data_type VARCHAR NOT NULL,
                fetch_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                success BOOLEAN DEFAULT FALSE,
                record_count INTEGER DEFAULT 0,
                error_message VARCHAR,
                retry_count INTEGER DEFAULT 0,
                start_date VARCHAR,
                end_date VARCHAR
            )
        """)
        
        # 同步状态表
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS sync_status (
                data_type VARCHAR PRIMARY KEY,
                last_sync_time TIMESTAMP,
                last_success_time TIMESTAMP,
                record_count INTEGER DEFAULT 0,
                status VARCHAR DEFAULT 'pending',
                error_message VARCHAR,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        
        # 每日数据汇总表
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS daily_summary (
                date VARCHAR NOT NULL,
                data_type VARCHAR NOT NULL,
                success_count INTEGER DEFAULT 0,
                failed_count INTEGER DEFAULT 0,
                total_records INTEGER DEFAULT 0,
                PRIMARY KEY (date, data_type)
            )
        """)
        
        # 创建索引
        self.conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_fetch_records_type 
            ON fetch_records(data_type)
        """)
        self.conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_fetch_records_time 
            ON fetch_records(fetch_time)
        """)
        
        logger.info("数据库表结构初始化完成")
    
    def execute(self, query: str, params: Optional[dict] = None) -> duckdb.DuckDBPyConnection:
        """执行 SQL"""
        if params:
            return self.conn.execute(query, params)
        return self.conn.execute(query)
    
    def insert_dataframe(self, df: pd.DataFrame, table_name: str, 
                        if_exists: str = "append"):
        """
        插入 DataFrame
        
        Args:
            df: pandas DataFrame
            table_name: 表名
            if_exists: 如果表存在的行为 ('fail', 'replace', 'append')
        """
        if df.empty:
            logger.warning(f"DataFrame 为空，跳过插入 {table_name}")
            return
        
        # 注册 DataFrame 为临时视图，然后从视图创建表
        temp_view_name = f"temp_{table_name}_{id(df)}"
        
        # 使用 duckdb 的 read_pandas 来创建视图
        self.conn.execute(f"CREATE OR REPLACE VIEW {temp_view_name} AS SELECT * FROM df")
        
        # 如果表不存在，从视图创建
        if not self.table_exists(table_name):
            self.conn.execute(f"CREATE TABLE {table_name} AS SELECT * FROM {temp_view_name}")
        else:
            # 表存在，插入数据
            self.conn.execute(f"INSERT INTO {table_name} SELECT * FROM {temp_view_name}")
        
        # 清理视图
        self.conn.execute(f"DROP VIEW {temp_view_name}")
        
        logger.info(f"成功插入 {len(df)} 条记录到 {table_name}")
    
    def insert_records(self, table_name: str, records: List[Dict[str, Any]]):
        """插入多条记录"""
        if not records:
            return
        
        df = pd.DataFrame(records)
        self.insert_dataframe(df, table_name)
    
    def query(self, sql: str, params: Optional[dict] = None) -> pd.DataFrame:
        """查询数据返回 DataFrame"""
        if params:
            result = self.conn.execute(sql, params).df()
        else:
            result = self.conn.execute(sql).df()
        return result
    
    def query_json(self, sql: str, params: Optional[dict] = None) -> List[Dict]:
        """查询数据返回 JSON"""
        df = self.query(sql, params)
        return df.to_dict(orient='records')
    
    def get_latest_date(self, table_name: str, date_column: str = "trade_date") -> Optional[str]:
        """获取表中最新的日期"""
        try:
            result = self.conn.execute(f"""
                SELECT MAX({date_column}) as max_date 
                FROM {table_name}
            """).fetchone()
            
            return result[0] if result and result[0] else None
        except Exception as e:
            logger.warning(f"获取最新日期失败: {e}")
            return None
    
    def get_table_count(self, table_name: str) -> int:
        """获取表记录数"""
        try:
            result = self.conn.execute(f"""
                SELECT COUNT(*) FROM {table_name}
            """).fetchone()
            return result[0] if result else 0
        except:
            return 0
    
    def table_exists(self, table_name: str) -> bool:
        """检查表是否存在"""
        result = self.conn.execute(f"""
            SELECT COUNT(*) FROM information_schema.tables 
            WHERE table_name = '{table_name}'
        """).fetchone()
        return result[0] > 0 if result else False
    
    def get_tables(self) -> List[str]:
        """获取所有表"""
        result = self.conn.execute("""
            SELECT table_name FROM information_schema.tables 
            WHERE table_schema = 'main'
        """).fetchall()
        return [r[0] for r in result]
    
    def save_fetch_record(self, data_type: str, success: bool, 
                          record_count: int = 0, 
                          error_message: Optional[str] = None,
                          start_date: Optional[str] = None,
                          end_date: Optional[str] = None):
        """保存数据获取记录"""
        self.conn.execute("""
            INSERT INTO fetch_records (data_type, success, record_count, 
                                        error_message, start_date, end_date)
            VALUES (?, ?, ?, ?, ?, ?)
        """, [data_type, success, record_count, error_message, start_date, end_date])
        
        # 更新同步状态
        self.update_sync_status(data_type, success, record_count, error_message)
    
    def update_sync_status(self, data_type: str, success: bool,
                           record_count: int = 0,
                           error_message: Optional[str] = None):
        """更新同步状态"""
        now = datetime.now()
        
        if success:
            self.conn.execute(f"""
                INSERT INTO sync_status (data_type, last_sync_time, last_success_time, 
                                        record_count, status, updated_at)
                VALUES (?, ?, ?, ?, 'success', ?)
                ON CONFLICT(data_type) DO UPDATE SET
                    last_sync_time = excluded.last_sync_time,
                    last_success_time = excluded.last_success_time,
                    record_count = excluded.record_count,
                    status = excluded.status,
                    updated_at = excluded.updated_at
            """, [data_type, now, now, record_count, now])
        else:
            self.conn.execute(f"""
                INSERT INTO sync_status (data_type, last_sync_time, 
                                        status, error_message, updated_at)
                VALUES (?, ?, 'failed', ?, ?)
                ON CONFLICT(data_type) DO UPDATE SET
                    last_sync_time = excluded.last_sync_time,
                    status = excluded.status,
                    error_message = excluded.error_message,
                    updated_at = excluded.updated_at
            """, [data_type, now, error_message, now])
    
    def get_sync_status(self, data_type: Optional[str] = None) -> Union[List[Dict], Dict]:
        """获取同步状态"""
        if data_type:
            result = self.conn.execute(f"""
                SELECT * FROM sync_status WHERE data_type = '{data_type}'
            """).fetchone()
            if result:
                return {
                    "data_type": result[0],
                    "last_sync_time": result[1],
                    "last_success_time": result[2],
                    "record_count": result[3],
                    "status": result[4],
                    "error_message": result[5]
                }
            return {}
        else:
            return self.conn.execute("SELECT * FROM sync_status").df().to_dict(orient='records')
    
    def incremental_update(self, table_name: str, df: pd.DataFrame,
                           key_columns: List[str],
                           date_column: Optional[str] = None):
        """
        增量更新数据
        
        Args:
            table_name: 表名
            df: 新数据 DataFrame
            key_columns: 主键列（用于判断重复）
            date_column: 日期列（用于增量判断）
        """
        if df.empty:
            return
        
        # 获取最新日期
        if date_column and date_column in df.columns:
            latest_date = self.get_latest_date(table_name, date_column)
            
            if latest_date:
                # 过滤掉已存在的数据
                df = df[df[date_column] > latest_date]
                
                if df.empty:
                    logger.info(f"{table_name} 无新数据需要更新")
                    return
        
        # 如果表不存在，创建表
        if not self.table_exists(table_name):
            # 从 DataFrame 创建表
            temp_view = f"temp_{table_name}"
            self.conn.execute(f"CREATE OR REPLACE TEMPORARY VIEW {temp_view} AS SELECT * FROM df")
            self.conn.execute(f"CREATE TABLE {table_name} AS SELECT * FROM {temp_view}")
            logger.info(f"创建新表 {table_name}")
            return
        
        # 增量插入（排除重复）
        for _, row in df.iterrows():
            conditions = " AND ".join([f"{col} = '{row[col]}'" for col in key_columns])
            exists = self.conn.execute(f"""
                SELECT COUNT(*) FROM {table_name} WHERE {conditions}
            """).fetchone()[0]
            
            if exists == 0:
                # 构建插入语句
                columns = ", ".join(df.columns)
                placeholders = ", ".join(["?" for _ in df.columns])
                values = [row[col] for col in df.columns]
                
                self.conn.execute(f"""
                    INSERT INTO {table_name} ({columns}) VALUES ({placeholders})
                """, values)
        
        logger.info(f"增量更新完成，插入 {len(df)} 条新记录到 {table_name}")
    
    def backup(self, backup_path: Optional[str] = None):
        """备份数据库"""
        if backup_path is None:
            backup_path = config.database.backup_path
        
        Path(backup_path).mkdir(parents=True, exist_ok=True)
        
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        backup_file = Path(backup_path) / f"amazing_data_{timestamp}.duckdb"
        
        # 复制数据库文件
        import shutil
        shutil.copy2(self.db_path, backup_file)
        
        logger.info(f"数据库已备份到 {backup_file}")
        return str(backup_file)
    
    def close(self):
        """关闭数据库连接"""
        self.conn.close()
    
    def __enter__(self):
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()


# 全局数据库实例
_db_instance: Optional[DuckDBManager] = None


def get_db() -> DuckDBManager:
    """获取数据库实例（单例）"""
    global _db_instance
    if _db_instance is None:
        _db_instance = DuckDBManager()
    return _db_instance


def close_db():
    """关闭数据库"""
    global _db_instance
    if _db_instance:
        _db_instance.close()
        _db_instance = None
