"""
ClickHouse 数据库模块
"""
import os
import json
from datetime import datetime, date, timedelta
from pathlib import Path
from typing import Optional, List, Dict, Any, Union
import clickhouse_connect
import pandas as pd
from pandas.api.types import (
    is_bool_dtype,
    is_datetime64_any_dtype,
    is_float_dtype,
    is_integer_dtype,
    is_object_dtype,
    is_string_dtype,
)
from src.common.config import config
from src.common.logger import logger
from src.common.models import DataSource


class ClickHouseManager:
    """ClickHouse 数据库管理器"""
    
    def __init__(self, host: Optional[str] = None, port: Optional[int] = None,
                 database: Optional[str] = None, username: Optional[str] = None,
                 password: Optional[str] = None):
        self.host = host or config.database.host
        self.port = port or config.database.port
        self.database = database  # 如果为 None，后续根据数据源选择
        self.username = username or config.database.username
        self.password = password or config.database.password
        
        # 如果指定了数据库，立即连接
        if self.database:
            self._connect()
        else:
            self.client = None
        
        logger.info(f"ClickHouse 管理器已初始化: {self.host}:{self.port}")
    
    def _connect(self):
        """连接到数据库"""
        self.client = clickhouse_connect.get_client(
            host=self.host,
            port=self.port,
            username=self.username,
            password=self.password,
            database=self.database
        )
        logger.info(f"已连接到 ClickHouse: {self.host}:{self.port}/{self.database}")
        
        # 初始化表结构
        self._init_tables()
    
    def use_database(self, database: str):
        """切换数据库"""
        if self.database != database:
            self.database = database
            if self.client:
                self.client.close()
            self._connect()
    
    def ensure_database(self, database: str):
        """确保数据库存在"""
        # 使用 system 数据库连接
        temp_client = clickhouse_connect.get_client(
            host=self.host,
            port=self.port,
            username=self.username,
            password=self.password
        )
        
        try:
            temp_client.command(f"CREATE DATABASE IF NOT EXISTS {database}")
            logger.info(f"数据库 {database} 已就绪")
        finally:
            temp_client.close()
    
    def _init_tables(self):
        """初始化表结构"""
        # 数据获取记录表
        self.client.command("""
            CREATE TABLE IF NOT EXISTS fetch_records (
                id UInt64,
                data_type String,
                fetch_time DateTime DEFAULT now(),
                success UInt8 DEFAULT 0,
                record_count UInt32 DEFAULT 0,
                error_message Nullable(String),
                retry_count UInt32 DEFAULT 0,
                start_date Nullable(String),
                end_date Nullable(String)
            ) ENGINE = MergeTree()
            ORDER BY (data_type, fetch_time)
        """)
        
        # 同步状态表
        self.client.command("""
            CREATE TABLE IF NOT EXISTS sync_status (
                data_type String,
                last_sync_time Nullable(DateTime),
                last_success_time Nullable(DateTime),
                record_count UInt32 DEFAULT 0,
                status String DEFAULT 'pending',
                error_message Nullable(String),
                created_at DateTime DEFAULT now(),
                updated_at DateTime DEFAULT now()
            ) ENGINE = ReplacingMergeTree(updated_at)
            ORDER BY data_type
        """)

        # 表级同步状态表
        self.client.command("""
            CREATE TABLE IF NOT EXISTS table_sync_status (
                table_name String,
                last_sync_time Nullable(DateTime),
                last_success_time Nullable(DateTime),
                record_count UInt64 DEFAULT 0,
                latest_date Nullable(String),
                status String DEFAULT 'pending',
                error_message Nullable(String),
                updated_at DateTime DEFAULT now()
            ) ENGINE = ReplacingMergeTree(updated_at)
            ORDER BY table_name
        """)

        # 同步错误日志表
        self.client.command("""
            CREATE TABLE IF NOT EXISTS sync_error_logs (
                id UInt64,
                scope String,
                method_name Nullable(String),
                table_name Nullable(String),
                batch_codes Nullable(String),
                error_message String,
                traceback Nullable(String),
                created_at DateTime DEFAULT now()
            ) ENGINE = MergeTree()
            ORDER BY (scope, created_at, id)
        """)
        
        # 每日数据汇总表
        self.client.command("""
            CREATE TABLE IF NOT EXISTS daily_summary (
                date String,
                data_type String,
                success_count UInt32 DEFAULT 0,
                failed_count UInt32 DEFAULT 0,
                total_records UInt32 DEFAULT 0
            ) ENGINE = ReplacingMergeTree()
            ORDER BY (date, data_type)
        """)
        
        logger.info("数据库表结构初始化完成")
    
    def execute(self, query: str, parameters: Optional[dict] = None):
        """执行 SQL"""
        return self.client.command(query, parameters=parameters)
    
    def insert_dataframe(self, df: pd.DataFrame, table_name: str, 
                        if_exists: str = "append", unique_keys: Optional[List[str]] = None):
        """
        插入 DataFrame（支持增量去重）
        
        Args:
            df: pandas DataFrame
            table_name: 表名
            if_exists: 如果表存在的行为 ('fail', 'replace', 'append')
            unique_keys: 唯一键列表，用于去重
        """
        if df.empty:
            logger.warning(f"DataFrame 为空，跳过插入 {table_name}")
            return

        df = self._normalize_dataframe_for_clickhouse(df)
        
        if not self.table_exists(table_name):
            # 自动创建表
            self._create_table_from_df(df, table_name)
            logger.info(f"创建表 {table_name}")
        else:
            self._ensure_table_schema(table_name, df)
            df = self._coerce_dataframe_to_table_schema(df, table_name)
        
        if unique_keys and if_exists == "append":
            # 增量插入，去重
            existing_keys = self._get_existing_keys(table_name, unique_keys)
            
            # 过滤掉已存在的记录
            if existing_keys:
                mask = ~df[unique_keys].apply(tuple, axis=1).isin(existing_keys)
                df = df[mask]
            
            if df.empty:
                logger.info(f"{table_name} 无新数据需要插入")
                return
        
        # 插入数据
        self.client.insert_df(table_name, df)
        logger.info(f"插入 {len(df)} 条记录到 {table_name}")
        self.update_table_sync_status(
            table_name=table_name,
            success=True,
            status="success",
        )
    
    def _create_table_from_df(self, df: pd.DataFrame, table_name: str):
        """从 DataFrame 自动创建表"""
        columns = []
        for col, dtype in df.dtypes.items():
            ch_type = self._infer_clickhouse_type(dtype)
            columns.append(f"`{col}` {ch_type}")
        
        columns_str = ", ".join(columns)
        
        # 使用第一列作为排序键
        order_by = df.columns[0]
        
        create_sql = f"""
            CREATE TABLE IF NOT EXISTS {table_name} (
                {columns_str}
            ) ENGINE = MergeTree()
            ORDER BY `{order_by}`
        """
        
        self.client.command(create_sql)

    def _infer_clickhouse_type(self, dtype) -> str:
        if is_datetime64_any_dtype(dtype):
            return 'DateTime'
        if is_integer_dtype(dtype):
            return 'Int64'
        if is_float_dtype(dtype):
            return 'Float64'
        if is_bool_dtype(dtype):
            return 'UInt8'
        return 'String'

    def _get_table_columns(self, table_name: str) -> Dict[str, str]:
        result = self.client.query(f"""
            SELECT name, type
            FROM system.columns
            WHERE database = '{self.database}' AND table = '{table_name}'
        """)
        return {row[0]: row[1] for row in result.result_rows}

    def _ensure_table_schema(self, table_name: str, df: pd.DataFrame):
        """确保表包含 DataFrame 中的所有列。"""
        existing_columns = self._get_table_columns(table_name)
        for col, dtype in df.dtypes.items():
            if col in existing_columns:
                continue
            ch_type = self._infer_clickhouse_type(dtype)
            self.client.command(
                f"ALTER TABLE {table_name} ADD COLUMN IF NOT EXISTS `{col}` {ch_type}"
            )
            logger.info(f"为 {table_name} 自动补充列 `{col}` {ch_type}")

    def _normalize_dataframe_for_clickhouse(self, df: pd.DataFrame) -> pd.DataFrame:
        """将 pandas DataFrame 归一化到 ClickHouse 更稳定的类型。"""
        normalized = df.copy()

        for col in normalized.columns:
            series = normalized[col]
            if is_datetime64_any_dtype(series.dtype):
                parsed = pd.to_datetime(series, errors="coerce")
                if getattr(parsed.dt, "tz", None) is not None:
                    parsed = parsed.dt.tz_localize(None)
                normalized[col] = parsed.astype("datetime64[ns]")
            elif is_object_dtype(series.dtype) or is_string_dtype(series.dtype):
                normalized[col] = series.map(
                    lambda value: None
                    if pd.isna(value)
                    else str(value)
                )

        return normalized

    def _coerce_dataframe_to_table_schema(self, df: pd.DataFrame, table_name: str) -> pd.DataFrame:
        """按目标表 schema 强制对齐 DataFrame 列类型。"""
        normalized = df.copy()
        table_columns = self._get_table_columns(table_name)

        for col in normalized.columns:
            ch_type = table_columns.get(col, "")
            if not ch_type:
                continue

            series = normalized[col]
            upper_type = ch_type.upper()

            if "STRING" in upper_type:
                normalized[col] = series.map(
                    lambda value: None if pd.isna(value) else str(value)
                )
            elif "DATETIME" in upper_type:
                parsed = pd.to_datetime(series, errors="coerce")
                if getattr(parsed.dt, "tz", None) is not None:
                    parsed = parsed.dt.tz_localize(None)
                normalized[col] = parsed.astype("datetime64[ns]")
            elif any(token in upper_type for token in ("INT", "UINT")):
                normalized[col] = pd.to_numeric(series, errors="coerce")
            elif "FLOAT" in upper_type or "DECIMAL" in upper_type:
                normalized[col] = pd.to_numeric(series, errors="coerce")

        return normalized
    
    def _get_existing_keys(self, table_name: str, key_columns: List[str]) -> set:
        """获取表中已存在的键"""
        keys_str = ", ".join([f"`{col}`" for col in key_columns])
        query = f"SELECT DISTINCT {keys_str} FROM {table_name}"
        
        result = self.client.query(query)
        return set(tuple(row) for row in result.result_rows)
    
    def insert_records(self, table_name: str, records: List[Dict[str, Any]]):
        """插入多条记录"""
        if not records:
            return
        
        df = pd.DataFrame(records)
        self.insert_dataframe(df, table_name)
    
    def query(self, sql: str, parameters: Optional[dict] = None) -> pd.DataFrame:
        """查询数据返回 DataFrame"""
        result = self.client.query_df(sql, parameters=parameters)
        return result
    
    def query_json(self, sql: str, parameters: Optional[dict] = None) -> List[Dict]:
        """查询数据返回 JSON"""
        df = self.query(sql, parameters)
        return df.to_dict(orient='records')
    
    def get_latest_date(self, table_name: str, date_column: str = "trade_date") -> Optional[str]:
        """获取表中最新的日期"""
        try:
            result = self.client.query(f"""
                SELECT MAX(`{date_column}`) as max_date 
                FROM {table_name}
            """)
            
            if result.result_rows and result.result_rows[0][0]:
                return str(result.result_rows[0][0])
            return None
        except Exception as e:
            logger.warning(f"获取最新日期失败: {e}")
            return None
    
    def get_table_count(self, table_name: str) -> int:
        """获取表记录数"""
        try:
            result = self.client.query(f"SELECT COUNT(*) FROM {table_name}")
            return result.result_rows[0][0] if result.result_rows else 0
        except:
            return 0
    
    def table_exists(self, table_name: str) -> bool:
        """检查表是否存在"""
        result = self.client.query(f"""
            SELECT count() FROM system.tables 
            WHERE database = '{self.database}' AND name = '{table_name}'
        """)
        return result.result_rows[0][0] > 0 if result.result_rows else False
    
    def get_tables(self) -> List[str]:
        """获取所有表"""
        result = self.client.query(f"""
            SELECT name FROM system.tables 
            WHERE database = '{self.database}'
        """)
        return [row[0] for row in result.result_rows]
    
    def save_fetch_record(self, data_type: str, success: bool, 
                          record_count: int = 0, 
                          error_message: Optional[str] = None,
                          start_date: Optional[str] = None,
                          end_date: Optional[str] = None):
        """保存数据获取记录"""
        # 生成 ID
        max_id_result = self.client.query("SELECT max(id) FROM fetch_records")
        max_id = max_id_result.result_rows[0][0] if max_id_result.result_rows[0][0] else 0
        new_id = max_id + 1
        
        self.client.insert('fetch_records', [[
            new_id, data_type, datetime.now(), 1 if success else 0,
            record_count, error_message, 0, start_date, end_date
        ]], column_names=['id', 'data_type', 'fetch_time', 'success', 
                         'record_count', 'error_message', 'retry_count', 
                         'start_date', 'end_date'])
        
        # 更新同步状态
        self.update_sync_status(data_type, success, record_count, error_message)

    def save_sync_error(
        self,
        scope: str,
        error_message: str,
        method_name: Optional[str] = None,
        table_name: Optional[str] = None,
        batch_codes: Optional[List[str]] = None,
        traceback_text: Optional[str] = None,
    ):
        """保存同步错误日志。"""
        max_id_result = self.client.query("SELECT max(id) FROM sync_error_logs")
        max_id = max_id_result.result_rows[0][0] if max_id_result.result_rows[0][0] else 0
        new_id = max_id + 1
        self.client.insert('sync_error_logs', [[
            new_id,
            scope,
            method_name,
            table_name,
            json.dumps(batch_codes, ensure_ascii=False) if batch_codes else None,
            error_message,
            traceback_text,
            datetime.now(),
        ]], column_names=[
            'id',
            'scope',
            'method_name',
            'table_name',
            'batch_codes',
            'error_message',
            'traceback',
            'created_at',
        ])
    
    def update_sync_status(self, data_type: str, success: bool,
                           record_count: int = 0,
                           error_message: Optional[str] = None):
        """更新同步状态"""
        now = datetime.now()
        
        if success:
            self.client.insert('sync_status', [[
                data_type, now, now, record_count, 'success', None, now, now
            ]], column_names=['data_type', 'last_sync_time', 'last_success_time',
                             'record_count', 'status', 'error_message', 
                             'created_at', 'updated_at'])
        else:
            self.client.insert('sync_status', [[
                data_type, now, None, record_count, 'failed', error_message, now, now
            ]], column_names=['data_type', 'last_sync_time', 'last_success_time',
                             'record_count', 'status', 'error_message', 
                             'created_at', 'updated_at'])
    
    def get_sync_status(self, data_type: Optional[str] = None) -> Union[List[Dict], Dict]:
        """获取同步状态"""
        if data_type:
            result = self.client.query(f"""
                SELECT * FROM sync_status 
                WHERE data_type = '{data_type}'
                ORDER BY updated_at DESC
                LIMIT 1
            """)
            if result.result_rows:
                row = result.result_rows[0]
                return {
                    "data_type": row[0],
                    "last_sync_time": row[1],
                    "last_success_time": row[2],
                    "record_count": row[3],
                    "status": row[4],
                    "error_message": row[5]
                }
            return {}
        else:
            df = self.client.query_df("""
                SELECT * FROM sync_status 
                ORDER BY updated_at DESC
            """)
            return df.to_dict(orient='records')
    
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
            if self.table_exists(table_name):
                self.update_table_sync_status(
                    table_name=table_name,
                    success=True,
                    date_column=date_column,
                    status="noop",
                )
            return
        
        # 获取最新日期
        if date_column and date_column in df.columns:
            latest_date = self.get_latest_date(table_name, date_column)
            
            if latest_date:
                # 过滤掉已存在的数据
                df = df[df[date_column] > latest_date]
                
                if df.empty:
                    logger.info(f"{table_name} 无新数据需要更新")
                    self.update_table_sync_status(
                        table_name=table_name,
                        success=True,
                        date_column=date_column,
                        status="noop",
                    )
                    return
        
        # 如果表不存在，创建表
        if not self.table_exists(table_name):
            self._create_table_from_df(df, table_name)
            logger.info(f"创建新表 {table_name}")
        
        # 使用去重插入
        self.insert_dataframe(df, table_name, if_exists="append", unique_keys=key_columns)
        logger.info(f"增量更新完成，插入 {len(df)} 条新记录到 {table_name}")
        self.update_table_sync_status(
            table_name=table_name,
            success=True,
            date_column=date_column,
            status="success",
        )

    def update_table_sync_status(
        self,
        table_name: str,
        success: bool,
        date_column: Optional[str] = None,
        status: Optional[str] = None,
        error_message: Optional[str] = None,
    ):
        """更新表级同步状态。"""
        now = datetime.now()
        table_exists = self.table_exists(table_name)
        record_count = self.get_table_count(table_name) if table_exists else 0
        latest_date = self.get_latest_date(table_name, date_column) if table_exists and date_column else None
        final_status = status or ("success" if success else "failed")

        self.client.insert('table_sync_status', [[
            table_name,
            now,
            now if success else None,
            record_count,
            latest_date,
            final_status,
            error_message,
            now,
        ]], column_names=[
            'table_name',
            'last_sync_time',
            'last_success_time',
            'record_count',
            'latest_date',
            'status',
            'error_message',
            'updated_at',
        ])

    def get_table_sync_status(self, table_name: Optional[str] = None) -> Union[List[Dict], Dict]:
        """获取表级同步状态。"""
        if table_name:
            result = self.client.query(f"""
                SELECT * FROM table_sync_status
                WHERE table_name = '{table_name}'
                ORDER BY updated_at DESC
                LIMIT 1
            """)
            if result.result_rows:
                row = result.result_rows[0]
                return {
                    "table_name": row[0],
                    "last_sync_time": row[1],
                    "last_success_time": row[2],
                    "record_count": row[3],
                    "latest_date": row[4],
                    "status": row[5],
                    "error_message": row[6],
                }
            return {}

        df = self.client.query_df("""
            SELECT * FROM table_sync_status
            ORDER BY updated_at DESC
        """)
        return df.to_dict(orient='records')
    
    def backup(self, backup_path: Optional[str] = None):
        """备份数据库（导出为 CSV）"""
        if backup_path is None:
            backup_path = config.database.backup_path
        
        Path(backup_path).mkdir(parents=True, exist_ok=True)
        
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        backup_dir = Path(backup_path) / f"amazing_data_{timestamp}"
        backup_dir.mkdir(exist_ok=True)
        
        # 导出所有表
        tables = self.get_tables()
        for table in tables:
            try:
                df = self.query(f"SELECT * FROM {table}")
                csv_file = backup_dir / f"{table}.csv"
                df.to_csv(csv_file, index=False)
                logger.info(f"已备份表 {table} 到 {csv_file}")
            except Exception as e:
                logger.error(f"备份表 {table} 失败: {e}")
        
        logger.info(f"数据库已备份到 {backup_dir}")
        return str(backup_dir)
    
    def close(self):
        """关闭数据库连接"""
        self.client.close()
    
    def __enter__(self):
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()


# 全局数据库实例（按数据源分类）
_db_instances: Dict[str, ClickHouseManager] = {}


def get_db(source: str = "starlight") -> ClickHouseManager:
    """
    获取数据库实例（单例，按数据源）
    
    Args:
        source: 数据源名称 ('baostock', 'starlight', 'miniqmt', 'akshare')
    """
    global _db_instances
    
    if source not in _db_instances:
        # 根据数据源选择数据库
        db_mapping = {
            "baostock": config.database.db_baostock,
            "starlight": config.database.db_starlight,
            "miniqmt": config.database.db_miniqmt,
            "akshare": config.database.db_akshare,
        }
        
        database = db_mapping.get(source)
        if not database:
            raise ValueError(f"未知的数据源: {source}")
        
        # 创建数据库管理器
        db = ClickHouseManager(database=None)
        db.ensure_database(database)
        db.use_database(database)
        
        _db_instances[source] = db
    
    return _db_instances[source]


def close_db(source: Optional[str] = None):
    """
    关闭数据库
    
    Args:
        source: 数据源名称，如果为 None 则关闭所有
    """
    global _db_instances
    
    if source:
        if source in _db_instances:
            _db_instances[source].close()
            del _db_instances[source]
    else:
        for db in _db_instances.values():
            db.close()
        _db_instances.clear()
