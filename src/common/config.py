"""
配置管理模块
"""
from dataclasses import dataclass
from typing import Optional
import os
from dotenv import load_dotenv

# 加载环境变量
load_dotenv()


@dataclass
class AmazingDataConfig:
    """AmazingData 连接配置"""
    account: str
    password: str
    ip: str
    port: int
    permission_start: str
    permission_end: str
    phone: str
    email: str


@dataclass
class DatabaseConfig:
    """数据库配置"""
    host: str
    port: int
    username: str
    password: str
    backup_path: str
    # 不同数据源使用不同的数据库
    db_baostock: str
    db_starlight: str
    db_miniqmt: str
    db_akshare: str


@dataclass
class APIConfig:
    """API 服务配置"""
    host: str
    port: int
    reload: bool = False


@dataclass
class SchedulerConfig:
    """定时任务配置"""
    enabled: bool
    interval_hours: int
    retry_times: int = 3
    retry_delay_seconds: int = 60


@dataclass
class LogConfig:
    """日志配置"""
    log_dir: str
    log_level: str
    max_log_files: int = 30


@dataclass
class MCPConfig:
    """MCP 服务配置"""
    enabled: bool
    port: int


@dataclass
class QMTConfig:
    """QMT 连接配置"""
    qmt_path: str
    account_id: str
    db_path: str


@dataclass
class AppConfig:
    """应用全局配置"""
    amazing_data: AmazingDataConfig
    database: DatabaseConfig
    api: APIConfig
    scheduler: SchedulerConfig
    log: LogConfig
    mcp: MCPConfig
    qmt: QMTConfig
    
    @classmethod
    def from_env(cls):
        """从环境变量加载配置"""
        return cls(
            amazing_data=AmazingDataConfig(
                account=os.getenv("AD_ACCOUNT"),
                password=os.getenv("AD_PASSWORD"),
                ip=os.getenv("AD_IP"),
                port=int(os.getenv("AD_PORT", "8600")),
                permission_start=os.getenv("AD_PERMISSION_START"),
                permission_end=os.getenv("AD_PERMISSION_END"),
                phone=os.getenv("AD_PHONE"),
                email=os.getenv("AD_EMAIL"),
            ),
            database=DatabaseConfig(
                host=os.getenv("DB_HOST", "localhost"),
                port=int(os.getenv("DB_PORT", "8123")),
                username=os.getenv("DB_USER", "default"),
                password=os.getenv("DB_PASSWORD", ""),
                backup_path=os.getenv("DB_BACKUP_PATH", "./data/backup"),
                db_baostock=os.getenv("DB_BAOSTOCK", "baostock_data"),
                db_starlight=os.getenv("DB_STARLIGHT", "starlight"),
                db_miniqmt=os.getenv("DB_MINIQMT", "miniqmt_data"),
                db_akshare=os.getenv("DB_AKSHARE", "akshare_data"),
            ),
            api=APIConfig(
                host=os.getenv("API_HOST", "0.0.0.0"),
                port=int(os.getenv("API_PORT", "8000")),
            ),
            scheduler=SchedulerConfig(
                enabled=os.getenv("SCHEDULER_ENABLED", "true").lower() == "true",
                interval_hours=int(os.getenv("SCHEDULER_INTERVAL", "1")),
            ),
            log=LogConfig(
                log_dir=os.getenv("LOG_DIR", "./logs"),
                log_level=os.getenv("LOG_LEVEL", "INFO"),
            ),
            mcp=MCPConfig(
                enabled=os.getenv("MCP_ENABLED", "true").lower() == "true",
                port=int(os.getenv("MCP_PORT", "8001")),
            ),
            qmt=QMTConfig(
                qmt_path=os.getenv("QMT_PATH", "C:/zhiyue/zqxtspeed/xmXtp"),
                account_id=os.getenv("QMT_ACCOUNT_ID", ""),
                db_path=os.getenv("QMT_DB_PATH", "./data/qmt_data.duckdb"),
            ),
        )


# 全局配置实例
config = AppConfig.from_env()
