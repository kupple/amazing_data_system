"""
配置管理模块
"""
from dataclasses import dataclass
from typing import Optional
import os


@dataclass
class AmazingDataConfig:
    """AmazingData 连接配置"""
    account: str = "225300062129"
    password: str = "22530006212920260312"
    ip: str = "120.86.124.106"
    port: int = 8600
    permission_start: str = "2026-03-12"
    permission_end: str = "2026-04-11"
    phone: str = "13169959600"
    email: str = "rtys788@icloud.com"


@dataclass
class DatabaseConfig:
    """数据库配置"""
    db_path: str = "./data/amazing_data.duckdb"
    backup_path: str = "./data/backup"


@dataclass
class APIConfig:
    """API 服务配置"""
    host: str = "0.0.0.0"
    port: int = 8000
    reload: bool = False


@dataclass
class SchedulerConfig:
    """定时任务配置"""
    enabled: bool = True
    interval_hours: int = 1  # 每小时增量更新
    retry_times: int = 3
    retry_delay_seconds: int = 60


@dataclass
class LogConfig:
    """日志配置"""
    log_dir: str = "./logs"
    log_level: str = "INFO"
    max_log_files: int = 30


@dataclass
class MCPConfig:
    """MCP 服务配置"""
    enabled: bool = True
    port: int = 8001


@dataclass
class QMTConfig:
    """QMT 连接配置"""
    qmt_path: str = "C:/zhiyue/zqxtspeed/xmXtp"  # QMT 安装路径
    account_id: str = ""  # 资金账号
    db_path: str = "./data/qmt_data.duckdb"  # QMT 专用数据库


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
                account=os.getenv("AD_ACCOUNT", "225300062129"),
                password=os.getenv("AD_PASSWORD", "22530006212920260312"),
                ip=os.getenv("AD_IP", "120.86.124.106"),
                port=int(os.getenv("AD_PORT", "8600")),
            ),
            database=DatabaseConfig(
                db_path=os.getenv("DB_PATH", "./data/amazing_data.duckdb"),
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
            ),
            mcp=MCPConfig(
                enabled=os.getenv("MCP_ENABLED", "true").lower() == "true",
            ),
            qmt=QMTConfig(
                qmt_path=os.getenv("QMT_PATH", "C:/zhiyue/zqxtspeed/xmXtp"),
                account_id=os.getenv("QMT_ACCOUNT_ID", ""),
                db_path=os.getenv("QMT_DB_PATH", "./data/qmt_data.duckdb"),
            ),
        )


# 全局配置实例
config = AppConfig.from_env()
