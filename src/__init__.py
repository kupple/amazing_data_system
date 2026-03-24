"""
AmazingData 数据管理系统
"""

__version__ = "1.0.0"
__author__ = "AmazingData"

from src.common.config import config
from src.collectors.starlight.client import get_client, AmazingDataClient
from src.common.database import get_db, ClickHouseManager
from src.collectors.starlight.scheduler import get_scheduler, start_scheduler, stop_scheduler
from src.common.logger import logger
