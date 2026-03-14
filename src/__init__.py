"""
AmazingData 数据管理系统
"""

__version__ = "1.0.0"
__author__ = "AmazingData"

from src.config import config
from src.client import get_client, AmazingDataClient
from src.database import get_db, DuckDBManager
from src.scheduler import get_scheduler, start_scheduler, stop_scheduler
from src.logger import logger
