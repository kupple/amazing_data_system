"""Starlight 数据采集器"""
from .client import AmazingDataClient, get_client, close_client
from .scheduler import get_scheduler, start_scheduler, stop_scheduler

__all__ = [
    'AmazingDataClient',
    'get_client',
    'close_client',
    'get_scheduler',
    'start_scheduler',
    'stop_scheduler',
]
