"""
Baostock 数据采集模块
"""
from src.collectors.baostock.client import BaostockClient, get_client, close_client

__all__ = ['BaostockClient', 'get_client', 'close_client']
