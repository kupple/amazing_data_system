"""
Akshare 数据采集模块
"""
from src.collectors.akshare.client import AkshareClient, get_client, close_client

__all__ = ['AkshareClient', 'get_client', 'close_client']
