"""数据采集器模块"""
from abc import ABC, abstractmethod
from typing import Optional, Dict, Any
import pandas as pd


class BaseCollector(ABC):
    """数据采集器基类"""
    
    def __init__(self, name: str):
        self.name = name
        self._connected = False
    
    @abstractmethod
    def connect(self) -> bool:
        """连接数据源"""
        pass
    
    @abstractmethod
    def disconnect(self) -> None:
        """断开连接"""
        pass
    
    @property
    def is_connected(self) -> bool:
        return self._connected
    
    @abstractmethod
    def get_stock_list(self, **kwargs) -> pd.DataFrame:
        """获取股票列表"""
        pass
    
    @abstractmethod
    def get_kline(self, sec_code: str, start_date: str, end_date: str, **kwargs) -> pd.DataFrame:
        """获取K线数据"""
        pass


# 注册数据源
from src.collectors.akshare import AkshareClient
from src.collectors.starlight import AmazingDataClient
from src.collectors.miniqmt import QMTClient as MiniQMTClient

__all__ = [
    'BaseCollector',
    'AkshareClient',
    'AmazingDataClient', 
    'MiniQMTClient',
]
