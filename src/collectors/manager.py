"""数据源管理器"""
from typing import Dict, Optional, Type
from src.common.logger import logger
from . import BaseCollector


class CollectorManager:
    """数据采集器管理器"""
    
    def __init__(self):
        self._collectors: Dict[str, BaseCollector] = {}
        self._active_collector: Optional[str] = None
    
    def register(self, name: str, collector: BaseCollector):
        """注册数据采集器"""
        self._collectors[name] = collector
        logger.info(f"已注册数据源: {name}")
        
        if self._active_collector is None:
            self._active_collector = name
    
    def get(self, name: str) -> Optional[BaseCollector]:
        """获取指定数据采集器"""
        return self._collectors.get(name)
    
    def set_active(self, name: str):
        """设置活跃数据源"""
        if name in self._collectors:
            self._active_collector = name
            logger.info(f"切换数据源: {name}")
        else:
            logger.error(f"数据源不存在: {name}")
    
    def get_active(self) -> Optional[BaseCollector]:
        """获取当前活跃数据源"""
        if self._active_collector:
            return self._collectors.get(self._active_collector)
        return None
    
    def list_collectors(self) -> list:
        """列出所有数据源"""
        return list(self._collectors.keys())
    
    def connect_all(self):
        """连接所有数据源"""
        for name, collector in self._collectors.items():
            try:
                if collector.connect():
                    logger.info(f"数据源 {name} 连接成功")
            except Exception as e:
                logger.error(f"数据源 {name} 连接失败: {e}")
    
    def disconnect_all(self):
        """断开所有数据源"""
        for collector in self._collectors.values():
            try:
                collector.disconnect()
            except:
                pass


# 全局管理器实例
_manager = CollectorManager()


def get_manager() -> CollectorManager:
    """获取管理器实例"""
    return _manager
