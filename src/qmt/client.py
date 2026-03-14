"""
QMT 数据采集客户端
使用迅投 XtQuant 获取数据
免费接口列表：
- get_stock_list_in_sector() - 获取行业/概念股票列表
- get_sector_list() - 获取行业/概念列表
- get_full_kline() - 获取K线数据
- get_full_tick() - 获取Tick数据
- get_market_data() - 获取行情数据
- get_financial_data() - 获取财务数据
- get_etf_info() - 获取ETF信息
- get_option_list() - 获取期权列表
- download_history_data() - 下载历史数据
- subscribe_quote() - 订阅实时行情
"""
import os
import sys
from typing import Optional, List, Dict, Any
from datetime import datetime, timedelta
import pandas as pd

from src.logger import logger
from src.retry import retry


# 检查 SDK 是否可用
def _check_qmt() -> bool:
    """检查 QMT SDK 是否可用"""
    try:
        import xtquant
        return True
    except ImportError:
        return False


class QMTClient:
    """QMT 数据客户端"""
    
    def __init__(self, 
                 qmt_path: Optional[str] = None,
                 account_id: Optional[str] = None):
        """
        初始化 QMT 客户端
        
        Args:
            qmt_path: QMT 安装路径 (例如: C:/zhiyue/zqxtspeed/xmXtp)
            account_id: 资金账号 (可以在 QMT 中查看)
        """
        self._client = None
        self._connected = False
        self.qmt_path = qmt_path
        self.account_id = account_id
        
        # 如果未指定，尝试自动发现 QMT 路径
        if not self.qmt_path:
            self.qmt_path = self._find_qmt_path()
    
    def _find_qmt_path(self) -> Optional[str]:
        """自动查找 QMT 安装路径"""
        possible_paths = [
            "C:/zhiyue/zqxtspeed/xmXtp",
            "C:/zhiyue/zqxt/xmXtp", 
            "C:/迅投/xmXtp",
            "D:/zhiyue/zqxtspeed/xmXtp",
        ]
        
        for path in possible_paths:
            if os.path.exists(path):
                logger.info(f"找到 QMT 路径: {path}")
                return path
        
        logger.warning("未找到 QMT 安装路径，请手动指定")
        return None
    
    def connect(self) -> bool:
        """连接 QMT"""
        if not _check_qmt():
            logger.error("QMT SDK (xtquant) 不可用，请安装: pip install xtquant")
            return False
        
        try:
            import xtquant
            
            if not self.qmt_path:
                logger.error("QMT 路径未设置")
                return False
            
            if not self.account_id:
                logger.error("资金账号未设置")
                return False
            
            # 创建交易通道
            self._client = xtquant.XtQuant(self.qmt_path, self.account_id)
            
            # 启动
            self._client.start()
            
            # 等待连接
            if self._client.connect_state:
                self._connected = True
                logger.info(f"成功连接 QMT (账号: {self.account_id})")
                return True
            else:
                logger.error("QMT 连接失败")
                return False
                
        except Exception as e:
            logger.error(f"连接 QMT 失败: {e}")
            self._connected = False
            return False
    
    def disconnect(self) -> None:
        """断开 QMT 连接"""
        if self._client and self._connected:
            self._client.stop()
            self._connected = False
            logger.info("QMT 已断开连接")
    
    @property
    def is_connected(self) -> bool:
        return self._connected
    
    # ========== 免费接口实现 ==========
    
    @retry(data_type="sector_list")
    def get_sector_list(self) -> pd.DataFrame:
        """
        获取行业/概念板块列表
        
        Returns:
            DataFrame: 板块列表
        """
        if not self._connected:
            self.connect()
        
        try:
            import xtquant.xtdata as xtd
            
            # 获取板块列表
            sectors = xtd.get_sector_list()
            
            if sectors:
                df = pd.DataFrame({
                    'sector_name': sectors,
                    'create_time': datetime.now()
                })
                return df
            return pd.DataFrame()
            
        except Exception as e:
            logger.error(f"获取板块列表失败: {e}")
            raise
    
    @retry(data_type="stock_list")
    def get_stock_list_in_sector(self, sector_name: str) -> pd.DataFrame:
        """
        获取指定板块内的股票列表
        
        Args:
            sector_name: 板块名称 (如: "银行", "新能源")
            
        Returns:
            DataFrame: 股票代码列表
        """
        if not self._connected:
            self.connect()
        
        try:
            import xtquant.xtdata as xtd
            
            # 获取板块内股票
            stocks = xtd.get_stock_list_in_sector(sector_name)
            
            if stocks:
                df = pd.DataFrame({
                    'sec_code': stocks,
                    'sector_name': sector_name,
                    'list_time': datetime.now()
                })
                return df
            return pd.DataFrame()
            
        except Exception as e:
            logger.error(f"获取板块股票列表失败: {e}")
            raise
    
    @retry(data_type="stock_basic")
    def get_stock_list(self) -> pd.DataFrame:
        """
        获取所有股票列表
        
        Returns:
            DataFrame: 股票列表
        """
        if not self._connected:
            self.connect()
        
        try:
            import xtquant.xtdata as xtd
            
            # 获取沪深A股列表
            stock_list = xtd.get_stock_list()
            
            if stock_list:
                df = pd.DataFrame({
                    'sec_code': stock_list,
                    'list_time': datetime.now()
                })
                return df
            return pd.DataFrame()
            
        except Exception as e:
            logger.error(f"获取股票列表失败: {e}")
            raise
    
    @retry(data_type="kline")
    def get_full_kline(self, 
                       sec_code: str, 
                       start_date: str, 
                       end_date: str,
                       period: str = "1d",
                       count: Optional[int] = None) -> pd.DataFrame:
        """
        获取K线数据
        
        Args:
            sec_code: 证券代码 (如: "600000.SH")
            start_date: 开始日期 (YYYYMMDD)
            end_date: 结束日期 (YYYYMMDD)
            period: K线周期 ("1d", "1w", "1M", "1k", "5k", "15k", "30k", "60k")
            count: 返回数量 (可选)
            
        Returns:
            DataFrame: K线数据
        """
        if not self._connected:
            self.connect()
        
        try:
            import xtquant.xtdata as xtd
            
            # 获取K线数据
            df = xtd.get_full_kline(
                sec_code, 
                period, 
                start_date, 
                end_date,
                count=count
            )
            
            if df is not None and not df.empty:
                df['sec_code'] = sec_code
                df['period'] = period
                return df
            return pd.DataFrame()
            
        except Exception as e:
            logger.error(f"获取K线数据失败: {e}")
            raise
    
    @retry(data_type="realtime_quote")
    def get_realtime_quote(self, sec_codes: List[str]) -> pd.DataFrame:
        """
        获取实时行情
        
        Args:
            sec_codes: 证券代码列表
            
        Returns:
            DataFrame: 实时行情数据
        """
        if not self._connected:
            self.connect()
        
        try:
            import xtquant.xtdata as xtd
            
            # 获取实时行情
            df = xtd.get_realtime_quote(sec_codes)
            
            if df is not None and not df.empty:
                return df
            return pd.DataFrame()
            
        except Exception as e:
            logger.error(f"获取实时行情失败: {e}")
            raise
    
    @retry(data_type="financial_data")
    def get_financial_data(self, 
                           sec_codes: List[str], 
                           data_type: str = "balancesheet") -> pd.DataFrame:
        """
        获取财务数据
        
        Args:
            sec_codes: 证券代码列表
            data_type: 数据类型 ("balancesheet", "incomestatement", "cashflow")
            
        Returns:
            DataFrame: 财务数据
        """
        if not self._connected:
            self.connect()
        
        try:
            import xtquant.xtdata as xtd
            
            # 获取财务数据
            df = xtd.get_financial_data(sec_codes, data_type)
            
            if df is not None and not df.empty:
                return df
            return pd.DataFrame()
            
        except Exception as e:
            logger.error(f"获取财务数据失败: {e}")
            raise
    
    @retry(data_type="etf_info")
    def get_etf_list(self) -> pd.DataFrame:
        """
        获取ETF列表
        
        Returns:
            DataFrame: ETF列表
        """
        if not self._connected:
            self.connect()
        
        try:
            import xtquant.xtdata as xtd
            
            # 获取ETF列表
            etf_list = xtd.get_etf_list()
            
            if etf_list:
                df = pd.DataFrame({
                    'sec_code': etf_list,
                    'list_time': datetime.now()
                })
                return df
            return pd.DataFrame()
            
        except Exception as e:
            logger.error(f"获取ETF列表失败: {e}")
            raise
    
    @retry(data_type="index_weight")
    def get_index_weight(self, index_code: str) -> pd.DataFrame:
        """
        获取指数成分股权重
        
        Args:
            index_code: 指数代码 (如: "000300.SH" 沪深300)
            
        Returns:
            DataFrame: 指数成分股权重
        """
        if not self._connected:
            self.connect()
        
        try:
            import xtquant.xtdata as xtd
            
            # 获取指数成分股
            df = xtd.get_index_weight(index_code)
            
            if df is not None and not df.empty:
                df['index_code'] = index_code
                return df
            return pd.DataFrame()
            
        except Exception as e:
            logger.error(f"获取指数成分股失败: {e}")
            raise
    
    def subscribe_quote(self, sec_codes: List[str], callback=None) -> bool:
        """
        订阅实时行情
        
        Args:
            sec_codes: 证券代码列表
            callback: 回调函数 (可选)
            
        Returns:
            bool: 是否成功
        """
        if not self._connected:
            self.connect()
        
        try:
            import xtquant.xtdata as xtd
            
            # 订阅行情
            xtd.subscribe_quote(sec_codes, callback=callback)
            logger.info(f"已订阅行情: {sec_codes}")
            return True
            
        except Exception as e:
            logger.error(f"订阅行情失败: {e}")
            return False


# 全局客户端实例
_qmt_client_instance: Optional[QMTClient] = None


def get_qmt_client(qmt_path: Optional[str] = None, 
                   account_id: Optional[str] = None) -> QMTClient:
    """获取 QMT 客户端实例（单例）"""
    global _qmt_client_instance
    if _qmt_client_instance is None:
        _qmt_client_instance = QMTClient(qmt_path, account_id)
    return _qmt_client_instance


def close_qmt_client():
    """关闭 QMT 客户端"""
    global _qmt_client_instance
    if _qmt_client_instance:
        _qmt_client_instance.disconnect()
        _qmt_client_instance = None
