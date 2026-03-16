"""
Akshare 数据采集客户端
免费接口：股票列表、个股信息、日线后复权
"""
import time
import akshare as ak
import pandas as pd
from datetime import datetime, timedelta
from typing import Optional, List
from src.common.logger import logger
from src.collectors import BaseCollector


class AkshareClient(BaseCollector):
    """Akshare 客户端"""

    def __init__(self):
        super().__init__("akshare")
        self._last_request_time = 0
        self._min_request_interval = 2  # 最小请求间隔（秒）

    def connect(self) -> bool:
        """连接（无需登录）"""
        self._connected = True
        logger.info("Akshare 连接成功")
        return True

    def disconnect(self):
        """断开连接"""
        self._connected = False
        logger.info("Akshare 已断开")

    def _wait_rate_limit(self):
        """等待速率限制"""
        elapsed = time.time() - self._last_request_time
        if elapsed < self._min_request_interval:
            time.sleep(self._min_request_interval - elapsed)
        self._last_request_time = time.time()

    def get_stock_list(self) -> pd.DataFrame:
        """
        获取股票列表
        
        Returns:
            DataFrame: 股票代码列表
            Columns: code, name
        """
        try:
            self._wait_rate_limit()
            df = ak.stock_info_a_code_name()
            df.columns = ['sec_code', 'name']
            logger.info(f"获取股票列表: {len(df)} 条")
            return df
        except Exception as e:
            logger.error(f"获取股票列表失败: {e}")
            raise

    def get_stock_info(self, sec_code: str) -> pd.DataFrame:
        """
        获取个股信息
        
        Args:
            sec_code: 股票代码 (如: '000001')
            
        Returns:
            DataFrame: 个股信息
        """
        try:
            # 去掉 .SH 或 .SZ 后缀
            code = sec_code.replace('.SH', '').replace('.SZ', '')
            df = ak.stock_individual_info_em(symbol=code)
            
            # 转换为键值对格式
            info = {}
            for _, row in df.iterrows():
                info[row['item']] = row['value']
            
            result = pd.DataFrame([info])
            result['sec_code'] = sec_code
            logger.info(f"获取个股信息: {sec_code}")
            return result
        except Exception as e:
            logger.error(f"获取个股信息失败: {e}")
            raise

    def get_daily_kline(self, 
                       sec_code: str, 
                       start_date: str = None, 
                       end_date: str = None,
                       adjust: str = "qfq") -> pd.DataFrame:
        """
        获取日线后复权股价
        
        Args:
            sec_code: 股票代码 (如: '000001', '600000')
            start_date: 开始日期 (YYYYMMDD 或 YYYY-MM-DD)
            end_date: 结束日期 (YYYYMMDD 或 YYYY-MM-DD)
            adjust: 复权类型 ("qfq"=前复权, "hfq"=后复权, ""=不复权)
            
        Returns:
            DataFrame: 日线数据
            Columns: 日期, 开盘, 收盘, 最高, 最低, 成交量, 成交额, 振幅, 涨跌幅, 涨跌额, 换手率
        """
        try:
            self._wait_rate_limit()
            
            # 去掉 .SH 或 .SZ 后缀
            code = sec_code.replace('.SH', '').replace('.SZ', '')
            
            # 转换日期格式
            if start_date:
                start_date = start_date.replace('-', '')
            if end_date:
                end_date = end_date.replace('-', '')
            
            # 默认获取近一年数据
            if not end_date:
                end_date = datetime.now().strftime('%Y%m%d')
            if not start_date:
                start_date = (datetime.now() - timedelta(days=365)).strftime('%Y%m%d')
            
            df = ak.stock_zh_a_hist(
                symbol=code,
                start_date=start_date,
                end_date=end_date,
                adjust=adjust
            )
            
            # 重命名列
            df = df.rename(columns={
                '日期': 'trade_date',
                '股票代码': 'sec_code',
                '开盘': 'open',
                '收盘': 'close',
                '最高': 'high',
                '最低': 'low',
                '成交量': 'volume',
                '成交额': 'amount',
                '振幅': 'amplitude',
                '涨跌幅': 'pct_change',
                '涨跌额': 'change',
                '换手率': 'turnover'
            })
            
            df['sec_code'] = sec_code
            logger.info(f"获取日线数据: {sec_code}, {len(df)} 条")
            return df
        except Exception as e:
            logger.error(f"获取日线数据失败: {e}")
            raise

    def get_kline(self, sec_code: str, start_date: str, end_date: str, **kwargs) -> pd.DataFrame:
        """获取K线 (get_daily_kline 的别名)"""
        adjust = kwargs.get('adjust', 'qfq')
        return self.get_daily_kline(sec_code, start_date, end_date, adjust)

    def get_realtime_quote(self, sec_codes: List[str]) -> pd.DataFrame:
        """
        获取实时行情
        
        Args:
            sec_codes: 股票代码列表
            
        Returns:
            DataFrame: 实时行情
        """
        try:
            # 去掉后缀
            codes = [c.replace('.SH', '').replace('.SZ', '') for c in sec_codes]
            df = ak.stock_zh_a_spot_em()
            
            # 过滤指定股票
            df = df[df['代码'].isin(codes)]
            df = df.rename(columns={
                '代码': 'sec_code',
                '名称': 'name',
                '最新价': 'last',
                '涨跌幅': 'pct_change',
                '涨跌额': 'change',
                '成交量': 'volume',
                '成交额': 'amount',
                '振幅': 'amplitude',
                '最高': 'high',
                '最低': 'low',
                '今开': 'open',
                '昨收': 'pre_close',
                '换手率': 'turnover',
                '市盈率-动态': 'pe',
                '市净率': 'pb'
            })
            logger.info(f"获取实时行情: {len(df)} 条")
            return df
        except Exception as e:
            logger.error(f"获取实时行情失败: {e}")
            raise


# 全局客户端
_client = None

def get_client() -> AkshareClient:
    """获取客户端实例"""
    global _client
    if _client is None:
        _client = AkshareClient()
        _client.connect()
    return _client

def close_client():
    """关闭客户端"""
    global _client
    if _client:
        _client.disconnect()
        _client = None
