"""
AmazingData SDK 客户端模块
按文档实现的正确接口
"""
import sys
from datetime import datetime, date, timedelta
from typing import Optional, List, Dict, Any, Union
import pandas as pd

# SDK 状态
SDK_AVAILABLE = None

def _check_sdk():
    """检查 SDK 是否可用"""
    global SDK_AVAILABLE
    if SDK_AVAILABLE is not None:
        return SDK_AVAILABLE
    try:
        import tgw
        import AmazingData
        SDK_AVAILABLE = True
        return True
    except Exception as e:
        SDK_AVAILABLE = False
        print(f"警告: AmazingData SDK 不可用: {e}")
        return False

from src.common.config import config
from src.common.logger import logger
from src.common.retry import retry


class AmazingDataClient:
    """AmazingData 客户端 - 按文档实现"""

    def __init__(self, account: Optional[str] = None,
                 password: Optional[str] = None,
                 ip: Optional[str] = None,
                 port: Optional[int] = None):
        self.account = account or config.amazing_data.account
        self.password = password or config.amazing_data.password
        self.ip = ip or config.amazing_data.ip
        self.port = port or config.amazing_data.port

        self._client = None
        self._base = None
        self._info = None
        self._connected = False

    def connect(self) -> bool:
        """连接服务器"""
        if not _check_sdk():
            logger.error("AmazingData SDK 不可用")
            return False

        try:
            import AmazingData as ad
            
            # 登录
            result = ad.login(self.account, self.password, self.ip, self.port)
            
            if result:
                # 获取 BaseData (注意: SDK 1.0.30 有 bug, calendar 可能为空)
                self._base = ad.BaseData()
                
                # 获取 InfoData
                self._info = ad.InfoData()
                
                self._connected = True
                logger.info(f"成功连接 AmazingData ({self.ip}:{self.port})")
            else:
                logger.error("登录失败")
                self._connected = False
                
            return result
        except Exception as e:
            logger.error(f"连接失败: {e}")
            self._connected = False
            return False

    def disconnect(self):
        """断开连接"""
        if self._connected:
            try:
                import AmazingData as ad
                ad.logout()
            except:
                pass
        self._connected = False
        logger.info("已断开连接")

    @property
    def is_connected(self) -> bool:
        return self._connected

    # ========== 基础数据接口 (BaseData) ==========
    
    @retry(max_attempts=3, data_type="code_list")
    def get_code_list(self, security_type: str = "EXTRA_STOCK_A_SH_SZ") -> List[str]:
        """每日最新代码表（沪深北）"""
        if not self._connected:
            self.connect()
        return self._base.get_code_list(security_type=security_type)

    @retry(max_attempts=3, data_type="code_info")
    def get_code_info(self, security_type: str = "EXTRA_STOCK_A") -> pd.DataFrame:
        """每日最新证券信息"""
        if not self._connected:
            self.connect()
        return self._base.get_code_info(security_type=security_type)

    @retry(max_attempts=3, data_type="future_code_list")
    def get_future_code_list(self) -> List[str]:
        """每日最新代码表（期货交易所）"""
        if not self._connected:
            self.connect()
        return self._base.get_future_code_list()

    @retry(max_attempts=3, data_type="option_code_list")
    def get_option_code_list(self, security_type: str = "EXTRA_STOCK_A") -> List[str]:
        """每日最新代码表（期权）"""
        if not self._connected:
            self.connect()
        return self._base.get_option_code_list(security_type=security_type)

    @retry(max_attempts=3, data_type="calendar")
    def get_calendar(self, date: str = None, market: str = "SH", data_type: str = "str") -> List:
        """交易日历"""
        if not self._connected:
            self.connect()
        return self._base.get_calendar(date=date, market=market, data_type=data_type)

    @retry(max_attempts=3, data_type="adj_factor")
    def get_adj_factor(self, sec_codes: List[str], start_date: str, end_date: str) -> pd.DataFrame:
        """复权因子（后复权因子）"""
        if not self._connected:
            self.connect()
        return self._base.get_adj_factor(sec_codes, start_date, end_date)

    @retry(max_attempts=3, data_type="backward_factor")
    def get_backward_factor(self, sec_codes: List[str], start_date: str, end_date: str) -> pd.DataFrame:
        """复权因子（单次复权因子）"""
        if not self._connected:
            self.connect()
        return self._base.get_backward_factor(sec_codes, start_date, end_date)

    @retry(max_attempts=3, data_type="hist_code_list")
    def get_hist_code_list(self, date: str, exchange: str = "SH") -> List[str]:
        """历史代码表"""
        if not self._connected:
            self.connect()
        return self._base.get_hist_code_list(date=date, exchange=exchange)

    @retry(max_attempts=3, data_type="etf_pcf")
    def get_etf_pcf(self, sec_codes: List[str], date: str) -> pd.DataFrame:
        """ETF 申赎清单"""
        if not self._connected:
            self.connect()
        return self._base.get_etf_pcf(sec_codes, date)

    @retry(max_attempts=3, data_type="future_code_info")
    def get_future_code_info(self, exchange: str = "CFFEX") -> pd.DataFrame:
        """期货代码信息"""
        if not self._connected:
            self.connect()
        return self._base.get_future_code_info(exchange=exchange)

    # ========== InfoData 财务数据接口 ==========

    @retry(max_attempts=3, data_type="balance_sheet")
    def get_balance_sheet(self, sec_codes: List[str], report_type: int = 1) -> pd.DataFrame:
        """资产负债表"""
        if not self._connected:
            self.connect()
        return self._info.get_balance_sheet(sec_codes, report_type)

    @retry(max_attempts=3, data_type="cash_flow")
    def get_cash_flow(self, sec_codes: List[str], report_type: int = 1) -> pd.DataFrame:
        """现金流量表"""
        if not self._connected:
            self.connect()
        return self._info.get_cash_flow(sec_codes, report_type)

    @retry(max_attempts=3, data_type="income")
    def get_income(self, sec_codes: List[str], report_type: int = 1) -> pd.DataFrame:
        """利润表"""
        if not self._connected:
            self.connect()
        return self._info.get_income(sec_codes, report_type)

    @retry(max_attempts=3, data_type="express_report")
    def get_express_report(self, sec_codes: List[str]) -> pd.DataFrame:
        """业绩快报"""
        if not self._connected:
            self.connect()
        return self._info.get_express_report(sec_codes)

    @retry(max_attempts=3, data_type="forecast_report")
    def get_forecast_report(self, sec_codes: List[str]) -> pd.DataFrame:
        """业绩预告"""
        if not self._connected:
            self.connect()
        return self._info.get_forecast_report(sec_codes)

    @retry(max_attempts=3, data_type="dividend")
    def get_dividend(self, sec_codes: List[str]) -> pd.DataFrame:
        """分红数据"""
        if not self._connected:
            self.connect()
        return self._info.get_dividend(sec_codes)

    @retry(max_attempts=3, data_type="right_issue")
    def get_right_issue(self, sec_codes: List[str]) -> pd.DataFrame:
        """配股数据"""
        if not self._connected:
            self.connect()
        return self._info.get_right_issue(sec_codes)

    # ========== 股东股本数据 ==========

    @retry(max_attempts=3, data_type="top10_holders")
    def get_top10_holders(self, sec_codes: List[str], date: str) -> pd.DataFrame:
        """十大股东数据"""
        if not self._connected:
            self.connect()
        return self._info.get_share_holder(sec_codes, date)

    @retry(max_attempts=3, data_type="shareholder_count")
    def get_shareholder_count(self, sec_codes: List[str], date: str) -> pd.DataFrame:
        """股东户数"""
        if not self._connected:
            self.connect()
        return self._info.get_holder_num(sec_codes, date)

    @retry(max_attempts=3, data_type="share_structure")
    def get_share_structure(self, sec_codes: List[str], date: str) -> pd.DataFrame:
        """股本结构"""
        if not self._connected:
            self.connect()
        return self._info.get_equity_structure(sec_codes, date)

    @retry(max_attempts=3, data_type="share_pledge")
    def get_share_pledge(self, sec_codes: List[str], date: str) -> pd.DataFrame:
        """股权冻结/质押"""
        if not self._connected:
            self.connect()
        return self._info.get_equity_pledge_freeze(sec_codes, date)

    @retry(max_attempts=3, data_type="restricted_shares")
    def get_restricted_shares(self, sec_codes: List[str], date: str) -> pd.DataFrame:
        """限售股解禁"""
        if not self._connected:
            self.connect()
        return self._info.get_equity_restricted(sec_codes, date)

    # ========== 融资融券数据 ==========

    @retry(max_attempts=3, data_type="margin_summary")
    def get_margin_summary(self, sec_codes: List[str], date: str) -> pd.DataFrame:
        """融资融券成交汇总"""
        if not self._connected:
            self.connect()
        return self._info.get_margin_summary(sec_codes, date)

    @retry(max_attempts=3, data_type="margin_detail")
    def get_margin_detail(self, sec_codes: List[str], date: str) -> pd.DataFrame:
        """融资融券交易明细"""
        if not self._connected:
            self.connect()
        return self._info.get_margin_detail(sec_codes, date)

    # ========== 交易异动数据 ==========

    @retry(max_attempts=3, data_type="dragon_tiger")
    def get_dragon_tiger(self, date: str) -> pd.DataFrame:
        """龙虎榜"""
        if not self._connected:
            self.connect()
        return self._info.get_long_hu_bang(date)

    @retry(max_attempts=3, data_type="block_trade")
    def get_block_trade(self, date: str) -> pd.DataFrame:
        """大宗交易"""
        if not self._connected:
            self.connect()
        return self._info.get_block_trading(date)

    # ========== 期权数据 ==========

    @retry(max_attempts=3, data_type="option_info")
    def get_option_info(self, sec_code: str) -> pd.DataFrame:
        """期权基本资料"""
        if not self._connected:
            self.connect()
        return self._info.get_option_basic_info(sec_code)

    @retry(max_attempts=3, data_type="option_contract")
    def get_option_contract(self, sec_code: str, expire_date: str) -> pd.DataFrame:
        """期权标准合约属性"""
        if not self._connected:
            self.connect()
        return self._info.get_option_std_ctr_specs(sec_code, expire_date)

    @retry(max_attempts=3, data_type="option_monthly")
    def get_option_monthly(self, sec_code: str, date: str) -> pd.DataFrame:
        """期权月合约属性"""
        if not self._connected:
            self.connect()
        return self._info.get_option_mon_ctr_specs(sec_code, date)

    # ========== ETF 数据 ==========

    @retry(max_attempts=3, data_type="etf_shares")
    def get_etf_shares(self, sec_codes: List[str], date: str) -> pd.DataFrame:
        """ETF 基金份额"""
        if not self._connected:
            self.connect()
        return self._info.get_fund_share(sec_codes, date)

    @retry(max_attempts=3, data_type="etf_iopv")
    def get_etf_iopv(self, sec_codes: List[str], date: str) -> pd.DataFrame:
        """ETF iopv"""
        if not self._connected:
            self.connect()
        return self._info.get_fund_iopv(sec_codes, date)

    # ========== 指数数据 ==========

    @retry(max_attempts=3, data_type="index_components")
    def get_index_components(self, index_code: str) -> pd.DataFrame:
        """指数成分股"""
        if not self._connected:
            self.connect()
        return self._info.get_index_constituent(index_code)

    @retry(max_attempts=3, data_type="index_weight")
    def get_index_weight(self, index_code: str, date: str) -> pd.DataFrame:
        """指数成分股权重"""
        if not self._connected:
            self.connect()
        return self._info.get_index_weight(index_code, date)

    # ========== 行业指数数据 ==========

    @retry(max_attempts=3, data_type="industry_index_info")
    def get_industry_index_info(self) -> pd.DataFrame:
        """行业指数基本信息"""
        if not self._connected:
            self.connect()
        return self._info.get_industry_base_info()

    @retry(max_attempts=3, data_type="industry_index_components")
    def get_industry_index_components(self, industry_code: str) -> pd.DataFrame:
        """行业指数成分股"""
        if not self._connected:
            self.connect()
        return self._info.get_industry_constituent(industry_code)

    @retry(max_attempts=3, data_type="industry_index_weight")
    def get_industry_index_weight(self, industry_code: str, date: str) -> pd.DataFrame:
        """行业指数权重"""
        if not self._connected:
            self.connect()
        return self._info.get_industry_weight(industry_code, date)

    @retry(max_attempts=3, data_type="industry_index_daily")
    def get_industry_index_daily(self, industry_code: str, date: str) -> pd.DataFrame:
        """行业指数日行情"""
        if not self._connected:
            self.connect()
        return self._info.get_industry_daily(industry_code, date)

    # ========== 可转债数据 ==========

    @retry(max_attempts=3, data_type="cb_issuance")
    def get_cb_issuance(self, date: str = None) -> pd.DataFrame:
        """可转债发行"""
        if not self._connected:
            self.connect()
        return self._info.get_kzz_issuance(date)

    @retry(max_attempts=3, data_type="cb_shares")
    def get_cb_shares(self, date: str) -> pd.DataFrame:
        """可转债份额"""
        if not self._connected:
            self.connect()
        return self._info.get_kzz_share(date)

    @retry(max_attempts=3, data_type="cb_conversion")
    def get_cb_conversion(self, date: str) -> pd.DataFrame:
        """可转债转股"""
        if not self._connected:
            self.connect()
        return self._info.get_kzz_conv(date)

    @retry(max_attempts=3, data_type="cb_conversion_change")
    def get_cb_conversion_change(self, date: str) -> pd.DataFrame:
        """可转债转股变动"""
        if not self._connected:
            self.connect()
        return self._info.get_kzz_conv_change(date)

    @retry(max_attempts=3, data_type="cb_modification")
    def get_cb_modification(self, date: str) -> pd.DataFrame:
        """可转债修正"""
        if not self._connected:
            self.connect()
        return self._info.get_kzz_corr(date)

    @retry(max_attempts=3, data_type="cb_redemption")
    def get_cb_redemption(self, date: str) -> pd.DataFrame:
        """可转债赎回"""
        if not self._connected:
            self.connect()
        return self._info.get_kzz_call(date)

    @retry(max_attempts=3, data_type="cb_put")
    def get_cb_put(self, date: str) -> pd.DataFrame:
        """可转债回售"""
        if not self._connected:
            self.connect()
        return self._info.get_kzz_put(date)

    @retry(max_attempts=3, data_type="cb_suspension")
    def get_cb_suspension(self, date: str) -> pd.DataFrame:
        """可转债停复牌"""
        if not self._connected:
            self.connect()
        return self._info.get_kzz_suspend(date)

    # ========== 国债收益率 ==========

    @retry(max_attempts=3, data_type="treasury_yield")
    def get_treasury_yield(self, date: str) -> pd.DataFrame:
        """国债收益率"""
        if not self._connected:
            self.connect()
        return self._info.get_treasury_yield(date)

    # ========== 历史状态 ==========

    @retry(max_attempts=3, data_type="history_stock_status")
    def get_history_stock_status(self, sec_codes: List[str], date: str) -> pd.DataFrame:
        """历史证券状态"""
        if not self._connected:
            self.connect()
        return self._info.get_history_stock_status(sec_codes, date)

    # ========== 北交所 ==========

    @retry(max_attempts=3, data_type="bse_mapping")
    def get_bse_code_mapping(self) -> pd.DataFrame:
        """北交所新旧代码对照"""
        if not self._connected:
            self.connect()
        return self._info.get_bj_code_mapping()


# 全局客户端实例
_client_instance = None

def get_client() -> AmazingDataClient:
    """获取客户端实例"""
    global _client_instance
    if _client_instance is None:
        _client_instance = AmazingDataClient()
    return _client_instance

def close_client():
    """关闭客户端"""
    global _client_instance
    if _client_instance:
        _client_instance.disconnect()
        _client_instance = None
