"""
AmazingData SDK 客户端模块
"""
import sys
from datetime import datetime, date, timedelta
from typing import Optional, List, Dict, Any, Union
import pandas as pd

# SDK 状态 - 延迟导入避免阻塞
SDK_AVAILABLE = None  # None = 未检查, True = 可用, False = 不可用

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

from src.config import config
from src.logger import logger
from src.retry import retry, retry_manager
from src.models import DataSource


class AmazingDataClient:
    """AmazingData 客户端"""

    def __init__(self, account: Optional[str] = None,
                 password: Optional[str] = None,
                 ip: Optional[str] = None,
                 port: Optional[int] = None):
        self.account = account or config.amazing_data.account
        self.password = password or config.amazing_data.password
        self.ip = ip or config.amazing_data.ip
        self.port = port or config.amazing_data.port

        self._client = None
        self._connected = False

    def connect(self) -> bool:
        """连接服务器"""
        if not _check_sdk():
            logger.error("AmazingData SDK 不可用")
            return False

        try:
            import AmazingData as ad
            
            # 登录
            result = ad.login(
                self.account,
                self.password,
                self.ip,
                self.port
            )
            
            if result:
                # 获取 BaseData 和 Calendar
                self._base = ad.BaseData()
                self._calendar = self._base.get_calendar()
                
                # 创建 MarketData
                self._client = ad.MarketData(self._calendar)
                self._connected = True
                logger.info(f"成功连接到 AmazingData 服务器 ({self.ip}:{self.port})")
            else:
                logger.error("登录失败")
                self._connected = False
                
            return result
        except Exception as e:
            logger.error(f"连接 AmazingData 失败: {e}")
            self._connected = False
            return False

    def disconnect(self):
        """断开连接"""
        if self._client:
            try:
                self._client.logout()
            except:
                pass
        self._connected = False
        logger.info("已断开与 AmazingData 的连接")

    @property
    def is_connected(self) -> bool:
        return self._connected

    @retry(max_attempts=3, data_type="login")
    def login(self) -> bool:
        """登录（带重试）"""
        return self.connect()

    # ==================== 基础数据接口 ====================

    @retry(data_type="security_info")
    def get_security_info(self, trade_date: Optional[str] = None) -> pd.DataFrame:
        """
        获取每日最新证券信息

        Args:
            trade_date: 交易日期 (YYYYMMDD)
        """
        if not self._connected:
            self.connect()

        if trade_date is None:
            trade_date = datetime.now().strftime("%Y%m%d")

        data = self._client.get_daily_security_info(trade_date)
        df = pd.DataFrame(data)

        logger.info(f"获取证券信息 {trade_date}, 共 {len(df)} 条")
        return df

    @retry(data_type="security_code")
    def get_security_code(self, trade_date: Optional[str] = None) -> pd.DataFrame:
        """获取每日最新代码表（沪深北）"""
        if not self._connected:
            self.connect()

        if trade_date is None:
            trade_date = datetime.now().strftime("%Y%m%d")

        data = self._client.get_security_code(trade_date)
        return pd.DataFrame(data)

    @retry(data_type="futures_code")
    def get_futures_code(self, trade_date: Optional[str] = None) -> pd.DataFrame:
        """获取每日最新代码表（期货交易所）"""
        if not self._connected:
            self.connect()

        if trade_date is None:
            trade_date = datetime.now().strftime("%Y%m%d")

        data = self._client.get_futures_code(trade_date)
        return pd.DataFrame(data)

    @retry(data_type="adjustment_factor")
    def get_adjustment_factor(self, sec_code: str,
                               start_date: Optional[str] = None,
                               end_date: Optional[str] = None) -> pd.DataFrame:
        """
        获取复权因子

        Args:
            sec_code: 证券代码
            start_date: 开始日期 (YYYYMMDD)
            end_date: 结束日期 (YYYYMMDD)
        """
        if not self._connected:
            self.connect()

        data = self._client.get_adjustment_factor(
            sec_code,
            start_date=start_date,
            end_date=end_date
        )
        return pd.DataFrame(data)

    @retry(data_type="trading_calendar")
    def get_trading_calendar(self, start_date: str, end_date: str) -> pd.DataFrame:
        """获取交易日历"""
        if not self._connected:
            self.connect()

        data = self._client.get_trading_calendar(start_date, end_date)
        return pd.DataFrame(data)

    @retry(data_type="security_basic")
    def get_security_basic(self, sec_code: Optional[str] = None) -> pd.DataFrame:
        """获取证券基础信息"""
        if not self._connected:
            self.connect()

        data = self._client.get_security_basic(sec_code=sec_code)
        return pd.DataFrame(data)

    # ==================== 实时行情接口 ====================

    @retry(data_type="index_snapshot")
    def get_index_snapshot(self, index_code: Optional[str] = None) -> pd.DataFrame:
        """获取指数实时快照"""
        if not self._connected:
            self.connect()

        data = self._client.get_index_snapshot(index_code)
        return pd.DataFrame(data)

    @retry(data_type="stock_snapshot")
    def get_stock_snapshot(self, sec_code: Optional[str] = None) -> pd.DataFrame:
        """获取股票实时快照"""
        if not self._connected:
            self.connect()

        from datetime import datetime
        today = int(datetime.now().strftime("%Y%m%d"))
        
        # 获取股票代码列表
        if sec_code:
            code_list = [sec_code]
        else:
            base_data = self._base
            code_list = base_data.get_code_list(security_type='EXTRA_STOCK_A')
        
        # 查询快照
        snapshot_dict = self._client.query_snapshot(code_list, begin_date=today, end_date=today)
        
        # 转换为 DataFrame
        if snapshot_dict and 'data' in snapshot_dict:
            return pd.DataFrame(snapshot_dict['data'])
        return pd.DataFrame()

    @retry(data_type="realtime_kline")
    def get_realtime_kline(self, sec_code: str,
                           kline_type: str = "1K",
                           count: int = 100) -> pd.DataFrame:
        """获取实时 K 线"""
        if not self._connected:
            self.connect()

        from datetime import datetime
        today = datetime.now().strftime("%Y%m%d")
        data = self._client.query_kline([sec_code], today, today, period=10000)
        return pd.DataFrame(data)

    # ==================== 历史行情接口 ====================

    @retry(data_type="historical_snapshot")
    def get_historical_snapshot(self, sec_code: str,
                                 start_date: str,
                                 end_date: str) -> pd.DataFrame:
        """获取历史快照"""
        if not self._connected:
            self.connect()

        data = self._client.get_historical_snapshot(
            sec_code, start_date, end_date
        )
        return pd.DataFrame(data)

    @retry(data_type="historical_kline")
    def get_historical_kline(self, sec_code: str,
                              kline_type: str = "1D",
                              start_date: Optional[str] = None,
                              end_date: Optional[str] = None,
                              count: Optional[int] = None) -> pd.DataFrame:
        """获取历史 K 线"""
        if not self._connected:
            self.connect()

        start = start_date or "20240101"
        end = end_date or "20991231"
        
        # kline_type to period mapping
        period_map = {"1D": 10000, "1W": 10001, "1M": 10002, "1K": 1, "5K": 5, "15K": 15, "30K": 30, "60K": 60}
        period = period_map.get(kline_type, 10000)
        
        data = self._client.query_kline([sec_code], start, end, period=period)
        return pd.DataFrame(data)

    # ==================== 财务数据接口 ====================

    @retry(data_type="balance_sheet")
    def get_balance_sheet(self, sec_code: str,
                          report_type: str = "1",
                          start_date: Optional[str] = None,
                          end_date: Optional[str] = None) -> pd.DataFrame:
        """获取资产负债表"""
        if not self._connected:
            self.connect()

        data = self._client.get_balance_sheet(
            sec_code, report_type, start_date, end_date
        )
        return pd.DataFrame(data)

    @retry(data_type="cash_flow")
    def get_cash_flow(self, sec_code: str,
                       report_type: str = "1",
                       start_date: Optional[str] = None,
                       end_date: Optional[str] = None) -> pd.DataFrame:
        """获取现金流量表"""
        if not self._connected:
            self.connect()

        data = self._client.get_cash_flow(
            sec_code, report_type, start_date, end_date
        )
        return pd.DataFrame(data)

    @retry(data_type="income")
    def get_income(self, sec_code: str,
                    report_type: str = "1",
                    start_date: Optional[str] = None,
                    end_date: Optional[str] = None) -> pd.DataFrame:
        """获取利润表"""
        if not self._connected:
            self.connect()

        data = self._client.get_income(
            sec_code, report_type, start_date, end_date
        )
        return pd.DataFrame(data)

    @retry(data_type="express_report")
    def get_express_report(self, sec_code: str,
                            start_date: Optional[str] = None,
                            end_date: Optional[str] = None) -> pd.DataFrame:
        """获取业绩快报"""
        if not self._connected:
            self.connect()

        data = self._client.get_express_report(sec_code, start_date, end_date)
        return pd.DataFrame(data)

    @retry(data_type="forecast_report")
    def get_forecast_report(self, sec_code: str,
                             start_date: Optional[str] = None,
                             end_date: Optional[str] = None) -> pd.DataFrame:
        """获取业绩预告"""
        if not self._connected:
            self.connect()

        data = self._client.get_forecast_report(sec_code, start_date, end_date)
        return pd.DataFrame(data)

    # ==================== 股东股本数据接口 ====================

    @retry(data_type="top10_holders")
    def get_top10_holders(self, sec_code: str,
                          start_date: Optional[str] = None,
                          end_date: Optional[str] = None) -> pd.DataFrame:
        """获取十大股东数据"""
        if not self._connected:
            self.connect()

        data = self._client.get_top10_holders(sec_code, start_date, end_date)
        return pd.DataFrame(data)

    @retry(data_type="shareholder_count")
    def get_shareholder_count(self, sec_code: str,
                               start_date: Optional[str] = None,
                               end_date: Optional[str] = None) -> pd.DataFrame:
        """获取股东户数"""
        if not self._connected:
            self.connect()

        data = self._client.get_shareholder_count(sec_code, start_date, end_date)
        return pd.DataFrame(data)

    @retry(data_type="share_structure")
    def get_share_structure(self, sec_code: str,
                            start_date: Optional[str] = None,
                            end_date: Optional[str] = None) -> pd.DataFrame:
        """获取股本结构"""
        if not self._connected:
            self.connect()

        data = self._client.get_share_structure(sec_code, start_date, end_date)
        return pd.DataFrame(data)

    # ==================== 融资融券数据接口 ====================

    @retry(data_type="margin_summary")
    def get_margin_summary(self, trade_date: Optional[str] = None) -> pd.DataFrame:
        """获取融资融券成交汇总"""
        if not self._connected:
            self.connect()

        if trade_date is None:
            trade_date = datetime.now().strftime("%Y%m%d")

        data = self._client.get_margin_summary(trade_date)
        return pd.DataFrame(data)

    @retry(data_type="margin_detail")
    def get_margin_detail(self, sec_code: str,
                           trade_date: Optional[str] = None) -> pd.DataFrame:
        """获取融资融券交易明细"""
        if not self._connected:
            self.connect()

        data = self._client.get_margin_detail(sec_code, trade_date)
        return pd.DataFrame(data)

    # ==================== 龙虎榜数据接口 ====================

    @retry(data_type="dragon_tiger")
    def get_dragon_tiger(self, trade_date: Optional[str] = None) -> pd.DataFrame:
        """获取龙虎榜数据"""
        if not self._connected:
            self.connect()

        if trade_date is None:
            trade_date = datetime.now().strftime("%Y%m%d")

        data = self._client.get_dragon_tiger(trade_date)
        return pd.DataFrame(data)

    @retry(data_type="block_trade")
    def get_block_trade(self, sec_code: Optional[str] = None,
                         start_date: Optional[str] = None,
                         end_date: Optional[str] = None) -> pd.DataFrame:
        """获取大宗交易数据"""
        if not self._connected:
            self.connect()

        data = self._client.get_block_trade(sec_code, start_date, end_date)
        return pd.DataFrame(data)

    # ==================== 期权数据接口 ====================

    @retry(data_type="option_info")
    def get_option_info(self, exchange: Optional[str] = None) -> pd.DataFrame:
        """获取期权基本资料"""
        if not self._connected:
            self.connect()
        data = self._client.get_option_info(exchange)
        return pd.DataFrame(data)

    @retry(data_type="option_contract")
    def get_option_contract(self, option_code: str) -> pd.DataFrame:
        """获取期权标准合约属性"""
        if not self._connected:
            self.connect()
        data = self._client.get_option_contract(option_code)
        return pd.DataFrame(data)

    @retry(data_type="option_monthly")
    def get_option_monthly(self, option_code: str, trade_date: Optional[str] = None) -> pd.DataFrame:
        """获取期权月合约属性变动"""
        if not self._connected:
            self.connect()
        data = self._client.get_option_monthly(option_code, trade_date)
        return pd.DataFrame(data)

    # ==================== ETF 数据接口 ====================

    @retry(data_type="etf_redeem")
    def get_etf_redeem(self, etf_code: Optional[str] = None, trade_date: Optional[str] = None) -> pd.DataFrame:
        """获取 ETF 每日最新申赎数据"""
        if not self._connected:
            self.connect()
        if trade_date is None:
            trade_date = datetime.now().strftime("%Y%m%d")
        data = self._client.get_etf_redeem(etf_code, trade_date)
        return pd.DataFrame(data)

    @retry(data_type="etf_shares")
    def get_etf_shares(self, etf_code: Optional[str] = None, start_date: Optional[str] = None, end_date: Optional[str] = None) -> pd.DataFrame:
        """获取 ETF 基金份额"""
        if not self._connected:
            self.connect()
        data = self._client.get_etf_shares(etf_code, start_date, end_date)
        return pd.DataFrame(data)

    @retry(data_type="etf_iopv")
    def get_etf_iopv(self, etf_code: Optional[str] = None, trade_date: Optional[str] = None) -> pd.DataFrame:
        """获取 ETF 每日收盘 iopv"""
        if not self._connected:
            self.connect()
        if trade_date is None:
            trade_date = datetime.now().strftime("%Y%m%d")
        data = self._client.get_etf_iopv(etf_code, trade_date)
        return pd.DataFrame(data)

    # ==================== 交易所指数接口 ====================

    @retry(data_type="index_components")
    def get_index_components(self, index_code: str) -> pd.DataFrame:
        """获取交易所指数成分股"""
        if not self._connected:
            self.connect()
        data = self._client.get_index_components(index_code)
        return pd.DataFrame(data)

    @retry(data_type="index_weight")
    def get_index_weight(self, index_code: str, trade_date: Optional[str] = None) -> pd.DataFrame:
        """获取交易所指数成分股日权重"""
        if not self._connected:
            self.connect()
        if trade_date is None:
            trade_date = datetime.now().strftime("%Y%m%d")
        data = self._client.get_index_weight(index_code, trade_date)
        return pd.DataFrame(data)

    # ==================== 行业指数接口 ====================

    @retry(data_type="industry_index_info")
    def get_industry_index_info(self, industry_code: Optional[str] = None) -> pd.DataFrame:
        """获取行业指数基本信息"""
        if not self._connected:
            self.connect()
        data = self._client.get_industry_index_info(industry_code)
        return pd.DataFrame(data)

    @retry(data_type="industry_components")
    def get_industry_index_components(self, industry_code: str) -> pd.DataFrame:
        """获取行业指数成分股"""
        if not self._connected:
            self.connect()
        data = self._client.get_industry_index_components(industry_code)
        return pd.DataFrame(data)

    @retry(data_type="industry_weight")
    def get_industry_index_weight(self, industry_code: str, trade_date: Optional[str] = None) -> pd.DataFrame:
        """获取行业指数成分股日权重"""
        if not self._connected:
            self.connect()
        if trade_date is None:
            trade_date = datetime.now().strftime("%Y%m%d")
        data = self._client.get_industry_index_weight(industry_code, trade_date)
        return pd.DataFrame(data)

    @retry(data_type="industry_index_daily")
    def get_industry_index_daily(self, industry_code: Optional[str] = None, start_date: Optional[str] = None, end_date: Optional[str] = None) -> pd.DataFrame:
        """获取行业指数日行情"""
        if not self._connected:
            self.connect()
        data = self._client.get_industry_index_daily(industry_code, start_date, end_date)
        return pd.DataFrame(data)

    # ==================== 可转债接口 ====================

    @retry(data_type="cb_issuance")
    def get_cb_issuance(self, cb_code: Optional[str] = None, start_date: Optional[str] = None, end_date: Optional[str] = None) -> pd.DataFrame:
        """获取可转债发行"""
        if not self._connected:
            self.connect()
        data = self._client.get_cb_issuance(cb_code, start_date, end_date)
        return pd.DataFrame(data)

    @retry(data_type="cb_shares")
    def get_cb_shares(self, cb_code: Optional[str] = None, start_date: Optional[str] = None, end_date: Optional[str] = None) -> pd.DataFrame:
        """获取可转债份额"""
        if not self._connected:
            self.connect()
        data = self._client.get_cb_shares(cb_code, start_date, end_date)
        return pd.DataFrame(data)

    @retry(data_type="cb_conversion")
    def get_cb_conversion(self, cb_code: Optional[str] = None, start_date: Optional[str] = None, end_date: Optional[str] = None) -> pd.DataFrame:
        """获取可转债转股数据"""
        if not self._connected:
            self.connect()
        data = self._client.get_cb_conversion(cb_code, start_date, end_date)
        return pd.DataFrame(data)

    @retry(data_type="cb_conversion_change")
    def get_cb_conversion_change(self, cb_code: str, trade_date: Optional[str] = None) -> pd.DataFrame:
        """获取可转债转股变动数据"""
        if not self._connected:
            self.connect()
        data = self._client.get_cb_conversion_change(cb_code, trade_date)
        return pd.DataFrame(data)

    @retry(data_type="cb_modification")
    def get_cb_modification(self, cb_code: Optional[str] = None, start_date: Optional[str] = None, end_date: Optional[str] = None) -> pd.DataFrame:
        """获取可转债修正数据"""
        if not self._connected:
            self.connect()
        data = self._client.get_cb_modification(cb_code, start_date, end_date)
        return pd.DataFrame(data)

    @retry(data_type="cb_redemption")
    def get_cb_redemption(self, cb_code: Optional[str] = None, start_date: Optional[str] = None, end_date: Optional[str] = None) -> pd.DataFrame:
        """获取可转债赎回数据"""
        if not self._connected:
            self.connect()
        data = self._client.get_cb_redemption(cb_code, start_date, end_date)
        return pd.DataFrame(data)

    @retry(data_type="cb_put")
    def get_cb_put(self, cb_code: Optional[str] = None, start_date: Optional[str] = None, end_date: Optional[str] = None) -> pd.DataFrame:
        """获取可转债回售数据"""
        if not self._connected:
            self.connect()
        data = self._client.get_cb_put(cb_code, start_date, end_date)
        return pd.DataFrame(data)

    @retry(data_type="cb_put_clause")
    def get_cb_put_clause(self, cb_code: Optional[str] = None) -> pd.DataFrame:
        """获取可转债回售赎回条款"""
        if not self._connected:
            self.connect()
        data = self._client.get_cb_put_clause(cb_code)
        return pd.DataFrame(data)

    @retry(data_type="cb_put_exec")
    def get_cb_put_exec(self, cb_code: Optional[str] = None, start_date: Optional[str] = None, end_date: Optional[str] = None) -> pd.DataFrame:
        """获取可转债回售条款执行说明"""
        if not self._connected:
            self.connect()
        data = self._client.get_cb_put_exec(cb_code, start_date, end_date)
        return pd.DataFrame(data)

    @retry(data_type="cb_redemption_exec")
    def get_cb_redemption_exec(self, cb_code: Optional[str] = None, start_date: Optional[str] = None, end_date: Optional[str] = None) -> pd.DataFrame:
        """获取可转债赎回条款执行说明"""
        if not self._connected:
            self.connect()
        data = self._client.get_cb_redemption_exec(cb_code, start_date, end_date)
        return pd.DataFrame(data)

    @retry(data_type="cb_suspension")
    def get_cb_suspension(self, cb_code: Optional[str] = None, start_date: Optional[str] = None, end_date: Optional[str] = None) -> pd.DataFrame:
        """获取可转债停复牌信息"""
        if not self._connected:
            self.connect()
        data = self._client.get_cb_suspension(cb_code, start_date, end_date)
        return pd.DataFrame(data)

    # ==================== 国债收益率接口 ====================

    @retry(data_type="treasury_yield")
    def get_treasury_yield(self, start_date: Optional[str] = None, end_date: Optional[str] = None) -> pd.DataFrame:
        """获取国债收益率"""
        if not self._connected:
            self.connect()
        data = self._client.get_treasury_yield(start_date, end_date)
        return pd.DataFrame(data)

    # ==================== 股东权益接口 ====================

    @retry(data_type="dividend")
    def get_dividend(self, sec_code: str, start_date: Optional[str] = None, end_date: Optional[str] = None) -> pd.DataFrame:
        """获取分红数据"""
        if not self._connected:
            self.connect()
        data = self._client.get_dividend(sec_code, start_date, end_date)
        return pd.DataFrame(data)

    @retry(data_type="rights_issue")
    def get_rights_issue(self, sec_code: str, start_date: Optional[str] = None, end_date: Optional[str] = None) -> pd.DataFrame:
        """获取配股数据"""
        if not self._connected:
            self.connect()
        data = self._client.get_rights_issue(sec_code, start_date, end_date)
        return pd.DataFrame(data)

    # ==================== 股权质押/冻结接口 ====================

    @retry(data_type="share_pledge")
    def get_share_pledge(self, sec_code: str, start_date: Optional[str] = None, end_date: Optional[str] = None) -> pd.DataFrame:
        """获取股权冻结/质押"""
        if not self._connected:
            self.connect()
        data = self._client.get_share_pledge(sec_code, start_date, end_date)
        return pd.DataFrame(data)

    @retry(data_type="restricted_shares")
    def get_restricted_shares(self, sec_code: str, start_date: Optional[str] = None, end_date: Optional[str] = None) -> pd.DataFrame:
        """获取限售股解禁"""
        if not self._connected:
            self.connect()
        data = self._client.get_restricted_shares(sec_code, start_date, end_date)
        return pd.DataFrame(data)

    # ==================== 逆回购/期货/港股通接口 ====================

    @retry(data_type="repo_snapshot")
    def get_repo_snapshot(self, repo_code: Optional[str] = None) -> pd.DataFrame:
        """获取逆回购实时快照"""
        if not self._connected:
            self.connect()
        data = self._client.get_repo_snapshot(repo_code)
        return pd.DataFrame(data)

    @retry(data_type="futures_snapshot")
    def get_futures_snapshot(self, futures_code: Optional[str] = None) -> pd.DataFrame:
        """获取期货实时快照"""
        if not self._connected:
            self.connect()
        data = self._client.get_futures_snapshot(futures_code)
        return pd.DataFrame(data)

    @retry(data_type="hk_connect_snapshot")
    def get_hk_connect_snapshot(self, sec_code: Optional[str] = None) -> pd.DataFrame:
        """获取港股通实时快照"""
        if not self._connected:
            self.connect()
        data = self._client.get_hk_connect_snapshot(sec_code)
        return pd.DataFrame(data)

    @retry(data_type="etf_option_snapshot")
    def get_etf_option_snapshot(self, option_code: Optional[str] = None) -> pd.DataFrame:
        """获取 ETF 期权实时快照"""
        if not self._connected:
            self.connect()
        data = self._client.get_etf_option_snapshot(option_code)
        return pd.DataFrame(data)

    # ==================== 复权因子接口 ====================

    @retry(data_type="adjustment_factor_back")
    def get_adjustment_factor_back(self, sec_code: str, start_date: Optional[str] = None, end_date: Optional[str] = None) -> pd.DataFrame:
        """获取后复权因子"""
        if not self._connected:
            self.connect()
        data = self._client.get_adjustment_factor_back(sec_code, start_date, end_date)
        return pd.DataFrame(data)

    @retry(data_type="adjustment_factor_single")
    def get_adjustment_factor_single(self, sec_code: str, trade_date: str) -> pd.DataFrame:
        """获取单次复权因子"""
        if not self._connected:
            self.connect()
        data = self._client.get_adjustment_factor_single(sec_code, trade_date)
        return pd.DataFrame(data)

    # ==================== 历史代码表接口 ====================

    @retry(data_type="historical_code")
    def get_historical_code(self, sec_code: str, trade_date: str) -> pd.DataFrame:
        """获取历史代码表"""
        if not self._connected:
            self.connect()
        data = self._client.get_historical_code(sec_code, trade_date)
        return pd.DataFrame(data)

    # ==================== 北交所代码对照接口 ====================

    @retry(data_type="bse_code_mapping")
    def get_bse_code_mapping(self) -> pd.DataFrame:
        """获取北交所新旧代码对照表"""
        if not self._connected:
            self.connect()
        data = self._client.get_bj_code_mapping()
        return pd.DataFrame(data)

    # ==================== 别名接口（兼容 PDF 命名） ====================
    
    # get_code_info 的别名
    def get_code_info(self, security_type: Optional[str] = None) -> pd.DataFrame:
        """获取每日最新证券信息（别名）"""
        return self.get_security_info()
    
    # get_code_list 的别名
    def get_code_list(self, security_type: Optional[str] = None) -> pd.DataFrame:
        """获取代码表（别名）"""
        return self.get_security_code(security_type)
    
    # get_future_code_list 的别名
    def get_future_code_list(self, trade_date: Optional[str] = None) -> pd.DataFrame:
        """获取期货代码表（别名）"""
        return self.get_futures_code(trade_date)
    
    # get_option_code_list 的别名
    def get_option_code_list(self, trade_date: Optional[str] = None) -> pd.DataFrame:
        """获取期权代码表（别名）"""
        return self.get_futures_code(trade_date)  # 同期货
    
    # get_history_stock_status 的别名
    def get_history_stock_status(self, sec_code: str, trade_date: str) -> pd.DataFrame:
        """获取历史证券状态（别名）"""
        return self.get_historical_code(sec_code, trade_date)
    
    # get_profit_express 的别名
    def get_profit_express(self, sec_code: str, start_date: Optional[str] = None, end_date: Optional[str] = None) -> pd.DataFrame:
        """获取业绩快报（别名）"""
        return self.get_express_report(sec_code, start_date, end_date)
    
    # get_profit_notice 的别名
    def get_profit_notice(self, sec_code: str, start_date: Optional[str] = None, end_date: Optional[str] = None) -> pd.DataFrame:
        """获取业绩预告（别名）"""
        return self.get_forecast_report(sec_code, start_date, end_date)
    
    # get_share_holder 的别名
    def get_share_holder(self, sec_code: str, start_date: Optional[str] = None, end_date: Optional[str] = None) -> pd.DataFrame:
        """获取十大股东（别名）"""
        return self.get_top10_holders(sec_code, start_date, end_date)
    
    # get_holder_num 的别名
    def get_holder_num(self, sec_code: str, start_date: Optional[str] = None, end_date: Optional[str] = None) -> pd.DataFrame:
        """获取股东户数（别名）"""
        return self.get_shareholder_count(sec_code, start_date, end_date)
    
    # get_equity_structure 的别名
    def get_equity_structure(self, sec_code: str, start_date: Optional[str] = None, end_date: Optional[str] = None) -> pd.DataFrame:
        """获取股本结构（别名）"""
        return self.get_share_structure(sec_code, start_date, end_date)
    
    # get_equity_pledge_freeze 的别名
    def get_equity_pledge_freeze(self, sec_code: str, start_date: Optional[str] = None, end_date: Optional[str] = None) -> pd.DataFrame:
        """获取股权冻结/质押（别名）"""
        return self.get_share_pledge(sec_code, start_date, end_date)
    
    # get_equity_restricted 的别名
    def get_equity_restricted(self, sec_code: str, start_date: Optional[str] = None, end_date: Optional[str] = None) -> pd.DataFrame:
        """获取限售股解禁（别名）"""
        return self.get_restricted_shares(sec_code, start_date, end_date)
    
    # get_long_hu_bang 的别名
    def get_long_hu_bang(self, trade_date: Optional[str] = None) -> pd.DataFrame:
        """获取龙虎榜（别名）"""
        return self.get_dragon_tiger(trade_date)
    
    # get_block_trading 的别名
    def get_block_trading(self, sec_code: Optional[str] = None, start_date: Optional[str] = None, end_date: Optional[str] = None) -> pd.DataFrame:
        """获取大宗交易（别名）"""
        return self.get_block_trade(sec_code, start_date, end_date)
    
    # get_option_basic_info 的别名
    def get_option_basic_info(self, exchange: Optional[str] = None) -> pd.DataFrame:
        """获取期权基本资料（别名）"""
        return self.get_option_info(exchange)
    
    # get_option_std_ctr_specs 的别名
    def get_option_std_ctr_specs(self, option_code: str) -> pd.DataFrame:
        """获取期权标准合约属性（别名）"""
        return self.get_option_contract(option_code)
    
    # get_option_mon_ctr_specs 的别名
    def get_option_mon_ctr_specs(self, option_code: str, trade_date: Optional[str] = None) -> pd.DataFrame:
        """获取期权月合约属性（别名）"""
        return self.get_option_monthly(option_code, trade_date)
    
    # get_etf_pcf 的别名
    def get_etf_pcf(self, etf_code: Optional[str] = None, trade_date: Optional[str] = None) -> pd.DataFrame:
        """获取 ETF 申赎数据（别名）"""
        return self.get_etf_redeem(etf_code, trade_date)
    
    # get_fund_share 的别名
    def get_fund_share(self, etf_code: Optional[str] = None, start_date: Optional[str] = None, end_date: Optional[str] = None) -> pd.DataFrame:
        """获取基金份额（别名）"""
        return self.get_etf_shares(etf_code, start_date, end_date)
    
    # get_fund_iopv 的别名
    def get_fund_iopv(self, etf_code: Optional[str] = None, trade_date: Optional[str] = None) -> pd.DataFrame:
        """获取 ETF IOPV（别名）"""
        return self.get_etf_iopv(etf_code, trade_date)
    
    # get_index_constituent 的别名
    def get_index_constituent(self, index_code: str) -> pd.DataFrame:
        """获取指数成分股（别名）"""
        return self.get_index_components(index_code)
    
    # get_industry_base_info 的别名
    def get_industry_base_info(self, industry_code: Optional[str] = None) -> pd.DataFrame:
        """获取行业指数基本信息（别名）"""
        return self.get_industry_index_info(industry_code)
    
    # get_industry_constituent 的别名
    def get_industry_constituent(self, industry_code: str) -> pd.DataFrame:
        """获取行业指数成分股（别名）"""
        return self.get_industry_index_components(industry_code)
    
    # get_industry_weight 的别名
    def get_industry_weight(self, industry_code: str, trade_date: Optional[str] = None) -> pd.DataFrame:
        """获取行业指数权重（别名）"""
        return self.get_industry_index_weight(industry_code, trade_date)
    
    # get_industry_daily 的别名
    def get_industry_daily(self, industry_code: Optional[str] = None, start_date: Optional[str] = None, end_date: Optional[str] = None) -> pd.DataFrame:
        """获取行业指数日行情（别名）"""
        return self.get_industry_index_daily(industry_code, start_date, end_date)
    
    # get_kzz_* 别名
    def get_kzz_issuance(self, cb_code: Optional[str] = None, start_date: Optional[str] = None, end_date: Optional[str] = None) -> pd.DataFrame:
        """获取可转债发行（别名）"""
        return self.get_cb_issuance(cb_code, start_date, end_date)
    
    def get_kzz_share(self, cb_code: Optional[str] = None, start_date: Optional[str] = None, end_date: Optional[str] = None) -> pd.DataFrame:
        """获取可转债份额（别名）"""
        return self.get_cb_shares(cb_code, start_date, end_date)
    
    def get_kzz_conv(self, cb_code: Optional[str] = None, start_date: Optional[str] = None, end_date: Optional[str] = None) -> pd.DataFrame:
        """获取可转债转股数据（别名）"""
        return self.get_cb_conversion(cb_code, start_date, end_date)
    
    def get_kzz_conv_change(self, cb_code: str, trade_date: Optional[str] = None) -> pd.DataFrame:
        """获取可转债转股变动（别名）"""
        return self.get_cb_conversion_change(cb_code, trade_date)
    
    def get_kzz_corr(self, cb_code: Optional[str] = None, start_date: Optional[str] = None, end_date: Optional[str] = None) -> pd.DataFrame:
        """获取可转债修正数据（别名）"""
        return self.get_cb_modification(cb_code, start_date, end_date)
    
    def get_kzz_call(self, cb_code: Optional[str] = None, start_date: Optional[str] = None, end_date: Optional[str] = None) -> pd.DataFrame:
        """获取可转债赎回数据（别名）"""
        return self.get_cb_redemption(cb_code, start_date, end_date)
    
    def get_kzz_put(self, cb_code: Optional[str] = None, start_date: Optional[str] = None, end_date: Optional[str] = None) -> pd.DataFrame:
        """获取可转债回售数据（别名）"""
        return self.get_cb_put(cb_code, start_date, end_date)
    
    def get_kzz_put_call_item(self, cb_code: Optional[str] = None) -> pd.DataFrame:
        """获取可转债回售赎回条款（别名）"""
        return self.get_cb_put_clause(cb_code)
    
    def get_kzz_put_explanation(self, cb_code: Optional[str] = None, start_date: Optional[str] = None, end_date: Optional[str] = None) -> pd.DataFrame:
        """获取可转债回售条款执行说明（别名）"""
        return self.get_cb_put_exec(cb_code, start_date, end_date)
    
    def get_kzz_call_explanation(self, cb_code: Optional[str] = None, start_date: Optional[str] = None, end_date: Optional[str] = None) -> pd.DataFrame:
        """获取可转债赎回条款执行说明（别名）"""
        return self.get_cb_redemption_exec(cb_code, start_date, end_date)
    
    def get_kzz_suspend(self, cb_code: Optional[str] = None, start_date: Optional[str] = None, end_date: Optional[str] = None) -> pd.DataFrame:
        """获取可转债停复牌信息（别名）"""
        return self.get_cb_suspension(cb_code, start_date, end_date)

    # ==================== ETF 实时快照 ====================

    @retry(data_type="etf_snapshot")
    def get_etf_snapshot(self, etf_code: Optional[str] = None) -> pd.DataFrame:
        """获取 ETF 实时快照"""
        if not self._connected:
            self.connect()
        data = self._client.get_etf_snapshot(etf_code)
        return pd.DataFrame(data)

    # ==================== 可转债实时快照 ====================

    @retry(data_type="cb_snapshot")
    def get_cb_snapshot(self, cb_code: Optional[str] = None) -> pd.DataFrame:
        """获取可转债实时快照"""
        if not self._connected:
            self.connect()
        data = self._client.get_cb_snapshot(cb_code)
        return pd.DataFrame(data)

    # ==================== 通用数据获取方法 ====================

    def fetch_data(self, data_type: DataSource,
                   **kwargs) -> pd.DataFrame:
        """
        通用数据获取方法

        Args:
            data_type: 数据类型
            **kwargs: 其他参数

        Returns:
            DataFrame
        """
        method_map = {
            # 基础数据
            DataSource.SECURITY_INFO: self.get_security_info,
            DataSource.SECURITY_CODE: self.get_security_code,
            DataSource.FUTURES_CODE: self.get_futures_code,
            DataSource.OPTIONS_CODE: self.get_futures_code,  # 期权代码同期货
            DataSource.ADJUSTMENT_FACTOR_BACK: self.get_adjustment_factor_back,
            DataSource.ADJUSTMENT_FACTOR_SINGLE: self.get_adjustment_factor_single,
            DataSource.HISTORICAL_CODE: self.get_historical_code,
            DataSource.TRADING_CALENDAR: self.get_trading_calendar,
            DataSource.SECURITY_BASIC: self.get_security_basic,
            DataSource.BSE_CODE_MAPPING: self.get_bse_code_mapping,
            # 实时行情
            DataSource.INDEX_SNAPSHOT: self.get_index_snapshot,
            DataSource.STOCK_SNAPSHOT: self.get_stock_snapshot,
            DataSource.REPO_SNAPSHOT: self.get_repo_snapshot,
            DataSource.FUTURES_SNAPSHOT: self.get_futures_snapshot,
            DataSource.ETF_SNAPSHOT: self.get_etf_snapshot,
            DataSource.CB_SNAPSHOT: self.get_cb_snapshot,
            DataSource.HK_CONNECT_SNAPSHOT: self.get_hk_connect_snapshot,
            DataSource.ETF_OPTION_SNAPSHOT: self.get_etf_option_snapshot,
            DataSource.REALTIME_KLINE: self.get_realtime_kline,
            # 历史行情
            DataSource.HISTORICAL_SNAPSHOT: self.get_historical_snapshot,
            DataSource.HISTORICAL_KLINE: self.get_historical_kline,
            # 财务数据
            DataSource.BALANCE_SHEET: self.get_balance_sheet,
            DataSource.CASH_FLOW: self.get_cash_flow,
            DataSource.INCOME: self.get_income,
            DataSource.EXPRESS_REPORT: self.get_express_report,
            DataSource.FORECAST_REPORT: self.get_forecast_report,
            # 股东股本
            DataSource.TOP10_HOLDERS: self.get_top10_holders,
            DataSource.SHAREHOLDER_COUNT: self.get_shareholder_count,
            DataSource.SHARE_STRUCTURE: self.get_share_structure,
            DataSource.SHARE_PLEDGE: self.get_share_pledge,
            DataSource.RESTRICTED_SHARES: self.get_restricted_shares,
            # 股东权益
            DataSource.DIVIDEND: self.get_dividend,
            DataSource.RIGHTS_ISSUE: self.get_rights_issue,
            # 融资融券
            DataSource.MARGIN_SUMMARY: self.get_margin_summary,
            DataSource.MARGIN_DETAIL: self.get_margin_detail,
            # 交易异动
            DataSource.DRAGON_TIGER: self.get_dragon_tiger,
            DataSource.BLOCK_TRADE: self.get_block_trade,
            # 期权数据
            DataSource.OPTION_INFO: self.get_option_info,
            DataSource.OPTION_CONTRACT: self.get_option_contract,
            DataSource.OPTION_MONTHLY: self.get_option_monthly,
            # ETF 数据
            DataSource.ETF_REDEEM: self.get_etf_redeem,
            DataSource.ETF_SHARES: self.get_etf_shares,
            DataSource.ETF_IOPV: self.get_etf_iopv,
            # 交易所指数
            DataSource.INDEX_COMPONENTS: self.get_index_components,
            DataSource.INDEX_WEIGHT: self.get_index_weight,
            # 行业指数
            DataSource.INDUSTRY_INDEX_INFO: self.get_industry_index_info,
            DataSource.INDUSTRY_INDEX_COMPONENTS: self.get_industry_index_components,
            DataSource.INDUSTRY_INDEX_WEIGHT: self.get_industry_index_weight,
            DataSource.INDUSTRY_INDEX_DAILY: self.get_industry_index_daily,
            # 可转债
            DataSource.CB_ISSUANCE: self.get_cb_issuance,
            DataSource.CB_SHARES: self.get_cb_shares,
            DataSource.CB_CONVERSION: self.get_cb_conversion,
            DataSource.CB_CONVERSION_CHANGE: self.get_cb_conversion_change,
            DataSource.CB_MODIFICATION: self.get_cb_modification,
            DataSource.CB_REDEMPTION: self.get_cb_redemption,
            DataSource.CB_PUT: self.get_cb_put,
            DataSource.CB_PUT_CLAUSE: self.get_cb_put_clause,
            DataSource.CB_PUT_EXEC: self.get_cb_put_exec,
            DataSource.CB_REDEMPTION_EXEC: self.get_cb_redemption_exec,
            DataSource.CB_SUSPENSION: self.get_cb_suspension,
            # 国债收益率
            DataSource.TREASURY_YIELD: self.get_treasury_yield,
        }

        method = method_map.get(data_type)
        if method is None:
            raise ValueError(f"不支持的数据类型: {data_type}")

        return method(**kwargs)


# 全局客户端实例
_client_instance: Optional[AmazingDataClient] = None


def get_client() -> AmazingDataClient:
    """获取客户端实例（单例）"""
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
