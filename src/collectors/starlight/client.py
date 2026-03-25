"""
AmazingData SDK 客户端模块 - 按官方文档实现
文档版本: V1.0.24
"""
import os
from datetime import datetime, date, timedelta
from typing import Optional, List, Dict, Any, Union
import pandas as pd

# SDK 状态检查
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


def _patch_pandas_frequency_aliases():
    """兼容旧版 SDK 仍使用大写频率别名的情况。"""
    try:
        from pandas.tseries import frequencies

        alias_pairs = {
            "S": "s",
            "T": "min",
            "L": "ms",
            "U": "us",
            "N": "ns",
        }

        for attr_name in ("_lite_rule_alias", "_offset_to_period_map", "OFFSET_TO_PERIOD_FREQSTR"):
            mapping = getattr(frequencies, attr_name, None)
            if not isinstance(mapping, dict):
                continue
            for legacy_key, modern_key in alias_pairs.items():
                if legacy_key not in mapping and modern_key in mapping:
                    mapping[legacy_key] = mapping[modern_key]
    except Exception:
        pass


_patch_pandas_frequency_aliases()


class AmazingDataClient:
    """AmazingData 客户端 - 严格按照官方文档实现"""

    def __init__(self, username: Optional[str] = None,
                 password: Optional[str] = None,
                 host: Optional[str] = None,
                 port: Optional[int] = None,
                 local_path: Optional[str] = None):
        """
        初始化客户端
        
        Args:
            username: 用户名（文档要求使用 username）
            password: 密码
            host: 服务器地址（文档要求使用 host）
            port: 端口号
            local_path: 本地缓存路径（Windows: 'D://cache//'）
        """
        self.username = username or config.amazing_data.account
        self.password = password or config.amazing_data.password
        self.host = host or config.amazing_data.ip
        self.port = port or config.amazing_data.port
        
        # Windows 路径格式: D://AmazingData_local_data//
        if local_path is None:
            cache_dir = os.path.join(os.getcwd(), 'amazing_data_cache')
            os.makedirs(cache_dir, exist_ok=True)
            # Windows 使用双斜杠
            self.local_path = cache_dir.replace('\\', '//') + '//'
        else:
            self.local_path = local_path
        
        logger.info(f"本地缓存路径: {self.local_path}")

        self._base = None  # BaseData 实例
        self._info = None  # InfoData 实例
        self._market = None  # MarketData 实例
        self._connected = False

    def connect(self) -> bool:
        """连接服务器 - 按文档实现"""
        if not _check_sdk():
            logger.error("AmazingData SDK 不可用")
            return False

        try:
            import AmazingData as ad
            
            # 文档要求: login(username='...', password='...', host='...', port=...)
            result = ad.login(
                username=self.username,
                password=self.password,
                host=self.host,
                port=self.port
            )
            
            if result:
                self._base = ad.BaseData()
                self._info = ad.InfoData()
                # MarketData 需要 calendar 参数
                calendar = self._base.get_calendar()
                self._market = ad.MarketData(calendar)
                
                self._connected = True
                logger.info(f"成功连接 AmazingData ({self.host}:{self.port})")
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
                ad.logout(username=self.username)
            except:
                pass
        self._connected = False
        logger.info("已断开连接")

    @property
    def is_connected(self) -> bool:
        return self._connected

    def _call_info_method(self, method, *args, is_local: bool = True,
                          force_remote: bool = False, **kwargs):
        """按文档规则调用 InfoData 方法。"""
        if not self._connected:
            self.connect()

        call_kwargs = {
            key: value
            for key, value in kwargs.items()
            if value is not None
        }

        if force_remote:
            call_kwargs["is_local"] = False
        else:
            call_kwargs["is_local"] = is_local
            if is_local:
                call_kwargs["local_path"] = self.local_path

        return method(*args, **call_kwargs)

    def _call_market_method(self, method, code_list: List[str], begin_date: int,
                            end_date: int, period: Optional[int] = None,
                            begin_time: Optional[int] = None,
                            end_time: Optional[int] = None):
        """按文档规则调用 MarketData 方法，省略未提供的可选时间参数。"""
        if not self._connected:
            self.connect()

        call_kwargs = {
            "begin_date": begin_date,
            "end_date": end_date,
        }

        if period is not None:
            call_kwargs["period"] = period
        if begin_time not in (None, 0):
            call_kwargs["begin_time"] = begin_time
        if end_time not in (None, 0):
            call_kwargs["end_time"] = end_time

        return method(code_list, **call_kwargs)

    # ========== 3.5.1 基础接口 ==========
    
    def update_password(self, old_password: str, new_password: str) -> bool:
        """更新密码"""
        if not self._connected:
            self.connect()
        try:
            import AmazingData as ad
            ad.update_password(self.username, old_password, new_password)
            return True
        except Exception as e:
            logger.error(f"更新密码失败: {e}")
            return False

    # ========== 3.5.2 基础数据 (BaseData) ==========
    
    @retry(max_attempts=3, data_type="code_info")
    def get_code_info(self, security_type: str = "EXTRA_STOCK_A") -> pd.DataFrame:
        """3.5.2.1 每日最新证券信息"""
        if not self._connected:
            self.connect()
        return self._base.get_code_info(security_type=security_type)

    @retry(max_attempts=3, data_type="code_list")
    def get_code_list(self, security_type: str = "EXTRA_STOCK_A") -> List[str]:
        """3.5.2.2 每日最新代码表（沪深北）"""
        if not self._connected:
            self.connect()
        return self._base.get_code_list(security_type=security_type)

    @retry(max_attempts=3, data_type="future_code_list")
    def get_future_code_list(self, security_type: str = "EXTRA_FUTURE") -> List[str]:
        """3.5.2.3 每日最新代码表（期货交易所）"""
        if not self._connected:
            self.connect()
        return self._base.get_future_code_list(security_type=security_type)

    @retry(max_attempts=3, data_type="option_code_list")
    def get_option_code_list(self, security_type: str = "EXTRA_ETF_OP") -> List[str]:
        """3.5.2.4 每日最新代码表（期权）"""
        if not self._connected:
            self.connect()
        return self._base.get_option_code_list(security_type=security_type)

    @retry(max_attempts=3, data_type="backward_factor")
    def get_backward_factor(self, code_list: List[str], is_local: bool = True) -> pd.DataFrame:
        """3.5.2.5 复权因子（后复权因子）"""
        if not self._connected:
            self.connect()
        return self._base.get_backward_factor(
            code_list, 
            local_path=self.local_path, 
            is_local=is_local
        )

    @retry(max_attempts=3, data_type="adj_factor")
    def get_adj_factor(self, code_list: List[str], is_local: bool = True) -> pd.DataFrame:
        """3.5.2.6 复权因子（单次复权因子）"""
        if not self._connected:
            self.connect()
        return self._base.get_adj_factor(
            code_list, 
            local_path=self.local_path, 
            is_local=is_local
        )

    @retry(max_attempts=3, data_type="hist_code_list")
    def get_hist_code_list(self, security_type: str, start_date: int, end_date: int) -> List[str]:
        """3.5.2.7 历史代码表"""
        if not self._connected:
            self.connect()
        return self._base.get_hist_code_list(
            security_type=security_type,
            start_date=start_date,
            end_date=end_date,
            local_path=self.local_path
        )

    @retry(max_attempts=3, data_type="calendar")
    def get_calendar(self, data_type: str = "str", market: str = "SH") -> List:
        """3.5.2.8 交易日历"""
        if not self._connected:
            self.connect()
        return self._base.get_calendar(data_type=data_type, market=market)

    @retry(max_attempts=3, data_type="stock_basic")
    def get_stock_basic(self, code_list: List[str]) -> pd.DataFrame:
        """3.5.2.9 证券基础信息"""
        if not self._connected:
            self.connect()
        if not hasattr(self, '_info') or self._info is None:
            import AmazingData as ad
            self._info = ad.InfoData()
        return self._info.get_stock_basic(code_list)

    @retry(max_attempts=3, data_type="history_stock_status")
    def get_history_stock_status(self, code_list: List[str], 
                                  is_local: bool = True,
                                  begin_date: Optional[int] = None,
                                  end_date: Optional[int] = None) -> pd.DataFrame:
        """3.5.2.10 历史证券信息"""
        if not self._connected:
            self.connect()
        if not hasattr(self, '_info') or self._info is None:
            import AmazingData as ad
            self._info = ad.InfoData()
        return self._info.get_history_stock_status(
            code_list,
            local_path=self.local_path,
            is_local=is_local,
            begin_date=begin_date,
            end_date=end_date
        )

    @retry(max_attempts=3, data_type="bj_code_mapping")
    def get_bj_code_mapping(self, is_local: bool = True) -> pd.DataFrame:
        """3.5.2.11 北交所新旧代码对照表"""
        if not self._connected:
            self.connect()
        if not hasattr(self, '_info') or self._info is None:
            import AmazingData as ad
            self._info = ad.InfoData()
        return self._info.get_bj_code_mapping(
            local_path=self.local_path,
            is_local=is_local
        )

    # ========== 3.5.3 实时行情数据 (SubscribeData) ==========
    # 注: 实时订阅需要单独实现，这里暂不实现

    # ========== 3.5.4 历史行情数据 (MarketData) ==========
    
    @retry(max_attempts=3, data_type="snapshot")
    def query_snapshot(self, code_list: List[str], begin_date: int, end_date: int,
                       begin_time: Optional[int] = None, end_time: Optional[int] = None) -> Dict:
        """3.5.4.1 历史快照"""
        return self._call_market_method(
            self._market.query_snapshot,
            code_list=code_list,
            begin_date=begin_date,
            end_date=end_date,
            begin_time=begin_time,
            end_time=end_time
        )

    @retry(max_attempts=3, data_type="kline")
    def query_kline(self, code_list: List[str], begin_date: int, end_date: int,
                    period: Optional[int] = 1440, begin_time: Optional[int] = None,
                    end_time: Optional[int] = None) -> Dict:
        """3.5.4.2 历史K线"""
        period = 1440 if period is None else period
        return self._call_market_method(
            self._market.query_kline,
            code_list=code_list,
            begin_date=begin_date,
            end_date=end_date,
            period=period,
            begin_time=begin_time,
            end_time=end_time
        )

    # ========== 3.5.5 财务数据 (InfoData) ==========

    @retry(max_attempts=3, data_type="balance_sheet")
    def get_balance_sheet(self, code_list: List[str], is_local: bool = True,
                          begin_date: Optional[int] = None, 
                          end_date: Optional[int] = None) -> Dict:
        """3.5.5.1 资产负债表"""
        if not self._connected:
            self.connect()
        return self._info.get_balance_sheet(
            code_list,
            local_path=self.local_path,
            is_local=is_local,
            begin_date=begin_date,
            end_date=end_date
        )

    @retry(max_attempts=3, data_type="cash_flow")
    def get_cash_flow(self, code_list: List[str], is_local: bool = True,
                      begin_date: Optional[int] = None, 
                      end_date: Optional[int] = None) -> Dict:
        """3.5.5.2 现金流量表"""
        if not self._connected:
            self.connect()
        return self._info.get_cash_flow(
            code_list,
            local_path=self.local_path,
            is_local=is_local,
            begin_date=begin_date,
            end_date=end_date
        )

    @retry(max_attempts=3, data_type="income")
    def get_income(self, code_list: List[str], is_local: bool = True,
                   begin_date: Optional[int] = None, 
                   end_date: Optional[int] = None) -> Dict:
        """3.5.5.3 利润表"""
        if not self._connected:
            self.connect()
        return self._info.get_income(
            code_list,
            local_path=self.local_path,
            is_local=is_local,
            begin_date=begin_date,
            end_date=end_date
        )

    @retry(max_attempts=3, data_type="profit_express")
    def get_profit_express(self, code_list: List[str], is_local: bool = True,
                           begin_date: Optional[int] = None, 
                           end_date: Optional[int] = None) -> pd.DataFrame:
        """3.5.5.4 业绩快报"""
        if not self._connected:
            self.connect()
        return self._info.get_profit_express(
            code_list,
            local_path=self.local_path,
            is_local=is_local,
            begin_date=begin_date,
            end_date=end_date
        )

    @retry(max_attempts=3, data_type="profit_notice")
    def get_profit_notice(self, code_list: List[str], is_local: bool = True,
                          begin_date: Optional[int] = None, 
                          end_date: Optional[int] = None) -> pd.DataFrame:
        """3.5.5.5 业绩预告"""
        if not self._connected:
            self.connect()
        return self._info.get_profit_notice(
            code_list,
            local_path=self.local_path,
            is_local=is_local,
            begin_date=begin_date,
            end_date=end_date
        )

    # ========== 3.5.6 股东股本数据 ==========

    @retry(max_attempts=3, data_type="share_holder")
    def get_share_holder(self, code_list: List[str], is_local: bool = True,
                         begin_date: Optional[int] = None, 
                         end_date: Optional[int] = None) -> pd.DataFrame:
        """3.5.6.1 十大股东数据"""
        if not self._connected:
            self.connect()
        return self._info.get_share_holder(
            code_list,
            local_path=self.local_path,
            is_local=is_local,
            begin_date=begin_date,
            end_date=end_date
        )

    @retry(max_attempts=3, data_type="holder_num")
    def get_holder_num(self, code_list: List[str], is_local: bool = True,
                       begin_date: Optional[int] = None, 
                       end_date: Optional[int] = None) -> pd.DataFrame:
        """3.5.6.2 股东户数"""
        if not self._connected:
            self.connect()
        return self._info.get_holder_num(
            code_list,
            local_path=self.local_path,
            is_local=is_local,
            begin_date=begin_date,
            end_date=end_date
        )

    @retry(max_attempts=3, data_type="equity_structure")
    def get_equity_structure(self, code_list: List[str], is_local: bool = True,
                             begin_date: Optional[int] = None, 
                             end_date: Optional[int] = None) -> pd.DataFrame:
        """3.5.6.3 股本结构"""
        if not self._connected:
            self.connect()
        return self._info.get_equity_structure(
            code_list,
            local_path=self.local_path,
            is_local=is_local,
            begin_date=begin_date,
            end_date=end_date
        )

    @retry(max_attempts=3, data_type="equity_pledge_freeze")
    def get_equity_pledge_freeze(self, code_list: List[str], is_local: bool = True,
                                  begin_date: Optional[int] = None, 
                                  end_date: Optional[int] = None) -> Dict:
        """3.5.6.4 股权冻结/质押"""
        if not self._connected:
            self.connect()
        return self._info.get_equity_pledge_freeze(
            code_list,
            local_path=self.local_path,
            is_local=is_local,
            begin_date=begin_date,
            end_date=end_date
        )

    @retry(max_attempts=3, data_type="equity_restricted")
    def get_equity_restricted(self, code_list: List[str], is_local: bool = True,
                              begin_date: Optional[int] = None, 
                              end_date: Optional[int] = None) -> Dict:
        """3.5.6.5 限售股解禁"""
        if not self._connected:
            self.connect()
        return self._info.get_equity_restricted(
            code_list,
            local_path=self.local_path,
            is_local=is_local,
            begin_date=begin_date,
            end_date=end_date
        )

    # ========== 3.5.7 股东权益数据 ==========

    @retry(max_attempts=3, data_type="dividend")
    def get_dividend(self, code_list: List[str], is_local: bool = True,
                     begin_date: Optional[int] = None, 
                     end_date: Optional[int] = None) -> pd.DataFrame:
        """3.5.7.1 分红数据"""
        if not self._connected:
            self.connect()
        return self._info.get_dividend(
            code_list,
            local_path=self.local_path,
            is_local=is_local,
            begin_date=begin_date,
            end_date=end_date
        )

    @retry(max_attempts=3, data_type="right_issue")
    def get_right_issue(self, code_list: List[str], is_local: bool = True,
                        begin_date: Optional[int] = None, 
                        end_date: Optional[int] = None) -> pd.DataFrame:
        """3.5.7.2 配股数据"""
        if not self._connected:
            self.connect()
        return self._info.get_right_issue(
            code_list,
            local_path=self.local_path,
            is_local=is_local,
            begin_date=begin_date,
            end_date=end_date
        )

    # ========== 3.5.8 融资融券数据 ==========

    @retry(max_attempts=3, data_type="margin_summary")
    def get_margin_summary(self, is_local: bool = True,
                           begin_date: Optional[int] = None, 
                           end_date: Optional[int] = None) -> pd.DataFrame:
        """3.5.8.1 融资融券成交汇总"""
        if not self._connected:
            self.connect()
        return self._info.get_margin_summary(
            local_path=self.local_path,
            is_local=is_local,
            begin_date=begin_date,
            end_date=end_date
        )

    @retry(max_attempts=3, data_type="margin_detail")
    def get_margin_detail(self, code_list: List[str], is_local: bool = True,
                          begin_date: Optional[int] = None, 
                          end_date: Optional[int] = None) -> Dict:
        """3.5.8.2 融资融券交易明细"""
        if not self._connected:
            self.connect()
        return self._info.get_margin_detail(
            code_list,
            local_path=self.local_path,
            is_local=is_local,
            begin_date=begin_date,
            end_date=end_date
        )

    # ========== 3.5.9 交易异动数据 ==========

    @retry(max_attempts=3, data_type="long_hu_bang")
    def get_long_hu_bang(self, code_list: List[str], is_local: bool = True,
                         begin_date: Optional[int] = None, 
                         end_date: Optional[int] = None) -> pd.DataFrame:
        """3.5.9.1 龙虎榜"""
        if not self._connected:
            self.connect()
        return self._info.get_long_hu_bang(
            code_list,
            local_path=self.local_path,
            is_local=is_local,
            begin_date=begin_date,
            end_date=end_date
        )

    @retry(max_attempts=3, data_type="block_trading")
    def get_block_trading(self, code_list: List[str], is_local: bool = True,
                          begin_date: Optional[int] = None, 
                          end_date: Optional[int] = None) -> pd.DataFrame:
        """3.5.9.2 大宗交易"""
        if not self._connected:
            self.connect()
        return self._info.get_block_trading(
            code_list,
            local_path=self.local_path,
            is_local=is_local,
            begin_date=begin_date,
            end_date=end_date
        )

    # ========== 3.5.10 期权数据 ==========

    @retry(max_attempts=3, data_type="option_basic_info")
    def get_option_basic_info(self, code_list: List[str], is_local: bool = True) -> pd.DataFrame:
        """3.5.10.1 期权基本资料"""
        if not self._connected:
            self.connect()
        return self._info.get_option_basic_info(
            code_list,
            local_path=self.local_path,
            is_local=is_local
        )

    @retry(max_attempts=3, data_type="option_std_ctr_specs")
    def get_option_std_ctr_specs(self, code_list: List[str], is_local: bool = True) -> pd.DataFrame:
        """3.5.10.2 期权标准合约属性"""
        if not self._connected:
            self.connect()
        return self._info.get_option_std_ctr_specs(
            code_list,
            local_path=self.local_path,
            is_local=is_local
        )

    @retry(max_attempts=3, data_type="option_mon_ctr_specs")
    def get_option_mon_ctr_specs(self, code_list: List[str], is_local: bool = True) -> pd.DataFrame:
        """3.5.10.3 期权月合约属性变动"""
        if not self._connected:
            self.connect()
        return self._info.get_option_mon_ctr_specs(
            code_list,
            local_path=self.local_path,
            is_local=is_local
        )

    # ========== 3.5.11 ETF 数据 ==========

    @retry(max_attempts=3, data_type="etf_pcf")
    def get_etf_pcf(self, code_list: List[str]) -> tuple:
        """3.5.11.1 ETF 每日最新申赎数据"""
        if not self._connected:
            self.connect()
        return self._base.get_etf_pcf(code_list)

    @retry(max_attempts=3, data_type="fund_share")
    def get_fund_share(self, code_list: List[str], is_local: bool = True,
                       begin_date: Optional[int] = None, 
                       end_date: Optional[int] = None) -> Dict:
        """3.5.11.2 ETF 基金份额"""
        if not self._connected:
            self.connect()
        return self._info.get_fund_share(
            code_list,
            local_path=self.local_path,
            is_local=is_local,
            begin_date=begin_date,
            end_date=end_date
        )

    @retry(max_attempts=3, data_type="fund_iopv")
    def get_fund_iopv(self, code_list: List[str], is_local: bool = True,
                      begin_date: Optional[int] = None, 
                      end_date: Optional[int] = None) -> Dict:
        """3.5.11.3 ETF 每日收盘iopv"""
        if not self._connected:
            self.connect()
        return self._info.get_fund_iopv(
            code_list,
            local_path=self.local_path,
            is_local=is_local,
            begin_date=begin_date,
            end_date=end_date
        )

    # ========== 3.5.12 交易所指数数据 ==========

    @retry(max_attempts=3, data_type="index_constituent")
    def get_index_constituent(self, code_list: List[str], is_local: bool = True) -> Dict:
        """3.5.12.1 交易所指数成分股"""
        if not self._connected:
            self.connect()
        return self._info.get_index_constituent(
            code_list,
            local_path=self.local_path,
            is_local=is_local
        )

    @retry(max_attempts=3, data_type="index_weight")
    def get_index_weight(self, code_list: List[str], is_local: bool = True,
                         begin_date: Optional[int] = None, 
                         end_date: Optional[int] = None) -> Dict:
        """3.5.12.2 交易所指数成分股日权重"""
        if not self._connected:
            self.connect()
        return self._info.get_index_weight(
            code_list,
            local_path=self.local_path,
            is_local=is_local,
            begin_date=begin_date,
            end_date=end_date
        )

    # ========== 3.5.13 行业指数数据 ==========

    @retry(max_attempts=3, data_type="industry_base_info")
    def get_industry_base_info(self, is_local: bool = True) -> pd.DataFrame:
        """3.5.13.1 行业指数基本信息"""
        if not self._connected:
            self.connect()
        return self._info.get_industry_base_info(
            local_path=self.local_path,
            is_local=is_local
        )

    @retry(max_attempts=3, data_type="industry_constituent")
    def get_industry_constituent(self, code_list: List[str], is_local: bool = True) -> Dict:
        """3.5.13.2 行业指数成分股"""
        if not self._connected:
            self.connect()
        return self._info.get_industry_constituent(
            code_list,
            local_path=self.local_path,
            is_local=is_local
        )

    @retry(max_attempts=3, data_type="industry_weight")
    def get_industry_weight(self, code_list: List[str], is_local: bool = True,
                            begin_date: Optional[int] = None, 
                            end_date: Optional[int] = None) -> Dict:
        """3.5.13.3 行业指数成分股日权重"""
        return self._call_info_method(
            self._info.get_industry_weight,
            code_list,
            is_local=is_local,
            begin_date=begin_date,
            end_date=end_date
        )

    @retry(max_attempts=3, data_type="industry_daily")
    def get_industry_daily(self, code_list: List[str], is_local: bool = True,
                           begin_date: Optional[int] = None, 
                           end_date: Optional[int] = None) -> Dict:
        """3.5.13.4 行业指数日行情"""
        return self._call_info_method(
            self._info.get_industry_daily,
            code_list,
            is_local=is_local,
            begin_date=begin_date,
            end_date=end_date
        )

    # ========== 3.5.14 可转债数据 ==========

    @retry(max_attempts=3, data_type="kzz_issuance")
    def get_kzz_issuance(self, code_list: List[str], is_local: bool = True) -> pd.DataFrame:
        """3.5.14.1 可转债发行"""
        if not self._connected:
            self.connect()
        return self._info.get_kzz_issuance(
            code_list,
            local_path=self.local_path,
            is_local=is_local
        )

    @retry(max_attempts=3, data_type="kzz_share")
    def get_kzz_share(self, code_list: List[str], is_local: bool = True) -> pd.DataFrame:
        """3.5.14.2 可转债份额"""
        if not self._connected:
            self.connect()
        return self._info.get_kzz_share(
            code_list,
            local_path=self.local_path,
            is_local=is_local
        )

    @retry(max_attempts=3, data_type="kzz_conv")
    def get_kzz_conv(self, code_list: List[str], is_local: bool = True) -> pd.DataFrame:
        """3.5.14.3 可转债转股数据"""
        if not self._connected:
            self.connect()
        return self._info.get_kzz_conv(
            code_list,
            local_path=self.local_path,
            is_local=is_local
        )

    @retry(max_attempts=3, data_type="kzz_conv_change")
    def get_kzz_conv_change(self, code_list: List[str], is_local: bool = True) -> pd.DataFrame:
        """3.5.14.4 可转债转股变动数据"""
        if not self._connected:
            self.connect()
        return self._info.get_kzz_conv_change(
            code_list,
            local_path=self.local_path,
            is_local=is_local
        )

    @retry(max_attempts=3, data_type="kzz_corr")
    def get_kzz_corr(self, code_list: List[str], is_local: bool = True) -> pd.DataFrame:
        """3.5.14.5 可转债修正数据"""
        if not self._connected:
            self.connect()
        return self._info.get_kzz_corr(
            code_list,
            local_path=self.local_path,
            is_local=is_local
        )

    @retry(max_attempts=3, data_type="kzz_call")
    def get_kzz_call(self, code_list: List[str], is_local: bool = True) -> pd.DataFrame:
        """3.5.14.6 可转债赎回数据"""
        if not self._connected:
            self.connect()
        return self._info.get_kzz_call(
            code_list,
            local_path=self.local_path,
            is_local=is_local
        )

    @retry(max_attempts=3, data_type="kzz_put")
    def get_kzz_put(self, code_list: List[str], is_local: bool = True) -> pd.DataFrame:
        """3.5.14.7 可转债回售数据"""
        if not self._connected:
            self.connect()
        return self._info.get_kzz_put(
            code_list,
            local_path=self.local_path,
            is_local=is_local
        )

    @retry(max_attempts=3, data_type="kzz_put_call_item")
    def get_kzz_put_call_item(self, code_list: List[str], is_local: bool = True) -> pd.DataFrame:
        """3.5.14.8 可转债回售赎回条款"""
        return self._call_info_method(
            self._info.get_kzz_put_call_item,
            code_list,
            is_local=is_local,
            force_remote=True
        )

    @retry(max_attempts=3, data_type="kzz_put_explanation")
    def get_kzz_put_explanation(self, code_list: List[str], is_local: bool = True) -> pd.DataFrame:
        """3.5.14.9 可转债回售条款执行说明"""
        return self._call_info_method(
            self._info.get_kzz_put_explanation,
            code_list,
            is_local=is_local,
            force_remote=True
        )

    @retry(max_attempts=3, data_type="kzz_call_explanation")
    def get_kzz_call_explanation(self, code_list: List[str], is_local: bool = True) -> pd.DataFrame:
        """3.5.14.10 可转债赎回条款执行说明"""
        return self._call_info_method(
            self._info.get_kzz_call_explanation,
            code_list,
            is_local=is_local,
            force_remote=True
        )

    @retry(max_attempts=3, data_type="kzz_suspend")
    def get_kzz_suspend(self, code_list: List[str], is_local: bool = True) -> pd.DataFrame:
        """3.5.14.11 可转债停复牌信息"""
        if not self._connected:
            self.connect()
        return self._info.get_kzz_suspend(
            code_list,
            local_path=self.local_path,
            is_local=is_local
        )

    # ========== 3.5.15 国债收益率数据 ==========

    @retry(max_attempts=3, data_type="treasury_yield")
    def get_treasury_yield(self, term_list: List[str], is_local: bool = True,
                           begin_date: Optional[int] = None, 
                           end_date: Optional[int] = None) -> Dict:
        """3.5.15.1 国债收益率"""
        if not self._connected:
            self.connect()
        return self._info.get_treasury_yield(
            term_list,
            local_path=self.local_path,
            is_local=is_local,
            begin_date=begin_date,
            end_date=end_date
        )


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
