"""
数据模型定义
"""
from dataclasses import dataclass, field
from datetime import datetime
from typing import Optional, List, Dict, Any
from enum import Enum


class DataSource(str, Enum):
    """数据源类型"""
    
    # ==================== 基础数据 ====================
    SECURITY_INFO = "security_info"               # 每日最新证券信息
    SECURITY_CODE = "security_code"               # 每日最新代码表（沪深北）
    FUTURES_CODE = "futures_code"                 # 每日最新代码表（期货交易所）
    OPTIONS_CODE = "options_code"                 # 每日最新代码表（期权）
    ADJUSTMENT_FACTOR_BACK = "adjustment_factor_back"  # 复权因子（后复权）
    ADJUSTMENT_FACTOR_SINGLE = "adjustment_factor_single"  # 复权因子（单次）
    HISTORICAL_CODE = "historical_code"           # 历史代码表
    TRADING_CALENDAR = "trading_calendar"         # 交易日历
    SECURITY_BASIC = "security_basic"              # 证券基础信息
    SECURITY_HISTORY = "security_history"          # 历史证券信息
    BSE_CODE_MAPPING = "bse_code_mapping"          # 北交所新旧代码对照表
    
    # ==================== 实时行情 ====================
    INDEX_SNAPSHOT = "index_snapshot"              # 指数实时快照
    STOCK_SNAPSHOT = "stock_snapshot"              # 股票实时快照
    REPO_SNAPSHOT = "repo_snapshot"               # 逆回购实时快照
    FUTURES_SNAPSHOT = "futures_snapshot"         # 期货实时快照
    ETF_SNAPSHOT = "etf_snapshot"                 # ETF 实时快照
    CB_SNAPSHOT = "cb_snapshot"                    # 可转债实时快照
    HK_CONNECT_SNAPSHOT = "hk_connect_snapshot"   # 港股通实时快照
    ETF_OPTION_SNAPSHOT = "etf_option_snapshot"    # ETF 期权实时快照
    REALTIME_KLINE = "realtime_kline"             # 实时 K 线
    
    # ==================== 历史行情 ====================
    HISTORICAL_SNAPSHOT = "historical_snapshot"   # 历史快照
    HISTORICAL_KLINE = "historical_kline"         # 历史 K 线
    
    # ==================== 财务数据 ====================
    BALANCE_SHEET = "balance_sheet"               # 资产负债表
    CASH_FLOW = "cash_flow"                        # 现金流量表
    INCOME = "income"                              # 利润表
    EXPRESS_REPORT = "express_report"             # 业绩快报
    FORECAST_REPORT = "forecast_report"           # 业绩预告
    
    # ==================== 股东股本 ====================
    TOP10_HOLDERS = "top10_holders"               # 十大股东数据
    SHAREHOLDER_COUNT = "shareholder_count"       # 股东户数
    SHARE_STRUCTURE = "share_structure"           # 股本结构
    SHARE_PLEDGE = "share_pledge"                 # 股权冻结/质押
    RESTRICTED_SHARES = "restricted_shares"       # 限售股解禁
    
    # ==================== 股东权益 ====================
    DIVIDEND = "dividend"                         # 分红数据
    RIGHTS_ISSUE = "rights_issue"                  # 配股数据
    
    # ==================== 融资融券 ====================
    MARGIN_SUMMARY = "margin_summary"              # 融资融券成交汇总
    MARGIN_DETAIL = "margin_detail"                # 融资融券交易明细
    
    # ==================== 交易异动 ====================
    DRAGON_TIGER = "dragon_tiger"                 # 龙虎榜
    BLOCK_TRADE = "block_trade"                   # 大宗交易
    
    # ==================== 期权数据 ====================
    OPTION_INFO = "option_info"                   # 期权基本资料
    OPTION_CONTRACT = "option_contract"           # 期权标准合约属性
    OPTION_MONTHLY = "option_monthly"             # 期权月合约属性变动
    
    # ==================== ETF 数据 ====================
    ETF_REDEEM = "etf_redeem"                     # ETF 每日最新申赎数据
    ETF_SHARES = "etf_shares"                     # ETF 基金份额
    ETF_IOPV = "etf_iopv"                         # ETF 每日收盘 iopv
    
    # ==================== 交易所指数 ====================
    INDEX_COMPONENTS = "index_components"          # 交易所指数成分股
    INDEX_WEIGHT = "index_weight"                  # 交易所指数成分股日权重
    
    # ==================== 行业指数 ====================
    INDUSTRY_INDEX_INFO = "industry_index_info"    # 行业指数基本信息
    INDUSTRY_INDEX_COMPONENTS = "industry_components"  # 行业指数成分股
    INDUSTRY_INDEX_WEIGHT = "industry_weight"      # 行业指数成分股日权重
    INDUSTRY_INDEX_DAILY = "industry_index_daily" # 行业指数日行情
    
    # ==================== 可转债 ====================
    CB_ISSUANCE = "cb_issuance"                   # 可转债发行
    CB_SHARES = "cb_shares"                       # 可转债份额
    CB_CONVERSION = "cb_conversion"               # 可转债转股数据
    CB_CONVERSION_CHANGE = "cb_conversion_change"  # 可转债转股变动数据
    CB_MODIFICATION = "cb_modification"           # 可转债修正数据
    CB_REDEMPTION = "cb_redemption"              # 可转债赎回数据
    CB_PUT = "cb_put"                            # 可转债回售数据
    CB_PUT_CLAUSE = "cb_put_clause"              # 可转债回售赎回条款
    CB_PUT_EXEC = "cb_put_exec"                  # 可转债回售条款执行说明
    CB_REDEMPTION_EXEC = "cb_redemption_exec"    # 可转债赎回条款执行说明
    CB_SUSPENSION = "cb_suspension"              # 可转债停复牌信息
    
    # ==================== 国债收益率 ====================
    TREASURY_YIELD = "treasury_yield"            # 国债收益率


@dataclass
class DataFetchRecord:
    """数据获取记录"""
    id: Optional[int] = None
    data_type: str = ""
    fetch_time: datetime = field(default_factory=datetime.now)
    success: bool = False
    record_count: int = 0
    error_message: Optional[str] = None
    retry_count: int = 0
    start_date: Optional[str] = None
    end_date: Optional[str] = None


@dataclass
class SyncStatus:
    """同步状态"""
    data_type: str
    last_sync_time: Optional[datetime]
    last_success_time: Optional[datetime]
    record_count: int
    status: str  # "success", "failed", "in_progress"
    error_message: Optional[str] = None


@dataclass
class APIResponse:
    """API 响应"""
    code: int = 200
    message: str = "success"
    data: Any = None
    total: int = 0
    page: int = 1
    page_size: int = 100
    
    def to_dict(self):
        return {
            "code": self.code,
            "message": self.message,
            "data": self.data,
            "total": self.total,
            "page": self.page,
            "page_size": self.page_size
        }
