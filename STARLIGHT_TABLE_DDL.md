# Starlight 数据表 DDL

```sql
-- 创建数据库
CREATE DATABASE IF NOT EXISTS starlight;

USE starlight;

-- ============================================
-- 1. 基础数据表
-- ============================================

-- 股票代码表
CREATE TABLE IF NOT EXISTS starlight.stock_codes (
    code String,
    update_time DateTime DEFAULT now()
) ENGINE = MergeTree()
ORDER BY code;

-- 交易日历
CREATE TABLE IF NOT EXISTS starlight.trading_calendar (
    trade_date Date
) ENGINE = MergeTree()
ORDER BY trade_date;

-- 证券基础信息
CREATE TABLE IF NOT EXISTS starlight.stock_basic (
    market_code String,
    sec_name String,
    sec_type String,
    list_date Nullable(Date),
    delist_date Nullable(Date),
    status String,
    exchange String
) ENGINE = MergeTree()
ORDER BY market_code;

-- 后复权因子
CREATE TABLE IF NOT EXISTS starlight.backward_factor (
    code String,
    date Date,
    factor Float64
) ENGINE = ReplacingMergeTree()
ORDER BY (code, date);

-- 前复权因子
CREATE TABLE IF NOT EXISTS starlight.adj_factor (
    code String,
    date Date,
    factor Float64
) ENGINE = ReplacingMergeTree()
ORDER BY (code, date);

-- ============================================
-- 2. 行情数据表
-- ============================================

-- 日K线数据（所有股票统一表）
CREATE TABLE IF NOT EXISTS starlight.kline_daily (
    code String,
    trade_time DateTime,
    open Float64,
    high Float64,
    low Float64,
    close Float64,
    volume Float64,
    amount Float64,
    pre_close Nullable(Float64),
    change_rate Nullable(Float64),
    turnover_rate Nullable(Float64)
) ENGINE = ReplacingMergeTree()
ORDER BY (code, trade_time);

-- 快照数据（所有股票统一表）
CREATE TABLE IF NOT EXISTS starlight.snapshot (
    code String,
    time DateTime,
    last_price Float64,
    open Float64,
    high Float64,
    low Float64,
    volume Float64,
    amount Float64,
    bid_price1 Nullable(Float64),
    bid_volume1 Nullable(Float64),
    ask_price1 Nullable(Float64),
    ask_volume1 Nullable(Float64)
) ENGINE = ReplacingMergeTree()
ORDER BY (code, time);

-- ============================================
-- 3. 财务数据表
-- ============================================

-- 资产负债表（所有股票统一表）
CREATE TABLE IF NOT EXISTS starlight.balance_sheet (
    market_code String,
    reporting_period Date,
    report_type String,
    total_assets Nullable(Float64),
    total_liabilities Nullable(Float64),
    total_equity Nullable(Float64),
    current_assets Nullable(Float64),
    non_current_assets Nullable(Float64),
    current_liabilities Nullable(Float64),
    non_current_liabilities Nullable(Float64)
) ENGINE = ReplacingMergeTree()
ORDER BY (market_code, reporting_period);

-- 现金流量表（所有股票统一表）
CREATE TABLE IF NOT EXISTS starlight.cash_flow (
    market_code String,
    reporting_period Date,
    report_type String,
    operating_cash_flow Nullable(Float64),
    investing_cash_flow Nullable(Float64),
    financing_cash_flow Nullable(Float64),
    net_cash_flow Nullable(Float64)
) ENGINE = ReplacingMergeTree()
ORDER BY (market_code, reporting_period);

-- 利润表（所有股票统一表）
CREATE TABLE IF NOT EXISTS starlight.income (
    market_code String,
    reporting_period Date,
    report_type String,
    total_revenue Nullable(Float64),
    operating_revenue Nullable(Float64),
    total_costs Nullable(Float64),
    operating_profit Nullable(Float64),
    total_profit Nullable(Float64),
    net_profit Nullable(Float64),
    net_profit_parent Nullable(Float64),
    basic_eps Nullable(Float64),
    diluted_eps Nullable(Float64)
) ENGINE = ReplacingMergeTree()
ORDER BY (market_code, reporting_period);

-- 业绩快报
CREATE TABLE IF NOT EXISTS starlight.profit_express (
    market_code String,
    reporting_period Date,
    announce_date Nullable(Date),
    total_revenue Nullable(Float64),
    net_profit Nullable(Float64),
    net_profit_parent Nullable(Float64),
    total_assets Nullable(Float64),
    basic_eps Nullable(Float64),
    roe Nullable(Float64)
) ENGINE = ReplacingMergeTree()
ORDER BY (market_code, reporting_period);

-- 业绩预告
CREATE TABLE IF NOT EXISTS starlight.profit_notice (
    market_code String,
    reporting_period Date,
    announce_date Nullable(Date),
    notice_type String,
    net_profit_min Nullable(Float64),
    net_profit_max Nullable(Float64),
    change_rate_min Nullable(Float64),
    change_rate_max Nullable(Float64),
    summary Nullable(String)
) ENGINE = ReplacingMergeTree()
ORDER BY (market_code, reporting_period);

-- ============================================
-- 4. 股东数据表
-- ============================================

-- 十大股东
CREATE TABLE IF NOT EXISTS starlight.share_holder (
    market_code String,
    holder_enddate Date,
    qty_num Int32,
    holder_name String,
    holder_type String,
    hold_num Nullable(Float64),
    hold_ratio Nullable(Float64),
    change_num Nullable(Float64)
) ENGINE = ReplacingMergeTree()
ORDER BY (market_code, holder_enddate, qty_num);

-- 股东户数
CREATE TABLE IF NOT EXISTS starlight.holder_num (
    market_code String,
    holder_enddate Date,
    holder_num Nullable(Int64),
    holder_num_change Nullable(Float64),
    holder_num_change_rate Nullable(Float64),
    avg_hold_num Nullable(Float64),
    avg_hold_amount Nullable(Float64)
) ENGINE = ReplacingMergeTree()
ORDER BY (market_code, holder_enddate);

-- 股本结构
CREATE TABLE IF NOT EXISTS starlight.equity_structure (
    market_code String,
    change_date Date,
    total_share Nullable(Float64),
    float_share Nullable(Float64),
    restricted_share Nullable(Float64),
    state_share Nullable(Float64),
    legal_person_share Nullable(Float64),
    a_share Nullable(Float64),
    b_share Nullable(Float64),
    h_share Nullable(Float64)
) ENGINE = ReplacingMergeTree()
ORDER BY (market_code, change_date);

-- ============================================
-- 5. 其他数据表
-- ============================================

-- 融资融券汇总
CREATE TABLE IF NOT EXISTS starlight.margin_summary (
    trade_date Date,
    market String,
    margin_balance Nullable(Float64),
    short_balance Nullable(Float64),
    total_balance Nullable(Float64),
    margin_buy_amount Nullable(Float64),
    short_sell_amount Nullable(Float64)
) ENGINE = ReplacingMergeTree()
ORDER BY (trade_date, market);

-- 融资融券明细
CREATE TABLE IF NOT EXISTS starlight.margin_detail (
    market_code String,
    trade_date Date,
    margin_balance Nullable(Float64),
    margin_buy_amount Nullable(Float64),
    margin_repay_amount Nullable(Float64),
    short_balance Nullable(Float64),
    short_sell_volume Nullable(Float64),
    short_repay_volume Nullable(Float64)
) ENGINE = ReplacingMergeTree()
ORDER BY (market_code, trade_date);

-- 龙虎榜
CREATE TABLE IF NOT EXISTS starlight.dragon_tiger (
    market_code String,
    trade_date Date,
    trader_name String,
    buy_amount Nullable(Float64),
    sell_amount Nullable(Float64),
    net_amount Nullable(Float64),
    reason String,
    close_price Nullable(Float64),
    change_rate Nullable(Float64)
) ENGINE = ReplacingMergeTree()
ORDER BY (market_code, trade_date, trader_name);

-- 大宗交易
CREATE TABLE IF NOT EXISTS starlight.block_trade (
    market_code String,
    trade_date Date,
    trade_price Nullable(Float64),
    trade_volume Nullable(Float64),
    trade_amount Nullable(Float64),
    buyer String,
    seller String,
    close_price Nullable(Float64),
    premium_rate Nullable(Float64)
) ENGINE = ReplacingMergeTree()
ORDER BY (market_code, trade_date);

-- 股权质押冻结
CREATE TABLE IF NOT EXISTS starlight.equity_pledge_freeze (
    market_code String,
    announce_date Date,
    holder_name String,
    pledge_num Nullable(Float64),
    pledge_ratio Nullable(Float64),
    pledgee String,
    start_date Nullable(Date),
    end_date Nullable(Date)
) ENGINE = ReplacingMergeTree()
ORDER BY (market_code, announce_date, holder_name);

-- 限售股解禁
CREATE TABLE IF NOT EXISTS starlight.equity_restricted (
    market_code String,
    lift_date Date,
    lift_num Nullable(Float64),
    lift_ratio Nullable(Float64),
    lift_type String,
    holder_name Nullable(String)
) ENGINE = ReplacingMergeTree()
ORDER BY (market_code, lift_date);

-- 分红送股
CREATE TABLE IF NOT EXISTS starlight.dividend (
    market_code String,
    announce_date Date,
    record_date Nullable(Date),
    ex_dividend_date Nullable(Date),
    dividend_date Nullable(Date),
    dividend_ratio Nullable(Float64),
    bonus_ratio Nullable(Float64),
    transfer_ratio Nullable(Float64),
    dividend_amount Nullable(Float64)
) ENGINE = ReplacingMergeTree()
ORDER BY (market_code, announce_date);

-- 配股
CREATE TABLE IF NOT EXISTS starlight.right_issue (
    market_code String,
    announce_date Date,
    record_date Nullable(Date),
    ex_right_date Nullable(Date),
    listing_date Nullable(Date),
    right_ratio Nullable(Float64),
    right_price Nullable(Float64),
    right_num Nullable(Float64)
) ENGINE = ReplacingMergeTree()
ORDER BY (market_code, announce_date);

-- ============================================
-- 6. 指数数据表
-- ============================================

-- 指数成分股
CREATE TABLE IF NOT EXISTS starlight.index_constituent (
    index_code String,
    market_code String,
    in_date Nullable(Date),
    out_date Nullable(Date)
) ENGINE = ReplacingMergeTree()
ORDER BY (index_code, market_code);

-- 指数权重
CREATE TABLE IF NOT EXISTS starlight.index_weight (
    index_code String,
    market_code String,
    trade_date Date,
    weight Nullable(Float64)
) ENGINE = ReplacingMergeTree()
ORDER BY (index_code, market_code, trade_date);

-- ============================================
-- 7. 行业数据表
-- ============================================

-- 行业基础信息
CREATE TABLE IF NOT EXISTS starlight.industry_base_info (
    industry_code String,
    industry_name String,
    industry_level Int32,
    parent_code Nullable(String)
) ENGINE = MergeTree()
ORDER BY industry_code;

-- 行业成分股
CREATE TABLE IF NOT EXISTS starlight.industry_constituent (
    industry_code String,
    market_code String,
    in_date Nullable(Date),
    out_date Nullable(Date)
) ENGINE = ReplacingMergeTree()
ORDER BY (industry_code, market_code);

-- 行业权重
CREATE TABLE IF NOT EXISTS starlight.industry_weight (
    industry_code String,
    market_code String,
    trade_date Date,
    weight Nullable(Float64)
) ENGINE = ReplacingMergeTree()
ORDER BY (industry_code, market_code, trade_date);

-- 行业日行情
CREATE TABLE IF NOT EXISTS starlight.industry_daily (
    industry_code String,
    trade_date Date,
    open Nullable(Float64),
    high Nullable(Float64),
    low Nullable(Float64),
    close Nullable(Float64),
    volume Nullable(Float64),
    amount Nullable(Float64),
    change_rate Nullable(Float64)
) ENGINE = MergeTree()
ORDER BY (industry_code, trade_date);

-- ============================================
-- 8. 可转债数据表
-- ============================================

-- 可转债发行
CREATE TABLE IF NOT EXISTS starlight.kzz_issuance (
    market_code String,
    bond_name String,
    stock_code String,
    issue_date Nullable(Date),
    listing_date Nullable(Date),
    maturity_date Nullable(Date),
    issue_price Nullable(Float64),
    issue_scale Nullable(Float64),
    coupon_rate Nullable(String)
) ENGINE = ReplacingMergeTree()
ORDER BY market_code;

-- 可转债余额
CREATE TABLE IF NOT EXISTS starlight.kzz_share (
    market_code String,
    trade_date Date,
    remain_balance Nullable(Float64),
    remain_ratio Nullable(Float64)
) ENGINE = ReplacingMergeTree()
ORDER BY (market_code, trade_date);

-- 可转债转股
CREATE TABLE IF NOT EXISTS starlight.kzz_conv (
    market_code String,
    trade_date Date,
    conv_price Nullable(Float64),
    conv_value Nullable(Float64),
    conv_premium_rate Nullable(Float64)
) ENGINE = ReplacingMergeTree()
ORDER BY (market_code, trade_date);

-- 转股价变动
CREATE TABLE IF NOT EXISTS starlight.kzz_conv_change (
    market_code String,
    change_date Date,
    old_conv_price Nullable(Float64),
    new_conv_price Nullable(Float64),
    change_reason String
) ENGINE = ReplacingMergeTree()
ORDER BY (market_code, change_date);

-- 可转债相关性
CREATE TABLE IF NOT EXISTS starlight.kzz_corr (
    market_code String,
    trade_date Date,
    bond_price Nullable(Float64),
    stock_price Nullable(Float64),
    correlation Nullable(Float64)
) ENGINE = ReplacingMergeTree()
ORDER BY (market_code, trade_date);

-- 可转债赎回
CREATE TABLE IF NOT EXISTS starlight.kzz_call (
    market_code String,
    announce_date Date,
    call_type String,
    call_price Nullable(Float64),
    call_start_date Nullable(Date),
    call_end_date Nullable(Date)
) ENGINE = ReplacingMergeTree()
ORDER BY (market_code, announce_date);

-- 可转债回售
CREATE TABLE IF NOT EXISTS starlight.kzz_put (
    market_code String,
    announce_date Date,
    put_price Nullable(Float64),
    put_start_date Nullable(Date),
    put_end_date Nullable(Date)
) ENGINE = ReplacingMergeTree()
ORDER BY (market_code, announce_date);

-- 可转债停牌
CREATE TABLE IF NOT EXISTS starlight.kzz_suspend (
    market_code String,
    suspend_date Date,
    resume_date Nullable(Date),
    suspend_reason String
) ENGINE = ReplacingMergeTree()
ORDER BY (market_code, suspend_date);

-- ============================================
-- 9. ETF 数据表
-- ============================================

-- ETF 申购赎回清单
CREATE TABLE IF NOT EXISTS starlight.etf_pcf (
    etf_code String,
    trade_date Date,
    cash_component Nullable(Float64),
    must_cash Nullable(Float64),
    creation_unit Nullable(Int64)
) ENGINE = ReplacingMergeTree()
ORDER BY (etf_code, trade_date);

-- 基金份额
CREATE TABLE IF NOT EXISTS starlight.fund_share (
    fund_code String,
    trade_date Date,
    total_share Nullable(Float64),
    change_share Nullable(Float64)
) ENGINE = ReplacingMergeTree()
ORDER BY (fund_code, trade_date);

-- 基金 IOPV
CREATE TABLE IF NOT EXISTS starlight.fund_iopv (
    fund_code String,
    trade_time DateTime,
    iopv Nullable(Float64)
) ENGINE = MergeTree()
ORDER BY (fund_code, trade_time);

-- ============================================
-- 10. 期权数据表
-- ============================================

-- 期权基础信息
CREATE TABLE IF NOT EXISTS starlight.option_basic_info (
    option_code String,
    option_name String,
    underlying_code String,
    option_type String,
    exercise_type String,
    strike_price Nullable(Float64),
    contract_unit Nullable(Float64),
    listing_date Nullable(Date),
    expiry_date Nullable(Date),
    exercise_date Nullable(Date)
) ENGINE = ReplacingMergeTree()
ORDER BY option_code;

-- 期权标准合约规格
CREATE TABLE IF NOT EXISTS starlight.option_std_ctr_specs (
    option_code String,
    contract_id String,
    underlying_code String,
    strike_price Nullable(Float64),
    contract_multiplier Nullable(Float64),
    tick_size Nullable(Float64)
) ENGINE = ReplacingMergeTree()
ORDER BY option_code;

-- 期权月度合约规格
CREATE TABLE IF NOT EXISTS starlight.option_mon_ctr_specs (
    option_code String,
    contract_month String,
    underlying_code String,
    strike_price Nullable(Float64),
    open_interest Nullable(Float64)
) ENGINE = ReplacingMergeTree()
ORDER BY (option_code, contract_month);

-- ============================================
-- 11. 国债收益率表
-- ============================================

-- 国债收益率
CREATE TABLE IF NOT EXISTS starlight.treasury_yield (
    trade_date Date,
    term String,
    yield Nullable(Float64)
) ENGINE = ReplacingMergeTree()
ORDER BY (trade_date, term);

-- ============================================
-- 12. 系统管理表
-- ============================================

-- 数据获取记录表
CREATE TABLE IF NOT EXISTS starlight.fetch_records (
    id UInt64,
    data_type String,
    fetch_time DateTime DEFAULT now(),
    success UInt8 DEFAULT 0,
    record_count UInt32 DEFAULT 0,
    error_message Nullable(String),
    retry_count UInt32 DEFAULT 0,
    start_date Nullable(String),
    end_date Nullable(String)
) ENGINE = MergeTree()
ORDER BY (data_type, fetch_time);

-- 同步状态表
CREATE TABLE IF NOT EXISTS starlight.sync_status (
    data_type String,
    last_sync_time Nullable(DateTime),
    last_success_time Nullable(DateTime),
    record_count UInt32 DEFAULT 0,
    status String DEFAULT 'pending',
    error_message Nullable(String),
    created_at DateTime DEFAULT now(),
    updated_at DateTime DEFAULT now()
) ENGINE = ReplacingMergeTree(updated_at)
ORDER BY data_type;

-- 每日数据汇总表
CREATE TABLE IF NOT EXISTS starlight.daily_summary (
    date String,
    data_type String,
    success_count UInt32 DEFAULT 0,
    failed_count UInt32 DEFAULT 0,
    total_records UInt32 DEFAULT 0
) ENGINE = ReplacingMergeTree()
ORDER BY (date, data_type);
```
