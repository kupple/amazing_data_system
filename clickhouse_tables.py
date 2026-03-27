#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""AmazingData SDK 第一阶段的 ClickHouse 表结构定义.

这层只关心数仓建模，不关心业务编排：
- 表名
- 字段类型
- MergeTree 策略
- 分区键与排序键

第一阶段先覆盖 `BaseData` 和已明确的 `InfoData` 表结构。

命名约定：
- ClickHouse 表名统一使用小写，前缀固定为 `ad_`
- ClickHouse 列名统一使用小写 `snake_case`
- SDK 原始大写字段名只保留在返回 schema / 字段映射层，不直接进入数据库 DDL
"""

from __future__ import annotations


AD_TRADE_CALENDAR_TABLE = "ad_trade_calendar"
AD_CODE_INFO_TABLE = "ad_code_info"
AD_HIST_CODE_DAILY_TABLE = "ad_hist_code_daily"
AD_PRICE_FACTOR_TABLE = "ad_price_factor"
AD_SYNC_TASK_LOG_TABLE = "ad_sync_task_log"
AD_SYNC_CHECKPOINT_TABLE = "ad_sync_checkpoint"
AD_STOCK_BASIC_TABLE = "ad_stock_basic"
AD_HISTORY_STOCK_STATUS_TABLE = "ad_history_stock_status"
AD_MARKET_KLINE_DAILY_TABLE = "ad_market_kline_daily"
AD_MARKET_SNAPSHOT_TABLE = "ad_market_snapshot"


CREATE_AD_TRADE_CALENDAR_TABLE = f"""
CREATE TABLE IF NOT EXISTS {AD_TRADE_CALENDAR_TABLE}
(
    -- 数据库列统一使用小写 snake_case
    market LowCardinality(String),
    trade_date Date
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(trade_date)
ORDER BY (market, trade_date)
"""


CREATE_AD_CODE_INFO_TABLE = f"""
CREATE TABLE IF NOT EXISTS {AD_CODE_INFO_TABLE}
(
    security_type LowCardinality(String),
    code String,
    symbol Nullable(String),
    security_status_raw Nullable(String),
    pre_close Nullable(Float64),
    high_limited Nullable(Float64),
    low_limited Nullable(Float64),
    price_tick Nullable(Float64)
)
ENGINE = MergeTree
ORDER BY (security_type, code)
"""


CREATE_AD_HIST_CODE_DAILY_TABLE = f"""
CREATE TABLE IF NOT EXISTS {AD_HIST_CODE_DAILY_TABLE}
(
    trade_date Date,
    security_type LowCardinality(String),
    code String
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(trade_date)
ORDER BY (security_type, trade_date, code)
"""


CREATE_AD_PRICE_FACTOR_TABLE = f"""
CREATE TABLE IF NOT EXISTS {AD_PRICE_FACTOR_TABLE}
(
    factor_type LowCardinality(String),
    trade_date Date,
    code String,
    factor_value Float64
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(trade_date)
ORDER BY (factor_type, code, trade_date)
"""


CREATE_AD_SYNC_TASK_LOG_TABLE = f"""
CREATE TABLE IF NOT EXISTS {AD_SYNC_TASK_LOG_TABLE}
(
    task_name LowCardinality(String),
    scope_key String,
    run_date Date,
    status LowCardinality(String),
    target_table LowCardinality(String),
    start_date Nullable(Date),
    end_date Nullable(Date),
    row_count UInt64,
    message Nullable(String),
    started_at DateTime64(3),
    finished_at DateTime64(3)
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(run_date)
ORDER BY (task_name, scope_key, run_date, started_at)
"""


CREATE_AD_SYNC_CHECKPOINT_TABLE = f"""
CREATE TABLE IF NOT EXISTS {AD_SYNC_CHECKPOINT_TABLE}
(
    task_name LowCardinality(String),
    scope_key String,
    run_date Date,
    status LowCardinality(String),
    target_table LowCardinality(String),
    checkpoint_date Nullable(Date),
    row_count UInt64,
    message Nullable(String),
    finished_at DateTime64(3)
)
ENGINE = ReplacingMergeTree(finished_at)
PARTITION BY toYYYYMM(run_date)
ORDER BY (task_name, scope_key, run_date)
"""


CREATE_AD_STOCK_BASIC_TABLE = f"""
CREATE TABLE IF NOT EXISTS {AD_STOCK_BASIC_TABLE}
(
    snapshot_date Date,
    market_code String,
    security_name Nullable(String),
    comp_name Nullable(String),
    pinyin Nullable(String),
    comp_name_eng Nullable(String),
    list_date Nullable(Int32),
    delist_date Nullable(Int32),
    listplate_name Nullable(String),
    comp_sname_eng Nullable(String),
    is_listed Nullable(Int32)
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(snapshot_date)
ORDER BY (snapshot_date, market_code)
"""


CREATE_AD_HISTORY_STOCK_STATUS_TABLE = f"""
CREATE TABLE IF NOT EXISTS {AD_HISTORY_STOCK_STATUS_TABLE}
(
    trade_date Date,
    market_code String,
    preclose Nullable(Float64),
    high_limited Nullable(Float64),
    low_limited Nullable(Float64),
    price_high_lmt_rate Nullable(Float64),
    price_low_lmt_rate Nullable(Float64),
    is_st_sec Nullable(String),
    is_susp_sec Nullable(String),
    is_wd_sec Nullable(String),
    is_xr_sec Nullable(String)
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(trade_date)
ORDER BY (trade_date, market_code)
"""


CREATE_AD_MARKET_KLINE_DAILY_TABLE = f"""
CREATE TABLE IF NOT EXISTS {AD_MARKET_KLINE_DAILY_TABLE}
(
    trade_time DateTime64(3),
    code String,
    period LowCardinality(String),
    open Nullable(Float64),
    high Nullable(Float64),
    low Nullable(Float64),
    close Nullable(Float64),
    volume Nullable(Float64),
    amount Nullable(Float64)
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(toDate(trade_time))
ORDER BY (period, code, trade_time)
"""


CREATE_AD_MARKET_SNAPSHOT_TABLE = f"""
CREATE TABLE IF NOT EXISTS {AD_MARKET_SNAPSHOT_TABLE}
(
    trade_time DateTime64(3),
    code String,
    pre_close Nullable(Float64),
    last Nullable(Float64),
    open Nullable(Float64),
    high Nullable(Float64),
    low Nullable(Float64),
    close Nullable(Float64),
    volume Nullable(Float64),
    amount Nullable(Float64),
    num_trades Nullable(Float64),
    high_limited Nullable(Float64),
    low_limited Nullable(Float64),
    ask_price1 Nullable(Float64),
    ask_price2 Nullable(Float64),
    ask_price3 Nullable(Float64),
    ask_price4 Nullable(Float64),
    ask_price5 Nullable(Float64),
    ask_volume1 Nullable(Int64),
    ask_volume2 Nullable(Int64),
    ask_volume3 Nullable(Int64),
    ask_volume4 Nullable(Int64),
    ask_volume5 Nullable(Int64),
    bid_price1 Nullable(Float64),
    bid_price2 Nullable(Float64),
    bid_price3 Nullable(Float64),
    bid_price4 Nullable(Float64),
    bid_price5 Nullable(Float64),
    bid_volume1 Nullable(Int64),
    bid_volume2 Nullable(Int64),
    bid_volume3 Nullable(Int64),
    bid_volume4 Nullable(Int64),
    bid_volume5 Nullable(Int64),
    iopv Nullable(Float64),
    trading_phase_code Nullable(String),
    total_long_position Nullable(Int64),
    pre_settle Nullable(Float64),
    auction_price Nullable(Float64),
    auction_volume Nullable(Int64),
    settle Nullable(Float64),
    contract_type Nullable(String),
    expire_date Nullable(Int32),
    underlying_security_code Nullable(String),
    exercise_price Nullable(Float64),
    action_day Nullable(String),
    trading_day Nullable(String),
    pre_open_interest Nullable(Int64),
    open_interest Nullable(Int64),
    average_price Nullable(Float64),
    nominal_price Nullable(Float64),
    ref_price Nullable(Float64),
    bid_price_limit_up Nullable(Float64),
    bid_price_limit_down Nullable(Float64),
    offer_price_limit_up Nullable(Float64),
    offer_price_limit_down Nullable(Float64)
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(toDate(trade_time))
ORDER BY (code, trade_time)
"""


BASE_DATA_TABLE_DDLS = (
    CREATE_AD_TRADE_CALENDAR_TABLE,
    CREATE_AD_CODE_INFO_TABLE,
    CREATE_AD_HIST_CODE_DAILY_TABLE,
    CREATE_AD_PRICE_FACTOR_TABLE,
    CREATE_AD_SYNC_TASK_LOG_TABLE,
    CREATE_AD_SYNC_CHECKPOINT_TABLE,
)

INFO_DATA_TABLE_DDLS = (
    CREATE_AD_STOCK_BASIC_TABLE,
    CREATE_AD_HISTORY_STOCK_STATUS_TABLE,
)

MARKET_DATA_TABLE_DDLS = (
    CREATE_AD_MARKET_KLINE_DAILY_TABLE,
    CREATE_AD_MARKET_SNAPSHOT_TABLE,
)


def iter_base_data_table_ddls() -> tuple[str, ...]:
    """按固定顺序返回 BaseData 所需 DDL."""

    return BASE_DATA_TABLE_DDLS


def iter_info_data_table_ddls() -> tuple[str, ...]:
    """按固定顺序返回 InfoData 当前已实现接口所需 DDL."""

    return INFO_DATA_TABLE_DDLS


def iter_market_data_table_ddls() -> tuple[str, ...]:
    """按固定顺序返回 MarketData 当前已实现接口所需 DDL."""

    return MARKET_DATA_TABLE_DDLS


__all__ = [
    "AD_CODE_INFO_TABLE",
    "AD_HISTORY_STOCK_STATUS_TABLE",
    "AD_HIST_CODE_DAILY_TABLE",
    "AD_MARKET_KLINE_DAILY_TABLE",
    "AD_MARKET_SNAPSHOT_TABLE",
    "AD_PRICE_FACTOR_TABLE",
    "AD_SYNC_CHECKPOINT_TABLE",
    "AD_STOCK_BASIC_TABLE",
    "AD_SYNC_TASK_LOG_TABLE",
    "AD_TRADE_CALENDAR_TABLE",
    "BASE_DATA_TABLE_DDLS",
    "INFO_DATA_TABLE_DDLS",
    "MARKET_DATA_TABLE_DDLS",
    "iter_base_data_table_ddls",
    "iter_info_data_table_ddls",
    "iter_market_data_table_ddls",
]
