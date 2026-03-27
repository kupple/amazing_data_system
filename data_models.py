#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""BaseData 使用的数据模型与日期转换工具."""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from datetime import date, datetime
from typing import Optional, Sequence


DEFAULT_SOURCE = "amazingdata"


def utcnow() -> datetime:
    """统一生成无时区的 UTC 时间戳，方便写入 ClickHouse DateTime64."""

    return datetime.utcnow()


def to_ch_date(value: date | datetime | int | str) -> date:
    """把常见日期输入统一转换为 ClickHouse `Date`.

    支持以下输入：
    - `date`
    - `datetime`
    - `20240327` 这种 8 位整数
    - `20240327` / `2024-03-27` 这种字符串
    """

    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    if isinstance(value, int):
        s = f"{value:08d}"
        return date(int(s[0:4]), int(s[4:6]), int(s[6:8]))
    if isinstance(value, str):
        cleaned = re.sub(r"[^0-9]", "", value.strip())
        if len(cleaned) != 8:
            raise ValueError(f"无法识别日期字符串: {value!r}")
        return date(int(cleaned[0:4]), int(cleaned[4:6]), int(cleaned[6:8]))
    raise TypeError(f"不支持的日期类型: {type(value)!r}")


def to_yyyymmdd(value: date | datetime | int | str) -> int:
    """统一转换回 `YYYYMMDD` 整数表示."""

    return int(to_ch_date(value).strftime("%Y%m%d"))


def normalize_code_list(code_list: Sequence[str]) -> list[str]:
    """清洗并去重证券代码，保留原始顺序."""

    seen: set[str] = set()
    normalized: list[str] = []
    for code in code_list:
        text = str(code).strip()
        if not text or text in seen:
            continue
        seen.add(text)
        normalized.append(text)
    return normalized


@dataclass(frozen=True)
class CalendarQuery:
    """交易日历查询参数."""

    market: str
    data_type: str = "str"


@dataclass(frozen=True)
class CodeInfoQuery:
    """证券基础信息查询参数."""

    security_type: str
    snapshot_date: Optional[date] = None


@dataclass(frozen=True)
class HistCodeQuery:
    """历史代码表查询参数."""

    security_type: str
    start_date: date
    end_date: date
    local_path: str


@dataclass(frozen=True)
class PriceFactorQuery:
    """复权因子查询参数."""

    factor_type: str
    code_list: tuple[str, ...]
    local_path: str
    is_local: bool = True


@dataclass(frozen=True)
class StockBasicQuery:
    """证券基础信息查询参数."""

    code_list: tuple[str, ...]


@dataclass(frozen=True)
class HistoryStockStatusQuery:
    """历史证券状态查询参数."""

    code_list: tuple[str, ...]
    local_path: str
    is_local: bool = True
    begin_date: Optional[date] = None
    end_date: Optional[date] = None


@dataclass(frozen=True)
class MarketKlineQuery:
    """K 线查询参数."""

    code_list: tuple[str, ...]
    begin_date: date
    end_date: date
    period: str
    begin_time: Optional[int] = None
    end_time: Optional[int] = None


@dataclass(frozen=True)
class MarketSnapshotQuery:
    """历史快照查询参数."""

    code_list: tuple[str, ...]
    begin_date: date
    end_date: date
    begin_time: Optional[int] = None
    end_time: Optional[int] = None


@dataclass(frozen=True)
class TradeCalendarRow:
    """交易日历落库行."""

    market: str
    trade_date: date
    source: str = DEFAULT_SOURCE
    synced_at: datetime = field(default_factory=utcnow)
    created_at: datetime = field(default_factory=utcnow)
    updated_at: datetime = field(default_factory=utcnow)


@dataclass(frozen=True)
class CodeInfoRow:
    """证券基础信息落库行."""

    snapshot_date: date
    security_type: str
    code: str
    symbol: Optional[str] = None
    security_status_raw: Optional[str] = None
    pre_close: Optional[float] = None
    high_limited: Optional[float] = None
    low_limited: Optional[float] = None
    price_tick: Optional[float] = None
    source: str = DEFAULT_SOURCE
    synced_at: datetime = field(default_factory=utcnow)
    created_at: datetime = field(default_factory=utcnow)
    updated_at: datetime = field(default_factory=utcnow)


@dataclass(frozen=True)
class HistCodeDailyRow:
    """历史代码表按日成员行.

    这里使用日级拍平结构，而不是“批次 + 明细”模式，
    是因为在 ClickHouse 中按交易日和代码组合查询会更直接。
    """

    trade_date: date
    security_type: str
    code: str
    source: str = DEFAULT_SOURCE
    synced_at: datetime = field(default_factory=utcnow)
    created_at: datetime = field(default_factory=utcnow)
    updated_at: datetime = field(default_factory=utcnow)


@dataclass(frozen=True)
class PriceFactorRow:
    """复权因子落库行."""

    factor_type: str
    trade_date: date
    code: str
    factor_value: float
    source: str = DEFAULT_SOURCE
    synced_at: datetime = field(default_factory=utcnow)
    created_at: datetime = field(default_factory=utcnow)
    updated_at: datetime = field(default_factory=utcnow)


@dataclass(frozen=True)
class StockBasicRow:
    """`get_stock_basic` 落库行.

    入库字段统一使用数据库风格的小写 snake_case。
    """

    snapshot_date: date
    market_code: str
    security_name: Optional[str] = None
    comp_name: Optional[str] = None
    pinyin: Optional[str] = None
    comp_name_eng: Optional[str] = None
    list_date: Optional[int] = None
    delist_date: Optional[int] = None
    listplate_name: Optional[str] = None
    comp_sname_eng: Optional[str] = None
    is_listed: Optional[int] = None
    source: str = DEFAULT_SOURCE
    synced_at: datetime = field(default_factory=utcnow)
    created_at: datetime = field(default_factory=utcnow)
    updated_at: datetime = field(default_factory=utcnow)


@dataclass(frozen=True)
class HistoryStockStatusRow:
    """`get_history_stock_status` 落库行."""

    trade_date: date
    market_code: str
    preclose: Optional[float] = None
    high_limited: Optional[float] = None
    low_limited: Optional[float] = None
    price_high_lmt_rate: Optional[float] = None
    price_low_lmt_rate: Optional[float] = None
    is_st_sec: Optional[str] = None
    is_susp_sec: Optional[str] = None
    is_wd_sec: Optional[str] = None
    is_xr_sec: Optional[str] = None
    source: str = DEFAULT_SOURCE
    synced_at: datetime = field(default_factory=utcnow)
    created_at: datetime = field(default_factory=utcnow)
    updated_at: datetime = field(default_factory=utcnow)


@dataclass(frozen=True)
class MarketKlineRow:
    """`query_kline` 落库行."""

    trade_time: datetime
    trade_date: date
    code: str
    period: str
    open: Optional[float] = None
    high: Optional[float] = None
    low: Optional[float] = None
    close: Optional[float] = None
    volume: Optional[float] = None
    amount: Optional[float] = None
    source: str = DEFAULT_SOURCE
    synced_at: datetime = field(default_factory=utcnow)
    created_at: datetime = field(default_factory=utcnow)
    updated_at: datetime = field(default_factory=utcnow)


@dataclass(frozen=True)
class MarketSnapshotRow:
    """`query_snapshot` 落库行.

    这里使用已知快照结构的并集列，并通过 `snapshot_kind` 区分具体结构。
    """

    trade_time: datetime
    trade_date: date
    code: str
    snapshot_kind: str
    pre_close: Optional[float] = None
    last: Optional[float] = None
    open: Optional[float] = None
    high: Optional[float] = None
    low: Optional[float] = None
    close: Optional[float] = None
    volume: Optional[float] = None
    amount: Optional[float] = None
    num_trades: Optional[float] = None
    high_limited: Optional[float] = None
    low_limited: Optional[float] = None
    ask_price1: Optional[float] = None
    ask_price2: Optional[float] = None
    ask_price3: Optional[float] = None
    ask_price4: Optional[float] = None
    ask_price5: Optional[float] = None
    ask_volume1: Optional[int] = None
    ask_volume2: Optional[int] = None
    ask_volume3: Optional[int] = None
    ask_volume4: Optional[int] = None
    ask_volume5: Optional[int] = None
    bid_price1: Optional[float] = None
    bid_price2: Optional[float] = None
    bid_price3: Optional[float] = None
    bid_price4: Optional[float] = None
    bid_price5: Optional[float] = None
    bid_volume1: Optional[int] = None
    bid_volume2: Optional[int] = None
    bid_volume3: Optional[int] = None
    bid_volume4: Optional[int] = None
    bid_volume5: Optional[int] = None
    iopv: Optional[float] = None
    trading_phase_code: Optional[str] = None
    total_long_position: Optional[int] = None
    pre_settle: Optional[float] = None
    auction_price: Optional[float] = None
    auction_volume: Optional[int] = None
    settle: Optional[float] = None
    contract_type: Optional[str] = None
    expire_date: Optional[int] = None
    underlying_security_code: Optional[str] = None
    exercise_price: Optional[float] = None
    action_day: Optional[str] = None
    trading_day: Optional[str] = None
    pre_open_interest: Optional[int] = None
    open_interest: Optional[int] = None
    average_price: Optional[float] = None
    nominal_price: Optional[float] = None
    ref_price: Optional[float] = None
    bid_price_limit_up: Optional[float] = None
    bid_price_limit_down: Optional[float] = None
    offer_price_limit_up: Optional[float] = None
    offer_price_limit_down: Optional[float] = None
    source: str = DEFAULT_SOURCE
    synced_at: datetime = field(default_factory=utcnow)
    created_at: datetime = field(default_factory=utcnow)
    updated_at: datetime = field(default_factory=utcnow)


@dataclass(frozen=True)
class SyncTaskLogRow:
    """同步任务日志行.

    这张表用于记录每次接口级同步的执行结果，支撑两类能力：
    1. 运行日志审计
    2. 当天同步成功后再次执行时的跳过判断
    """

    task_name: str
    scope_key: str
    run_date: date
    status: str
    target_table: str
    start_date: Optional[date] = None
    end_date: Optional[date] = None
    row_count: int = 0
    message: Optional[str] = None
    started_at: datetime = field(default_factory=utcnow)
    finished_at: datetime = field(default_factory=utcnow)
    created_at: datetime = field(default_factory=utcnow)
    updated_at: datetime = field(default_factory=utcnow)


__all__ = [
    "CalendarQuery",
    "CodeInfoQuery",
    "CodeInfoRow",
    "DEFAULT_SOURCE",
    "HistoryStockStatusQuery",
    "HistoryStockStatusRow",
    "MarketKlineQuery",
    "MarketKlineRow",
    "MarketSnapshotQuery",
    "MarketSnapshotRow",
    "HistCodeDailyRow",
    "HistCodeQuery",
    "PriceFactorQuery",
    "PriceFactorRow",
    "StockBasicQuery",
    "StockBasicRow",
    "SyncTaskLogRow",
    "TradeCalendarRow",
    "normalize_code_list",
    "to_ch_date",
    "to_yyyymmdd",
    "utcnow",
]
