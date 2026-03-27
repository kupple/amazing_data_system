#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""正式同步入口.

第一版先提供两个面向生产运行的同步任务：
- `daily_kline`: 日线 K 线同步
- `daily_snapshot`: 历史快照同步

设计目标：
- 不做测试回显，专门做“批量同步”
- 默认先刷新代码池，再按批次同步
- 充分复用各模块已有的增量、跳过、日志能力
"""

from __future__ import annotations

import argparse
import logging
from typing import Iterable, Sequence

from amazingdata_constants import Market, PeriodName, SecurityType
from amazingdata_sdk_provider import AmazingDataSDKConfig, AmazingDataSDKProvider
from base_data import BaseData
from clickhouse_client import ClickHouseConfig
from market_data import MarketData


logger = logging.getLogger(__name__)
DEFAULT_FULL_SYNC_BEGIN_DATE = 20100101


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="AmazingData 正式同步入口")
    parser.add_argument("task", choices=["daily_kline", "daily_snapshot"])
    parser.add_argument("--env-file", default=".env", help=argparse.SUPPRESS)
    parser.add_argument("--security-type", default=SecurityType.EXTRA_STOCK_A_SH_SZ)
    parser.add_argument("--codes", default="", help="逗号分隔的证券代码列表；不传则自动从代码池获取")
    parser.add_argument("--begin-date", type=int, help="开始日期 YYYYMMDD；默认 20100101")
    parser.add_argument("--end-date", type=int, help="结束日期 YYYYMMDD；默认最新交易日")
    parser.add_argument("--batch-size", type=int, default=300, help="每批同步的证券数量")
    parser.add_argument("--limit", type=int, default=0, help="调试时限制同步证券数量，0 表示不限制")
    parser.add_argument("--force", action="store_true", help="忽略当天成功跳过逻辑，强制同步")
    parser.add_argument("--log-level", default="INFO")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    logging.basicConfig(
        level=getattr(logging, args.log_level.upper(), logging.INFO),
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )

    sdk_config = AmazingDataSDKConfig.from_env(env_file=args.env_file)
    clickhouse_config = ClickHouseConfig.from_env()
    provider = AmazingDataSDKProvider(sdk_config)

    base_data = None
    market_data = None
    try:
        base_data = BaseData.from_clickhouse_config(clickhouse_config, sync_provider=provider)
        market_data = MarketData.from_clickhouse_config(clickhouse_config, sync_provider=provider)

        begin_date, end_date = resolve_date_window(
            provider=provider,
            begin_date=args.begin_date,
            end_date=args.end_date,
        )
        code_list = resolve_code_list(
            base_data=base_data,
            security_type=args.security_type,
            raw_codes=args.codes,
            limit=args.limit,
        )

        logger.info(
            "sync task=%s code_count=%s begin_date=%s end_date=%s batch_size=%s",
            args.task,
            len(code_list),
            begin_date,
            end_date,
            args.batch_size,
        )

        if args.task == "daily_kline":
            return run_daily_kline(
                market_data=market_data,
                code_list=code_list,
                begin_date=begin_date,
                end_date=end_date,
                batch_size=args.batch_size,
                force=args.force,
            )

        if args.task == "daily_snapshot":
            return run_daily_snapshot(
                market_data=market_data,
                code_list=code_list,
                begin_date=begin_date,
                end_date=end_date,
                batch_size=args.batch_size,
                force=args.force,
            )

        raise ValueError(f"未知任务: {args.task}")
    finally:
        if market_data is not None:
            try:
                market_data.close()
            except Exception:
                pass
        if base_data is not None:
            try:
                base_data.close()
            except Exception:
                pass
        provider.close()


def run_daily_kline(
    market_data: MarketData,
    code_list: Sequence[str],
    begin_date: int,
    end_date: int,
    batch_size: int,
    force: bool,
) -> int:
    """按批同步日线 K 线.

    日线同步逻辑：
    1. 先拿最新股票池，避免已退市/新上市代码不同步
    2. 股票代码先排序，保证批次切分稳定，便于中断后按同样批次恢复
    3. 按批次切代码，避免一次请求过大
    4. 每批调用 `MarketData.sync_kline(...)`
    5. `sync_kline` 内部再按 ClickHouse 最新日期做增量同步
    6. 同一天同一批次已成功同步则自动跳过
    7. 如果中断，重跑时会继续基于库里最新日期增量同步
    """

    total_inserted = 0
    for batch_index, batch_codes in enumerate(iter_batches(code_list, batch_size), start=1):
        logger.info(
            "daily_kline batch=%s code_count=%s first_code=%s last_code=%s period=%s",
            batch_index,
            len(batch_codes),
            batch_codes[0],
            batch_codes[-1],
            PeriodName.DAY,
        )
        logger.info("daily_kline batch=%s codes=%s", batch_index, batch_codes)
        inserted = market_data.sync_kline(
            code_list=batch_codes,
            begin_date=begin_date,
            end_date=end_date,
            period=PeriodName.DAY,
            force=force,
        )
        total_inserted += int(inserted)

    logger.info("daily_kline finished total_inserted=%s", total_inserted)
    return 0


def run_daily_snapshot(
    market_data: MarketData,
    code_list: Sequence[str],
    begin_date: int,
    end_date: int,
    batch_size: int,
    force: bool,
) -> int:
    """按批同步历史快照."""

    total_inserted = 0
    for batch_index, batch_codes in enumerate(iter_batches(code_list, batch_size), start=1):
        logger.info(
            "daily_snapshot batch=%s code_count=%s first_code=%s last_code=%s",
            batch_index,
            len(batch_codes),
            batch_codes[0],
            batch_codes[-1],
        )
        logger.info("daily_snapshot batch=%s codes=%s", batch_index, batch_codes)
        inserted = market_data.sync_snapshot(
            code_list=batch_codes,
            begin_date=begin_date,
            end_date=end_date,
            force=force,
        )
        total_inserted += int(inserted)

    logger.info("daily_snapshot finished total_inserted=%s", total_inserted)
    return 0


def resolve_date_window(
    provider: AmazingDataSDKProvider,
    begin_date: int | None,
    end_date: int | None,
) -> tuple[int, int]:
    latest_trade_date = provider.session.get_latest_trade_date(Market.SH)
    latest_trade_date_value = int(latest_trade_date.strftime("%Y%m%d"))
    resolved_begin_date = begin_date or DEFAULT_FULL_SYNC_BEGIN_DATE
    resolved_end_date = end_date or latest_trade_date_value
    if resolved_begin_date > resolved_end_date:
        raise ValueError("begin_date 不能大于 end_date")
    return resolved_begin_date, resolved_end_date


def resolve_code_list(
    base_data: BaseData,
    security_type: str,
    raw_codes: str,
    limit: int,
) -> list[str]:
    codes = parse_codes(raw_codes)
    if not codes:
        # 先刷新一次代码池，再从 ClickHouse 获取最新代码列表。
        base_data.sync_code_info(security_type=security_type, force=False)
        codes = base_data.get_code_list(security_type=security_type)
        logger.info(
            "code_list source=clickhouse_latest_code_pool security_type=%s raw_count=%s",
            security_type,
            len(codes),
        )
    else:
        logger.info("code_list source=user_input raw_count=%s", len(codes))
    codes = sorted(dict.fromkeys(codes))
    if limit and limit > 0:
        codes = codes[:limit]
    if not codes:
        raise RuntimeError("未获取到可同步的证券代码。")
    logger.info("resolved code_list count=%s codes=%s", len(codes), codes)
    return codes


def parse_codes(raw: str) -> list[str]:
    text = str(raw).strip()
    if not text:
        return []
    return [item.strip() for item in text.split(",") if item.strip()]


def iter_batches(items: Sequence[str], batch_size: int) -> Iterable[list[str]]:
    size = max(1, int(batch_size))
    for index in range(0, len(items), size):
        yield list(items[index : index + size])


if __name__ == "__main__":
    raise SystemExit(main())
