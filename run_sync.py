#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""正式同步入口.

当前版本只服务一条主线：
- 只同步 `EXTRA_STOCK_A`
- 不做批量调度，直接逐股顺序同步

正式任务：
- `daily_kline`
- `market_snapshot`
"""

from __future__ import annotations

import argparse
import logging

from amazingdata_constants import PeriodName, SecurityType
from amazingdata_sdk_provider import AmazingDataSDKConfig, AmazingDataSDKProvider
from base_data import BaseData, BaseDataCacheMissError
from clickhouse_client import ClickHouseConfig
from market_data import MarketData


logger = logging.getLogger(__name__)
DEFAULT_FULL_SYNC_BEGIN_DATE = 20100101
DEFAULT_SYNC_SECURITY_TYPE = SecurityType.EXTRA_STOCK_A


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="AmazingData 正式同步入口")
    parser.add_argument("task", choices=["daily_kline", "market_snapshot"])
    parser.add_argument("--env-file", default=".env", help=argparse.SUPPRESS)
    parser.add_argument("--codes", default="", help="逗号分隔的证券代码列表；不传则自动从代码池获取")
    parser.add_argument("--begin-date", type=int, help="开始日期 YYYYMMDD；默认 20100101")
    parser.add_argument("--end-date", type=int, help="结束日期 YYYYMMDD；默认最新交易日")
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
            raw_codes=args.codes,
            limit=args.limit,
        )

        logger.info(
            "sync task=%s total_code_count=%s security_type=%s begin_date=%s end_date=%s mode=per_stock_sequential",
            args.task,
            len(code_list),
            DEFAULT_SYNC_SECURITY_TYPE,
            begin_date,
            end_date,
        )

        if args.task == "daily_kline":
            return run_daily_kline(
                market_data=market_data,
                code_list=code_list,
                begin_date=begin_date,
                end_date=end_date,
                force=args.force,
            )

        if args.task == "market_snapshot":
            return run_market_snapshot(
                market_data=market_data,
                code_list=code_list,
                begin_date=begin_date,
                end_date=end_date,
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
    code_list: list[str],
    begin_date: int,
    end_date: int,
    force: bool,
) -> int:
    """按单只股票顺序同步日线 K 线."""

    total_inserted = 0
    logger.info(
        "daily_kline start total_codes=%s begin_date=%s end_date=%s",
        len(code_list),
        begin_date,
        end_date,
    )
    for index, code in enumerate(code_list, start=1):
        logger.info(
            "daily_kline progress=%s/%s security_type=%s period=%s",
            index,
            len(code_list),
            DEFAULT_SYNC_SECURITY_TYPE,
            PeriodName.DAY,
        )
        inserted = market_data.sync_kline(
            code_list=[code],
            begin_date=begin_date,
            end_date=end_date,
            period=PeriodName.DAY,
            force=force,
        )
        logger.info("daily_kline progress=%s/%s inserted_rows=%s", index, len(code_list), inserted)
        total_inserted += int(inserted)

    logger.info("daily_kline finished total_inserted=%s", total_inserted)
    return 0


def run_market_snapshot(
    market_data: MarketData,
    code_list: list[str],
    begin_date: int,
    end_date: int,
    force: bool,
) -> int:
    """按单只股票顺序同步历史快照."""

    total_inserted = 0
    logger.info(
        "market_snapshot start total_codes=%s begin_date=%s end_date=%s",
        len(code_list),
        begin_date,
        end_date,
    )
    for index, code in enumerate(code_list, start=1):
        logger.info(
            "market_snapshot progress=%s/%s security_type=%s",
            index,
            len(code_list),
            DEFAULT_SYNC_SECURITY_TYPE,
        )
        inserted = market_data.sync_snapshot(
            code_list=[code],
            begin_date=begin_date,
            end_date=end_date,
            force=force,
        )
        logger.info("market_snapshot progress=%s/%s inserted_rows=%s", index, len(code_list), inserted)
        total_inserted += int(inserted)

    logger.info("market_snapshot finished total_inserted=%s", total_inserted)
    return 0


def resolve_date_window(
    provider: AmazingDataSDKProvider,
    begin_date: int | None,
    end_date: int | None,
) -> tuple[int, int]:
    latest_trade_date = provider.session.get_latest_trade_date()
    latest_trade_date_value = int(latest_trade_date.strftime("%Y%m%d"))
    resolved_begin_date = begin_date or DEFAULT_FULL_SYNC_BEGIN_DATE
    resolved_end_date = end_date or latest_trade_date_value
    if resolved_begin_date > resolved_end_date:
        raise ValueError("begin_date 不能大于 end_date")
    return resolved_begin_date, resolved_end_date


def resolve_code_list(
    base_data: BaseData,
    raw_codes: str,
    limit: int,
) -> list[str]:
    codes = parse_codes(raw_codes)
    if not codes:
        try:
            codes = base_data.get_stock_universe(security_type=DEFAULT_SYNC_SECURITY_TYPE, force=False)
        except BaseDataCacheMissError as exc:
            raise RuntimeError(f"未获取到可同步的证券代码: {exc}") from exc
        logger.info(
            "code_list source=base_data.get_stock_universe security_type=%s raw_count=%s",
            DEFAULT_SYNC_SECURITY_TYPE,
            len(codes),
        )
    else:
        logger.info("code_list source=user_input raw_count=%s", len(codes))

    codes = sorted(dict.fromkeys(codes))
    if limit and limit > 0:
        codes = codes[:limit]
    if not codes:
        raise RuntimeError("未获取到可同步的证券代码。")

    logger.info("resolved code_list count=%s", len(codes))
    return codes


def parse_codes(raw: str) -> list[str]:
    text = str(raw).strip()
    if not text:
        return []
    return [item.strip() for item in text.split(",") if item.strip()]


if __name__ == "__main__":
    raise SystemExit(main())
