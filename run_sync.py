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
from base_data import BaseData, BaseDataCacheMissError
from clickhouse_client import ClickHouseConfig
from market_data import MarketData


logger = logging.getLogger(__name__)
DEFAULT_FULL_SYNC_BEGIN_DATE = 20100101
DEFAULT_SYNC_SECURITY_TYPE = SecurityType.EXTRA_STOCK_A


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="AmazingData 正式同步入口")
    parser.add_argument("task", choices=["daily_kline", "daily_snapshot"])
    parser.add_argument("--env-file", default=".env", help=argparse.SUPPRESS)
    parser.add_argument(
        "--security-type",
        default=DEFAULT_SYNC_SECURITY_TYPE,
        help="默认使用 EXTRA_STOCK_A，同步 A 股股票",
    )
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
        code_groups = resolve_code_groups(
            task=args.task,
            base_data=base_data,
            raw_security_type=args.security_type,
            raw_codes=args.codes,
            limit=args.limit,
        )

        logger.info(
            "sync task=%s group_count=%s total_code_count=%s security_type=%s begin_date=%s end_date=%s batch_size=%s",
            args.task,
            len(code_groups),
            sum(len(codes) for _, codes in code_groups),
            args.security_type,
            begin_date,
            end_date,
            args.batch_size,
        )

        if args.task == "daily_kline":
            return run_daily_kline(
                market_data=market_data,
                code_groups=code_groups,
                begin_date=begin_date,
                end_date=end_date,
                batch_size=args.batch_size,
                force=args.force,
            )

        if args.task == "daily_snapshot":
            return run_daily_snapshot(
                market_data=market_data,
                code_groups=code_groups,
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
    code_groups: Sequence[tuple[str, list[str]]],
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
    logger.info(
        "daily_kline start total_groups=%s total_codes=%s begin_date=%s end_date=%s",
        len(code_groups),
        sum(len(codes) for _, codes in code_groups),
        begin_date,
        end_date,
    )
    for group_index, (security_type, code_list) in enumerate(code_groups, start=1):
        batches = list(iter_batches(code_list, batch_size))
        total_batches = len(batches)
        logger.info(
            "daily_kline group=%s/%s security_type=%s total_codes=%s total_batches=%s",
            group_index,
            len(code_groups),
            security_type,
            len(code_list),
            total_batches,
        )
        for batch_index, batch_codes in enumerate(batches, start=1):
            logger.info(
                "daily_kline group=%s/%s batch=%s/%s security_type=%s code_count=%s period=%s",
                group_index,
                len(code_groups),
                batch_index,
                total_batches,
                security_type,
                len(batch_codes),
                PeriodName.DAY,
            )
            logger.info(
                "daily_kline security_type=%s batch=%s calling sync_kline begin_date=%s end_date=%s",
                security_type,
                batch_index,
                begin_date,
                end_date,
            )
            inserted = market_data.sync_kline(
                code_list=batch_codes,
                begin_date=begin_date,
                end_date=end_date,
                period=PeriodName.DAY,
                force=force,
            )
            logger.info(
                "daily_kline security_type=%s batch=%s sync_kline returned inserted_rows=%s",
                security_type,
                batch_index,
                inserted,
            )
            total_inserted += int(inserted)

    logger.info("daily_kline finished total_inserted=%s", total_inserted)
    return 0


def run_daily_snapshot(
    market_data: MarketData,
    code_groups: Sequence[tuple[str, list[str]]],
    begin_date: int,
    end_date: int,
    batch_size: int,
    force: bool,
) -> int:
    """按批同步历史快照."""

    total_inserted = 0
    logger.info(
        "daily_snapshot start total_groups=%s total_codes=%s begin_date=%s end_date=%s",
        len(code_groups),
        sum(len(codes) for _, codes in code_groups),
        begin_date,
        end_date,
    )
    for group_index, (security_type, code_list) in enumerate(code_groups, start=1):
        batches = list(iter_batches(code_list, batch_size))
        total_batches = len(batches)
        logger.info(
            "daily_snapshot group=%s/%s security_type=%s total_codes=%s total_batches=%s",
            group_index,
            len(code_groups),
            security_type,
            len(code_list),
            total_batches,
        )
        for batch_index, batch_codes in enumerate(batches, start=1):
            logger.info(
                "daily_snapshot group=%s/%s batch=%s/%s security_type=%s code_count=%s",
                group_index,
                len(code_groups),
                batch_index,
                total_batches,
                security_type,
                len(batch_codes),
            )
            logger.info(
                "daily_snapshot security_type=%s batch=%s calling sync_snapshot begin_date=%s end_date=%s",
                security_type,
                batch_index,
                begin_date,
                end_date,
            )
            inserted = market_data.sync_snapshot(
                code_list=batch_codes,
                begin_date=begin_date,
                end_date=end_date,
                force=force,
            )
            logger.info(
                "daily_snapshot security_type=%s batch=%s sync_snapshot returned inserted_rows=%s",
                security_type,
                batch_index,
                inserted,
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


def resolve_code_groups(
    task: str,
    base_data: BaseData,
    raw_security_type: str,
    raw_codes: str,
    limit: int,
) -> list[tuple[str, list[str]]]:
    codes = parse_codes(raw_codes)
    if not codes:
        security_types = resolve_security_types(task=task, raw_security_type=raw_security_type)
        groups: list[tuple[str, list[str]]] = []
        skipped_security_types: list[str] = []
        for security_type in security_types:
            try:
                part_codes = base_data.get_security_universe(security_type=security_type, force=False)
            except BaseDataCacheMissError as exc:
                logger.warning(
                    "security_type=%s 未获取到证券代码，跳过该品种。error=%s",
                    security_type,
                    exc,
                )
                skipped_security_types.append(security_type)
                continue
            logger.info(
                "code_list source=base_data.get_security_universe security_type=%s raw_count=%s",
                security_type,
                len(part_codes),
            )
            part_codes = sorted(dict.fromkeys(part_codes))
            if limit and limit > 0:
                part_codes = part_codes[:limit]
            if not part_codes:
                skipped_security_types.append(security_type)
                continue
            logger.info(
                "resolved code_list security_type=%s count=%s",
                security_type,
                len(part_codes),
            )
            groups.append((security_type, part_codes))
        logger.info(
            "code_list grouped security_types=%s group_count=%s skipped_security_types=%s",
            security_types,
            len(groups),
            skipped_security_types,
        )
        if not groups:
            raise RuntimeError("未获取到可同步的证券代码。")
        return groups

    codes = sorted(dict.fromkeys(codes))
    if limit and limit > 0:
        codes = codes[:limit]
    if not codes:
        raise RuntimeError("未获取到可同步的证券代码。")
    logger.info("code_list source=user_input raw_count=%s", len(codes))
    return [("user_input", codes)]


def resolve_security_types(task: str, raw_security_type: str) -> list[str]:
    text = str(raw_security_type).strip()
    if text:
        return sorted(dict.fromkeys(item.strip() for item in text.split(",") if item.strip()))
    return [DEFAULT_SYNC_SECURITY_TYPE]


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
