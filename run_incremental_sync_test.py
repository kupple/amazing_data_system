#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""增量同步测试入口.

示例：
`python run_incremental_sync_test.py code_info`
`python run_incremental_sync_test.py stock_basic --codes 000001.SZ,600000.SH`
`python run_incremental_sync_test.py history_stock_status --codes 000001.SZ --begin-date 20240101 --end-date 20240131`
"""

from __future__ import annotations

import argparse
import logging
from typing import Sequence

from amazingdata_constants import Market, SecurityType
from amazingdata_sdk_provider import AmazingDataSDKConfig, AmazingDataSDKProvider
from base_data import BaseData
from clickhouse_client import ClickHouseConfig
from info_data import InfoData


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="AmazingData 增量同步测试")
    parser.add_argument(
        "target",
        choices=[
            "calendar",
            "code_info",
            "code_list",
            "hist_code_list",
            "stock_basic",
            "history_stock_status",
            "adj_factor",
            "backward_factor",
        ],
    )
    parser.add_argument("--env-file", default=".env", help="dotenv 文件路径")
    parser.add_argument("--market", default=Market.SH)
    parser.add_argument("--security-type", default=SecurityType.EXTRA_STOCK_A)
    parser.add_argument("--codes", default="", help="逗号分隔的证券代码列表")
    parser.add_argument("--limit", type=int, default=5, help="未显式传 codes 时自动取样的数量")
    parser.add_argument("--begin-date", type=int)
    parser.add_argument("--end-date", type=int)
    parser.add_argument("--data-type", default="str", choices=["str", "datetime"])
    parser.add_argument("--local-path", default="", help="AmazingData 本地缓存路径")
    parser.add_argument("--insert-batch-size", type=int, default=5000)
    parser.add_argument("--log-level", default="INFO")
    parser.add_argument("--head", type=int, default=10)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    logging.basicConfig(
        level=getattr(logging, args.log_level.upper(), logging.INFO),
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )

    sdk_config = AmazingDataSDKConfig.from_env(env_file=args.env_file, local_path=args.local_path or None)
    clickhouse_config = ClickHouseConfig.from_env()
    provider = AmazingDataSDKProvider(sdk_config)

    base_data = None
    info_data = None
    try:
        if args.target in {"calendar", "code_info", "code_list", "hist_code_list", "adj_factor", "backward_factor"}:
            base_data = BaseData.from_clickhouse_config(
                clickhouse_config,
                sync_provider=provider,
                insert_batch_size=args.insert_batch_size,
            )

        if args.target in {"stock_basic", "history_stock_status"}:
            info_data = InfoData.from_clickhouse_config(
                clickhouse_config,
                sync_provider=provider,
                insert_batch_size=args.insert_batch_size,
            )

        codes = parse_codes(args.codes)
        if args.target in {"stock_basic", "history_stock_status", "adj_factor", "backward_factor"} and not codes:
            if base_data is None:
                base_data = BaseData.from_clickhouse_config(
                    clickhouse_config,
                    sync_provider=provider,
                    insert_batch_size=args.insert_batch_size,
                )
            codes = base_data.get_code_list(security_type=args.security_type)[: max(1, args.limit)]

        if args.target == "calendar":
            result = base_data.get_calendar(data_type=args.data_type, market=args.market)
            print_summary(result, head=args.head)
            return 0

        if args.target == "code_info":
            result = base_data.get_code_info(security_type=args.security_type)
            print_summary(result, head=args.head)
            return 0

        if args.target == "code_list":
            result = base_data.get_code_list(security_type=args.security_type)
            print_summary(result, head=args.head)
            return 0

        if args.target == "hist_code_list":
            if args.begin_date is None or args.end_date is None:
                raise ValueError("hist_code_list 测试需要 --begin-date 和 --end-date")
            result = base_data.get_hist_code_list(
                security_type=args.security_type,
                start_date=args.begin_date,
                end_date=args.end_date,
                local_path=sdk_config.local_path,
            )
            print_summary(result, head=args.head)
            return 0

        if args.target == "stock_basic":
            result = info_data.get_stock_basic(codes)
            print_summary(result, head=args.head)
            return 0

        if args.target == "history_stock_status":
            result = info_data.get_history_stock_status(
                code_list=codes,
                local_path=sdk_config.local_path,
                is_local=False,
                begin_date=args.begin_date,
                end_date=args.end_date,
            )
            print_summary(result, head=args.head)
            return 0

        if args.target == "adj_factor":
            result = base_data.get_adj_factor(codes, local_path=sdk_config.local_path, is_local=False)
            print_summary(result, head=args.head)
            return 0

        if args.target == "backward_factor":
            result = base_data.get_backward_factor(codes, local_path=sdk_config.local_path, is_local=False)
            print_summary(result, head=args.head)
            return 0

        raise ValueError(f"未知 target: {args.target}")
    finally:
        if info_data is not None:
            try:
                info_data.close()
            except Exception:
                pass
        if base_data is not None:
            try:
                base_data.close()
            except Exception:
                pass
        provider.close()


def parse_codes(raw: str) -> list[str]:
    text = str(raw).strip()
    if not text:
        return []
    return [item.strip() for item in text.split(",") if item.strip()]


def print_summary(result, head: int) -> None:
    if hasattr(result, "head") and hasattr(result, "shape"):
        print(f"shape={result.shape}")
        print(result.head(head))
        return
    if isinstance(result, list):
        print(f"len={len(result)}")
        print(result[:head])
        return
    print(result)


if __name__ == "__main__":
    raise SystemExit(main())
