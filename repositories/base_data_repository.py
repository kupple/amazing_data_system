#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""BaseData 对应的 ClickHouse 读写层."""

from __future__ import annotations

import logging
from dataclasses import asdict
from datetime import date
from typing import Iterable, Sequence

try:
    import pandas as pd
except Exception:  # pragma: no cover
    pd = None  # type: ignore

from amazingdata_constants import SyncStatus
from clickhouse_client import ClickHouseConnection
from clickhouse_tables import (
    AD_CODE_INFO_DAILY_TABLE,
    AD_HIST_CODE_DAILY_TABLE,
    AD_PRICE_FACTOR_TABLE,
    AD_SYNC_TASK_LOG_TABLE,
    AD_TRADE_CALENDAR_TABLE,
    iter_base_data_table_ddls,
)
from data_models import (
    CalendarQuery,
    CodeInfoQuery,
    CodeInfoRow,
    HistCodeDailyRow,
    HistCodeQuery,
    PriceFactorQuery,
    PriceFactorRow,
    SyncTaskLogRow,
    TradeCalendarRow,
    to_ch_date,
)


logger = logging.getLogger(__name__)


class BaseDataRepository:
    """BaseData 的 ClickHouse repository.

    设计原则：
    - repository 只负责 SQL 与数据落库
    - 不负责参数校验和自动补数策略
    - 返回给 SDK 层时，尽量已经接近最终可消费结构
    """

    TRADE_CALENDAR_COLUMNS = (
        "market",
        "trade_date",
        "source",
        "synced_at",
        "created_at",
        "updated_at",
    )
    CODE_INFO_COLUMNS = (
        "snapshot_date",
        "security_type",
        "code",
        "symbol",
        "security_status_raw",
        "pre_close",
        "high_limited",
        "low_limited",
        "price_tick",
        "source",
        "synced_at",
        "created_at",
        "updated_at",
    )
    HIST_CODE_DAILY_COLUMNS = (
        "trade_date",
        "security_type",
        "code",
        "source",
        "synced_at",
        "created_at",
        "updated_at",
    )
    PRICE_FACTOR_COLUMNS = (
        "factor_type",
        "trade_date",
        "code",
        "factor_value",
        "source",
        "synced_at",
        "created_at",
        "updated_at",
    )
    SYNC_TASK_LOG_COLUMNS = (
        "task_name",
        "scope_key",
        "run_date",
        "status",
        "target_table",
        "start_date",
        "end_date",
        "row_count",
        "message",
        "started_at",
        "finished_at",
        "created_at",
        "updated_at",
    )

    def __init__(self, client: ClickHouseConnection, insert_batch_size: int = 5000) -> None:
        self.client = client
        self.insert_batch_size = max(1, int(insert_batch_size))

    def ensure_tables(self) -> None:
        for ddl in iter_base_data_table_ddls():
            self.client.command(ddl)

    def save_calendar_rows(self, rows: Iterable[TradeCalendarRow]) -> int:
        return self._insert_dataclass_rows_in_batches(
            table=AD_TRADE_CALENDAR_TABLE,
            columns=self.TRADE_CALENDAR_COLUMNS,
            rows=rows,
            partition_field="trade_date",
        )

    def save_code_info_rows(self, rows: Iterable[CodeInfoRow]) -> int:
        return self._insert_dataclass_rows_in_batches(
            table=AD_CODE_INFO_DAILY_TABLE,
            columns=self.CODE_INFO_COLUMNS,
            rows=rows,
            partition_field="snapshot_date",
        )

    def save_hist_code_daily_rows(self, rows: Iterable[HistCodeDailyRow]) -> int:
        return self._insert_dataclass_rows_in_batches(
            table=AD_HIST_CODE_DAILY_TABLE,
            columns=self.HIST_CODE_DAILY_COLUMNS,
            rows=rows,
            partition_field="trade_date",
        )

    def save_price_factor_rows(self, rows: Iterable[PriceFactorRow]) -> int:
        return self._insert_dataclass_rows_in_batches(
            table=AD_PRICE_FACTOR_TABLE,
            columns=self.PRICE_FACTOR_COLUMNS,
            rows=rows,
            partition_field="trade_date",
        )

    def insert_sync_log(self, row: SyncTaskLogRow) -> None:
        self._insert_dataclass_rows_in_batches(
            table=AD_SYNC_TASK_LOG_TABLE,
            columns=self.SYNC_TASK_LOG_COLUMNS,
            rows=[row],
            partition_field="run_date",
        )

    def load_calendar_dates(self, query: CalendarQuery) -> list[date]:
        sql = f"""
        SELECT trade_date
        FROM {AD_TRADE_CALENDAR_TABLE}
        WHERE market = {{market:String}}
        GROUP BY trade_date
        ORDER BY trade_date
        """
        rows = self.client.query_rows(sql, {"market": query.market})
        return [row[0] for row in rows if row and row[0] is not None]

    def load_latest_calendar_date(self, market: str):
        sql = f"""
        SELECT max(trade_date)
        FROM {AD_TRADE_CALENDAR_TABLE}
        WHERE market = {{market:String}}
        """
        return self.client.query_value(sql, {"market": market})

    def load_latest_code_info_snapshot_date(self, security_type: str):
        sql = f"""
        SELECT max(snapshot_date)
        FROM {AD_CODE_INFO_DAILY_TABLE}
        WHERE security_type = {{security_type:String}}
        """
        return self.client.query_value(sql, {"security_type": security_type})

    def load_latest_hist_code_trade_date(self, security_type: str):
        sql = f"""
        SELECT max(trade_date)
        FROM {AD_HIST_CODE_DAILY_TABLE}
        WHERE security_type = {{security_type:String}}
        """
        return self.client.query_value(sql, {"security_type": security_type})

    def load_latest_price_factor_trade_date(self, factor_type: str, code_list: Sequence[str]):
        if not code_list:
            return None

        sql = f"""
        SELECT code, max(trade_date) AS latest_trade_date
        FROM {AD_PRICE_FACTOR_TABLE}
        WHERE factor_type = {{factor_type:String}}
          AND code IN {{code_list:Array(String)}}
        GROUP BY code
        ORDER BY code
        """
        rows = self.client.query_rows(
            sql,
            {
                "factor_type": factor_type,
                "code_list": list(code_list),
            },
        )
        if len(rows) != len(code_list):
            # 只要有任一 code 完全缺失，就从头同步这批 code。
            return None
        latest_dates = [row[1] for row in rows if len(row) > 1 and row[1] is not None]
        if not latest_dates:
            return None
        return min(latest_dates)

    def has_successful_sync_today(self, task_name: str, scope_key: str, run_date: date) -> bool:
        sql = f"""
        SELECT count()
        FROM {AD_SYNC_TASK_LOG_TABLE}
        WHERE task_name = {{task_name:String}}
          AND scope_key = {{scope_key:String}}
          AND run_date = {{run_date:Date}}
          AND status = {{status:String}}
        """
        value = self.client.query_value(
            sql,
            {
                "task_name": task_name,
                "scope_key": scope_key,
                "run_date": run_date,
                "status": SyncStatus.SUCCESS,
            },
        )
        return bool(value and int(value) > 0)

    def load_code_info_frame(self, query: CodeInfoQuery):
        if pd is None:  # pragma: no cover
            raise RuntimeError("未安装 pandas，无法返回 DataFrame。")

        snapshot_date = query.snapshot_date or self.load_latest_code_info_snapshot_date(query.security_type)
        if snapshot_date is None:
            return pd.DataFrame(
                columns=[
                    "symbol",
                    "security_status",
                    "pre_close",
                    "high_limited",
                    "low_limited",
                    "price_tick",
                ]
            )

        sql = f"""
        SELECT
            code,
            argMax(symbol, updated_at) AS symbol,
            argMax(security_status_raw, updated_at) AS security_status,
            argMax(pre_close, updated_at) AS pre_close,
            argMax(high_limited, updated_at) AS high_limited,
            argMax(low_limited, updated_at) AS low_limited,
            argMax(price_tick, updated_at) AS price_tick
        FROM {AD_CODE_INFO_DAILY_TABLE}
        WHERE security_type = {{security_type:String}}
          AND snapshot_date = {{snapshot_date:Date}}
        GROUP BY code
        ORDER BY code
        """
        frame = self.client.query_df(
            sql,
            {"security_type": query.security_type, "snapshot_date": snapshot_date},
        )
        if frame.empty:
            return frame
        frame = frame.set_index("code")
        frame.index.name = None
        return frame

    def load_hist_code_list(self, query: HistCodeQuery) -> list[str]:
        sql = f"""
        SELECT code
        FROM {AD_HIST_CODE_DAILY_TABLE}
        WHERE security_type = {{security_type:String}}
          AND trade_date >= {{start_date:Date}}
          AND trade_date <= {{end_date:Date}}
        GROUP BY code
        ORDER BY code
        """
        rows = self.client.query_rows(
            sql,
            {
                "security_type": query.security_type,
                "start_date": query.start_date,
                "end_date": query.end_date,
            },
        )
        return [str(row[0]) for row in rows if row and row[0] is not None]

    def load_price_factor_frame(self, query: PriceFactorQuery):
        if pd is None:  # pragma: no cover
            raise RuntimeError("未安装 pandas，无法返回 DataFrame。")

        if not query.code_list:
            return pd.DataFrame(columns=["trade_date", "code", "factor_value"])

        sql = f"""
        SELECT
            trade_date,
            code,
            argMax(factor_value, updated_at) AS factor_value
        FROM {AD_PRICE_FACTOR_TABLE}
        WHERE factor_type = {{factor_type:String}}
          AND code IN {{code_list:Array(String)}}
        GROUP BY trade_date, code
        ORDER BY trade_date, code
        """
        return self.client.query_df(
            sql,
            {
                "factor_type": query.factor_type,
                "code_list": list(query.code_list),
            },
        )

    def _insert_dataclass_rows_in_batches(
        self,
        table: str,
        columns: Sequence[str],
        rows: Iterable[object],
        partition_field: str | None = None,
    ) -> int:
        total = 0
        if partition_field is None:
            batch: list[tuple] = []

            for row in rows:
                record = asdict(row)
                batch.append(tuple(record[column] for column in columns))
                if len(batch) >= self.insert_batch_size:
                    self.client.insert_rows(table, columns, batch)
                    total += len(batch)
                    logger.info("Inserted %s rows into %s", len(batch), table)
                    batch = []

            if batch:
                self.client.insert_rows(table, columns, batch)
                total += len(batch)
                logger.info("Inserted %s rows into %s", len(batch), table)

            return total

        partition_batches: dict[str, list[tuple]] = {}
        for row in rows:
            record = asdict(row)
            partition_key = self._get_partition_key(record.get(partition_field))
            batch = partition_batches.setdefault(partition_key, [])
            batch.append(tuple(record[column] for column in columns))
            if len(batch) >= self.insert_batch_size:
                self.client.insert_rows(table, columns, batch)
                total += len(batch)
                logger.info("Inserted %s rows into %s partition=%s", len(batch), table, partition_key)
                partition_batches[partition_key] = []

        for partition_key, batch in partition_batches.items():
            if not batch:
                continue
            self.client.insert_rows(table, columns, batch)
            total += len(batch)
            logger.info("Inserted %s rows into %s partition=%s", len(batch), table, partition_key)

        return total

    @staticmethod
    def _get_partition_key(value) -> str:
        if value is None:
            return "unknown"
        dt = to_ch_date(value)
        return dt.strftime("%Y%m")


__all__ = ["BaseDataRepository"]
