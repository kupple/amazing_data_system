#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""InfoData 对应的 ClickHouse 读写层."""

from __future__ import annotations

from datetime import date

try:
    import pandas as pd
except Exception:  # pragma: no cover
    pd = None  # type: ignore

from amazingdata_constants import (
    HISTORY_STOCK_STATUS_FIELD_DB_MAPPING,
    HISTORY_STOCK_STATUS_FIELDS,
    STOCK_BASIC_FIELD_DB_MAPPING,
    STOCK_BASIC_FIELDS,
)
from clickhouse_tables import (
    AD_HISTORY_STOCK_STATUS_DAILY_TABLE,
    AD_STOCK_BASIC_DAILY_TABLE,
    iter_info_data_table_ddls,
)
from data_models import (
    HistoryStockStatusQuery,
    HistoryStockStatusRow,
    StockBasicQuery,
    StockBasicRow,
)
from repositories.base_data_repository import BaseDataRepository


class InfoDataRepository(BaseDataRepository):
    """InfoData 的 repository.

    先复用 BaseDataRepository 已经抽好的通用能力：
    - 批量写入
    - 同步日志
    - 当日成功跳过判断
    """

    STOCK_BASIC_COLUMNS = (
        "snapshot_date",
        "market_code",
        "security_name",
        "comp_name",
        "pinyin",
        "comp_name_eng",
        "list_date",
        "delist_date",
        "listplate_name",
        "comp_sname_eng",
        "is_listed",
        "source",
        "synced_at",
        "created_at",
        "updated_at",
    )
    HISTORY_STOCK_STATUS_COLUMNS = (
        "trade_date",
        "market_code",
        "preclose",
        "high_limited",
        "low_limited",
        "price_high_lmt_rate",
        "price_low_lmt_rate",
        "is_st_sec",
        "is_susp_sec",
        "is_wd_sec",
        "is_xr_sec",
        "source",
        "synced_at",
        "created_at",
        "updated_at",
    )

    def ensure_tables(self) -> None:
        super().ensure_tables()
        for ddl in iter_info_data_table_ddls():
            self.client.command(ddl)

    def save_stock_basic_rows(self, rows) -> int:
        return self._insert_dataclass_rows_in_batches(
            table=AD_STOCK_BASIC_DAILY_TABLE,
            columns=self.STOCK_BASIC_COLUMNS,
            rows=rows,
            partition_field="snapshot_date",
        )

    def save_history_stock_status_rows(self, rows) -> int:
        return self._insert_dataclass_rows_in_batches(
            table=AD_HISTORY_STOCK_STATUS_DAILY_TABLE,
            columns=self.HISTORY_STOCK_STATUS_COLUMNS,
            rows=rows,
            partition_field="trade_date",
        )

    def load_latest_stock_basic_snapshot_date(self, code_list: list[str]):
        if not code_list:
            return None

        sql = f"""
        SELECT market_code, max(snapshot_date) AS latest_snapshot_date
        FROM {AD_STOCK_BASIC_DAILY_TABLE}
        WHERE market_code IN {{code_list:Array(String)}}
        GROUP BY market_code
        ORDER BY market_code
        """
        rows = self.client.query_rows(sql, {"code_list": code_list})
        if len(rows) != len(code_list):
            return None
        latest_dates = [row[1] for row in rows if len(row) > 1 and row[1] is not None]
        if not latest_dates:
            return None
        return min(latest_dates)

    def load_latest_history_stock_status_trade_date(self, code_list: list[str]):
        if not code_list:
            return None

        sql = f"""
        SELECT market_code, max(trade_date) AS latest_trade_date
        FROM {AD_HISTORY_STOCK_STATUS_DAILY_TABLE}
        WHERE market_code IN {{code_list:Array(String)}}
        GROUP BY market_code
        ORDER BY market_code
        """
        rows = self.client.query_rows(sql, {"code_list": code_list})
        if len(rows) != len(code_list):
            return None
        latest_dates = [row[1] for row in rows if len(row) > 1 and row[1] is not None]
        if not latest_dates:
            return None
        return min(latest_dates)

    def load_stock_basic_frame(self, query: StockBasicQuery):
        if pd is None:  # pragma: no cover
            raise RuntimeError("未安装 pandas，无法返回 DataFrame。")

        if not query.code_list:
            return pd.DataFrame(columns=list(STOCK_BASIC_FIELDS))

        snapshot_date = self.load_latest_stock_basic_snapshot_date(list(query.code_list))
        if snapshot_date is None:
            return pd.DataFrame(columns=list(STOCK_BASIC_FIELDS))

        sql = f"""
        SELECT
            market_code,
            argMax(security_name, updated_at) AS security_name,
            argMax(comp_name, updated_at) AS comp_name,
            argMax(pinyin, updated_at) AS pinyin,
            argMax(comp_name_eng, updated_at) AS comp_name_eng,
            argMax(list_date, updated_at) AS list_date,
            argMax(delist_date, updated_at) AS delist_date,
            argMax(listplate_name, updated_at) AS listplate_name,
            argMax(comp_sname_eng, updated_at) AS comp_sname_eng,
            argMax(is_listed, updated_at) AS is_listed
        FROM {AD_STOCK_BASIC_DAILY_TABLE}
        WHERE snapshot_date = {{snapshot_date:Date}}
          AND market_code IN {{code_list:Array(String)}}
        GROUP BY market_code
        ORDER BY market_code
        """
        frame = self.client.query_df(
            sql,
            {
                "snapshot_date": snapshot_date,
                "code_list": list(query.code_list),
            },
        )
        if frame.empty:
            return frame

        rename_map = {db_name: raw_name for raw_name, db_name in STOCK_BASIC_FIELD_DB_MAPPING.items()}
        frame = frame.rename(columns=rename_map)
        frame["_code_order"] = frame["MARKET_CODE"].apply(lambda value: query.code_list.index(value))
        frame = frame.sort_values("_code_order").drop(columns=["_code_order"]).reset_index(drop=True)
        return frame.loc[:, list(STOCK_BASIC_FIELDS)]

    def load_history_stock_status_frame(self, query: HistoryStockStatusQuery):
        if pd is None:  # pragma: no cover
            raise RuntimeError("未安装 pandas，无法返回 DataFrame。")

        if not query.code_list:
            return pd.DataFrame(columns=list(HISTORY_STOCK_STATUS_FIELDS))

        sql = f"""
        SELECT
            market_code,
            trade_date,
            argMax(preclose, updated_at) AS preclose,
            argMax(high_limited, updated_at) AS high_limited,
            argMax(low_limited, updated_at) AS low_limited,
            argMax(price_high_lmt_rate, updated_at) AS price_high_lmt_rate,
            argMax(price_low_lmt_rate, updated_at) AS price_low_lmt_rate,
            argMax(is_st_sec, updated_at) AS is_st_sec,
            argMax(is_susp_sec, updated_at) AS is_susp_sec,
            argMax(is_wd_sec, updated_at) AS is_wd_sec,
            argMax(is_xr_sec, updated_at) AS is_xr_sec
        FROM {AD_HISTORY_STOCK_STATUS_DAILY_TABLE}
        WHERE market_code IN {{code_list:Array(String)}}
        """
        parameters = {
            "code_list": list(query.code_list),
        }
        if query.begin_date is not None:
            sql += "\n  AND trade_date >= {begin_date:Date}"
            parameters["begin_date"] = query.begin_date
        if query.end_date is not None:
            sql += "\n  AND trade_date <= {end_date:Date}"
            parameters["end_date"] = query.end_date
        sql += "\nGROUP BY market_code, trade_date\nORDER BY market_code, trade_date"

        frame = self.client.query_df(sql, parameters)
        if frame.empty:
            return frame

        frame["trade_date"] = pd.to_datetime(frame["trade_date"]).dt.strftime("%Y%m%d")
        rename_map = {db_name: raw_name for raw_name, db_name in HISTORY_STOCK_STATUS_FIELD_DB_MAPPING.items()}
        frame = frame.rename(columns=rename_map)
        frame["_code_order"] = frame["MARKET_CODE"].apply(lambda value: query.code_list.index(value))
        frame = frame.sort_values(["_code_order", "TRADE_DATE"]).drop(columns=["_code_order"]).reset_index(drop=True)
        return frame.loc[:, list(HISTORY_STOCK_STATUS_FIELDS)]


__all__ = ["InfoDataRepository"]
