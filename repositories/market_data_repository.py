#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""MarketData 对应的 ClickHouse 读写层."""

from __future__ import annotations

try:
    import pandas as pd
except Exception:  # pragma: no cover
    pd = None  # type: ignore

from amazingdata_constants import KLINE_FIELDS, SNAPSHOT_FIELDS, SNAPSHOT_KIND_TO_FIELDS, SnapshotKind
from clickhouse_tables import (
    AD_MARKET_KLINE_TABLE,
    AD_MARKET_SNAPSHOT_TABLE,
    iter_market_data_table_ddls,
)
from data_models import MarketKlineQuery, MarketKlineRow, MarketSnapshotQuery, MarketSnapshotRow
from repositories.base_data_repository import BaseDataRepository


class MarketDataRepository(BaseDataRepository):
    """MarketData 的 repository."""

    MARKET_KLINE_COLUMNS = (
        "trade_time",
        "trade_date",
        "code",
        "period",
        "open",
        "high",
        "low",
        "close",
        "volume",
        "amount",
        "source",
        "synced_at",
        "created_at",
        "updated_at",
    )
    MARKET_SNAPSHOT_COLUMNS = (
        "trade_time",
        "trade_date",
        "code",
        "snapshot_kind",
        "pre_close",
        "last",
        "open",
        "high",
        "low",
        "close",
        "volume",
        "amount",
        "num_trades",
        "high_limited",
        "low_limited",
        "ask_price1",
        "ask_price2",
        "ask_price3",
        "ask_price4",
        "ask_price5",
        "ask_volume1",
        "ask_volume2",
        "ask_volume3",
        "ask_volume4",
        "ask_volume5",
        "bid_price1",
        "bid_price2",
        "bid_price3",
        "bid_price4",
        "bid_price5",
        "bid_volume1",
        "bid_volume2",
        "bid_volume3",
        "bid_volume4",
        "bid_volume5",
        "iopv",
        "trading_phase_code",
        "total_long_position",
        "pre_settle",
        "auction_price",
        "auction_volume",
        "settle",
        "contract_type",
        "expire_date",
        "underlying_security_code",
        "exercise_price",
        "action_day",
        "trading_day",
        "pre_open_interest",
        "open_interest",
        "average_price",
        "nominal_price",
        "ref_price",
        "bid_price_limit_up",
        "bid_price_limit_down",
        "offer_price_limit_up",
        "offer_price_limit_down",
        "source",
        "synced_at",
        "created_at",
        "updated_at",
    )

    def ensure_tables(self) -> None:
        super().ensure_tables()
        for ddl in iter_market_data_table_ddls():
            self.client.command(ddl)

    def save_market_kline_rows(self, rows) -> int:
        return self._insert_dataclass_rows_in_batches(
            table=AD_MARKET_KLINE_TABLE,
            columns=self.MARKET_KLINE_COLUMNS,
            rows=rows,
            partition_field="trade_date",
        )

    def save_market_snapshot_rows(self, rows) -> int:
        return self._insert_dataclass_rows_in_batches(
            table=AD_MARKET_SNAPSHOT_TABLE,
            columns=self.MARKET_SNAPSHOT_COLUMNS,
            rows=rows,
            partition_field="trade_date",
        )

    def load_latest_kline_trade_date(self, code_list: list[str], period: str):
        if not code_list:
            return None
        sql = f"""
        SELECT code, max(trade_date) AS latest_trade_date
        FROM {AD_MARKET_KLINE_TABLE}
        WHERE period = {{period:String}}
          AND code IN {{code_list:Array(String)}}
        GROUP BY code
        ORDER BY code
        """
        rows = self.client.query_rows(sql, {"period": period, "code_list": code_list})
        if len(rows) != len(code_list):
            return None
        latest_dates = [row[1] for row in rows if len(row) > 1 and row[1] is not None]
        if not latest_dates:
            return None
        return min(latest_dates)

    def load_latest_kline_trade_date_map(self, code_list: list[str], period: str) -> dict[str, object]:
        if not code_list:
            return {}
        sql = f"""
        SELECT code, max(trade_date) AS latest_trade_date
        FROM {AD_MARKET_KLINE_TABLE}
        WHERE period = {{period:String}}
          AND code IN {{code_list:Array(String)}}
        GROUP BY code
        ORDER BY code
        """
        rows = self.client.query_rows(sql, {"period": period, "code_list": code_list})
        result = {str(code): None for code in code_list}
        for row in rows:
            if not row:
                continue
            code = str(row[0])
            latest_trade_date = row[1] if len(row) > 1 else None
            result[code] = latest_trade_date
        return result

    def load_latest_snapshot_trade_date(self, code_list: list[str]):
        if not code_list:
            return None
        sql = f"""
        SELECT code, max(trade_date) AS latest_trade_date
        FROM {AD_MARKET_SNAPSHOT_TABLE}
        WHERE code IN {{code_list:Array(String)}}
        GROUP BY code
        ORDER BY code
        """
        rows = self.client.query_rows(sql, {"code_list": code_list})
        if len(rows) != len(code_list):
            return None
        latest_dates = [row[1] for row in rows if len(row) > 1 and row[1] is not None]
        if not latest_dates:
            return None
        return min(latest_dates)

    def load_kline_dict(self, query: MarketKlineQuery):
        if pd is None:  # pragma: no cover
            raise RuntimeError("未安装 pandas，无法返回 DataFrame。")
        if not query.code_list:
            return {}

        sql = f"""
        SELECT
            code,
            trade_time,
            argMax(open, updated_at) AS open,
            argMax(high, updated_at) AS high,
            argMax(low, updated_at) AS low,
            argMax(close, updated_at) AS close,
            argMax(volume, updated_at) AS volume,
            argMax(amount, updated_at) AS amount
        FROM {AD_MARKET_KLINE_TABLE}
        WHERE period = {{period:String}}
          AND code IN {{code_list:Array(String)}}
          AND trade_date >= {{begin_date:Date}}
          AND trade_date <= {{end_date:Date}}
        GROUP BY code, trade_time
        ORDER BY code, trade_time
        """
        frame = self.client.query_df(
            sql,
            {
                "period": query.period,
                "code_list": list(query.code_list),
                "begin_date": query.begin_date,
                "end_date": query.end_date,
            },
        )
        if frame.empty:
            return {}

        frame["trade_time"] = pd.to_datetime(frame["trade_time"])
        frame = _filter_kline_time(frame, query.begin_time, query.end_time)
        if frame.empty:
            return {}

        return _build_market_dict(
            frame=frame,
            code_list=query.code_list,
            ordered_columns=[field for field in KLINE_FIELDS if field != "trade_time"],
        )

    def load_snapshot_dict(self, query: MarketSnapshotQuery):
        if pd is None:  # pragma: no cover
            raise RuntimeError("未安装 pandas，无法返回 DataFrame。")
        if not query.code_list:
            return {}

        sql = f"""
        SELECT
            code,
            trade_time,
            argMax(snapshot_kind, updated_at) AS snapshot_kind,
            argMax(pre_close, updated_at) AS pre_close,
            argMax(last, updated_at) AS last,
            argMax(open, updated_at) AS open,
            argMax(high, updated_at) AS high,
            argMax(low, updated_at) AS low,
            argMax(close, updated_at) AS close,
            argMax(volume, updated_at) AS volume,
            argMax(amount, updated_at) AS amount,
            argMax(num_trades, updated_at) AS num_trades,
            argMax(high_limited, updated_at) AS high_limited,
            argMax(low_limited, updated_at) AS low_limited,
            argMax(ask_price1, updated_at) AS ask_price1,
            argMax(ask_price2, updated_at) AS ask_price2,
            argMax(ask_price3, updated_at) AS ask_price3,
            argMax(ask_price4, updated_at) AS ask_price4,
            argMax(ask_price5, updated_at) AS ask_price5,
            argMax(ask_volume1, updated_at) AS ask_volume1,
            argMax(ask_volume2, updated_at) AS ask_volume2,
            argMax(ask_volume3, updated_at) AS ask_volume3,
            argMax(ask_volume4, updated_at) AS ask_volume4,
            argMax(ask_volume5, updated_at) AS ask_volume5,
            argMax(bid_price1, updated_at) AS bid_price1,
            argMax(bid_price2, updated_at) AS bid_price2,
            argMax(bid_price3, updated_at) AS bid_price3,
            argMax(bid_price4, updated_at) AS bid_price4,
            argMax(bid_price5, updated_at) AS bid_price5,
            argMax(bid_volume1, updated_at) AS bid_volume1,
            argMax(bid_volume2, updated_at) AS bid_volume2,
            argMax(bid_volume3, updated_at) AS bid_volume3,
            argMax(bid_volume4, updated_at) AS bid_volume4,
            argMax(bid_volume5, updated_at) AS bid_volume5,
            argMax(iopv, updated_at) AS iopv,
            argMax(trading_phase_code, updated_at) AS trading_phase_code,
            argMax(total_long_position, updated_at) AS total_long_position,
            argMax(pre_settle, updated_at) AS pre_settle,
            argMax(auction_price, updated_at) AS auction_price,
            argMax(auction_volume, updated_at) AS auction_volume,
            argMax(settle, updated_at) AS settle,
            argMax(contract_type, updated_at) AS contract_type,
            argMax(expire_date, updated_at) AS expire_date,
            argMax(underlying_security_code, updated_at) AS underlying_security_code,
            argMax(exercise_price, updated_at) AS exercise_price,
            argMax(action_day, updated_at) AS action_day,
            argMax(trading_day, updated_at) AS trading_day,
            argMax(pre_open_interest, updated_at) AS pre_open_interest,
            argMax(open_interest, updated_at) AS open_interest,
            argMax(average_price, updated_at) AS average_price,
            argMax(nominal_price, updated_at) AS nominal_price,
            argMax(ref_price, updated_at) AS ref_price,
            argMax(bid_price_limit_up, updated_at) AS bid_price_limit_up,
            argMax(bid_price_limit_down, updated_at) AS bid_price_limit_down,
            argMax(offer_price_limit_up, updated_at) AS offer_price_limit_up,
            argMax(offer_price_limit_down, updated_at) AS offer_price_limit_down
        FROM {AD_MARKET_SNAPSHOT_TABLE}
        WHERE code IN {{code_list:Array(String)}}
          AND trade_date >= {{begin_date:Date}}
          AND trade_date <= {{end_date:Date}}
        GROUP BY code, trade_time
        ORDER BY code, trade_time
        """
        frame = self.client.query_df(
            sql,
            {
                "code_list": list(query.code_list),
                "begin_date": query.begin_date,
                "end_date": query.end_date,
            },
        )
        if frame.empty:
            return {}

        frame["trade_time"] = pd.to_datetime(frame["trade_time"])
        frame = _filter_snapshot_time(frame, query.begin_time, query.end_time)
        if frame.empty:
            return {}

        return _build_snapshot_dict(frame=frame, code_list=query.code_list)


def _build_market_dict(frame, code_list, ordered_columns: list[str]):
    result = {}
    for code in code_list:
        subset = frame[frame["code"] == code].copy()
        if subset.empty:
            continue
        subset = subset.set_index("trade_time")
        result[code] = subset.loc[:, ordered_columns]
    return result


def _build_snapshot_dict(frame, code_list):
    result = {}
    for code in code_list:
        subset = frame[frame["code"] == code].copy()
        if subset.empty:
            continue
        snapshot_kind = (
            subset["snapshot_kind"].iloc[0]
            if "snapshot_kind" in subset.columns and not subset["snapshot_kind"].isna().all()
            else SnapshotKind.SNAPSHOT
        )
        ordered_columns = [
            field
            for field in SNAPSHOT_KIND_TO_FIELDS.get(snapshot_kind, SNAPSHOT_FIELDS)
            if field != "trade_time"
        ]
        subset = subset.set_index("trade_time")
        result[code] = subset.loc[:, [col for col in ordered_columns if col in subset.columns]]
    return result


def _filter_kline_time(frame, begin_time, end_time):
    if begin_time is None and end_time is None:
        return frame
    hhmm = frame["trade_time"].dt.hour * 100 + frame["trade_time"].dt.minute
    mask = pd.Series(True, index=frame.index)
    if begin_time is not None:
        mask &= hhmm >= int(begin_time)
    if end_time is not None:
        mask &= hhmm <= int(end_time)
    return frame.loc[mask].copy()


def _filter_snapshot_time(frame, begin_time, end_time):
    if begin_time is None and end_time is None:
        return frame
    hhmmssmmm = (
        frame["trade_time"].dt.hour * 10000000
        + frame["trade_time"].dt.minute * 100000
        + frame["trade_time"].dt.second * 1000
        + (frame["trade_time"].dt.microsecond // 1000)
    )
    mask = pd.Series(True, index=frame.index)
    if begin_time is not None:
        mask &= hhmmssmmm >= int(begin_time)
    if end_time is not None:
        mask &= hhmmssmmm <= int(end_time)
    return frame.loc[mask].copy()


__all__ = ["MarketDataRepository"]
