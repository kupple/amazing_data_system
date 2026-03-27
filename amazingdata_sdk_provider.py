#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""AmazingData 官方 SDK 到本地增量同步框架的适配层."""

from __future__ import annotations

import logging
import os
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any, Iterable, Optional, Sequence
from zoneinfo import ZoneInfo

try:
    import pandas as pd
except Exception:  # pragma: no cover
    pd = None  # type: ignore

try:
    from dotenv import load_dotenv
except Exception:  # pragma: no cover
    load_dotenv = None  # type: ignore

from amazingdata_constants import (
    FactorType,
    Market,
    SNAPSHOT_FIELDS,
    SNAPSHOT_FUTURE_FIELDS,
    SNAPSHOT_HKT_FIELDS,
    SNAPSHOT_INDEX_FIELDS,
    SNAPSHOT_OPTION_FIELDS,
    SnapshotKind,
)
from base_data import BaseDataSyncProvider
from data_models import (
    CodeInfoRow,
    HistCodeDailyRow,
    HistoryStockStatusRow,
    MarketKlineRow,
    MarketSnapshotRow,
    PriceFactorRow,
    StockBasicRow,
    TradeCalendarRow,
    normalize_code_list,
    to_ch_date,
    to_yyyymmdd,
)
from info_data import InfoDataSyncProvider
from market_data import MarketDataSyncProvider


logger = logging.getLogger(__name__)
SHANGHAI_TZ = ZoneInfo("Asia/Shanghai")


@dataclass(frozen=True)
class AmazingDataSDKConfig:
    """AmazingData SDK 登录配置."""

    username: str
    password: str
    host: str
    port: int
    local_path: str

    @classmethod
    def from_env(
        cls,
        env_file: Optional[str] = None,
        local_path: Optional[str] = None,
    ) -> "AmazingDataSDKConfig":
        if load_dotenv is not None:
            load_dotenv(env_file, override=False)

        username = os.getenv("AD_ACCOUNT", "").strip()
        password = os.getenv("AD_PASSWORD", "").strip()
        host = os.getenv("AD_IP", "").strip()
        port = int(os.getenv("AD_PORT", "0") or 0)

        if not username:
            raise ValueError("缺少环境变量 AD_ACCOUNT")
        if not password:
            raise ValueError("缺少环境变量 AD_PASSWORD")
        if not host:
            raise ValueError("缺少环境变量 AD_IP")
        if not port:
            raise ValueError("缺少环境变量 AD_PORT")

        resolved_local_path = _normalize_local_path(
            local_path or os.getenv("AD_LOCAL_PATH", "") or str(Path.cwd() / "amazing_data_cache")
        )
        Path(resolved_local_path.replace("//", "/")).mkdir(parents=True, exist_ok=True)

        return cls(
            username=username,
            password=password,
            host=host,
            port=port,
            local_path=resolved_local_path,
        )


class AmazingDataSDKSession:
    """AmazingData SDK 登录会话.

    这里把登录、对象创建和登出统一包起来，避免 provider 每个方法都重复做连接管理。
    """

    def __init__(self, config: AmazingDataSDKConfig) -> None:
        self.config = config
        self._ad = None
        self._base = None
        self._info = None
        self._market = None
        self._connected = False
        self._calendar_cache: dict[str, list] = {}
        self._raw_calendar_cache = None

    def ensure_connected(self) -> None:
        if self._connected:
            return

        try:
            import AmazingData as ad
        except Exception as exc:  # pragma: no cover
            raise RuntimeError(
                "未安装 AmazingData 官方 SDK，无法执行真实同步测试。"
            ) from exc

        ok = ad.login(
            username=self.config.username,
            password=self.config.password,
            host=self.config.host,
            port=self.config.port,
        )
        if ok is False:
            raise RuntimeError("AmazingData 登录失败，请检查账号、密码、IP、端口和权限。")

        self._ad = ad
        self._base = ad.BaseData()
        self._info = ad.InfoData()
        self._connected = True
        logger.info("AmazingData SDK login success host=%s port=%s", self.config.host, self.config.port)

    @property
    def base(self):
        self.ensure_connected()
        return self._base

    @property
    def info(self):
        self.ensure_connected()
        return self._info

    @property
    def market(self):
        self.ensure_connected()
        if self._market is None:
            self._market = self._build_market_client()
        return self._market

    def get_calendar_dates(self, market: str = Market.SH) -> list:
        self.ensure_connected()
        # 当前 AmazingData SDK 版本直接 `get_calendar()` 即可，`market` 参数在这里不再向下透传。
        cache_key = "default"
        if cache_key not in self._calendar_cache:
            dates = self._load_calendar_dates()
            self._calendar_cache[cache_key] = dates
        return self._calendar_cache[cache_key]

    def get_latest_trade_date(self, market: str = Market.SH):
        dates = self.get_calendar_dates(market=market)
        if not dates:
            fallback = date.today()
            logger.warning(
                "AmazingData get_calendar 不可用，latest_trade_date 临时回退为 today=%s",
                fallback,
            )
            return fallback
        return to_ch_date(dates[-1])

    def get_snapshot_date(self) -> date:
        """日级快照接口统一使用当天日期作为落库快照日.

        `get_code_info` / `get_stock_basic` 都属于“每日最新快照”类型，
        不需要为了拿一个快照日期再去强依赖 `get_calendar()`。
        """

        return date.today()

    def _load_calendar_dates(self) -> list:
        try:
            result = self.base.get_calendar()
        except Exception as exc:
            logger.warning("AmazingData get_calendar() 调用失败: %s", exc)
            return []

        normalized = _normalize_calendar_result(result)
        if normalized:
            logger.info("AmazingData get_calendar() success count=%s", len(normalized))
            return normalized

        logger.warning("AmazingData get_calendar() 返回空结果")
        return []

    def get_raw_calendar(self):
        self.ensure_connected()
        if self._raw_calendar_cache is None:
            try:
                self._raw_calendar_cache = self.base.get_calendar()
            except Exception as exc:
                logger.warning("AmazingData get_calendar() 原始结果获取失败: %s", exc)
                self._raw_calendar_cache = []
        return self._raw_calendar_cache

    def resolve_period_value(self, period: str | int) -> int:
        self.ensure_connected()
        text = str(period).strip()
        if not text:
            raise ValueError("period 不能为空。")
        if text.isdigit():
            return int(text)

        period_obj = getattr(getattr(self._ad, "constant", object()), "Period", None)
        if period_obj is not None and hasattr(period_obj, text):
            attr = getattr(period_obj, text)
            value = getattr(attr, "value", attr)
            return int(value)
        raise ValueError(f"无法解析官方 Period 枚举: {period!r}")

    def _build_market_client(self):
        errors: list[str] = []
        constructors = []
        raw_calendar = self.get_raw_calendar()
        if raw_calendar:
            constructors.append(((raw_calendar,), "MarketData(calendar)"))
        constructors.append((([],), "MarketData([])"))
        constructors.append((tuple(), "MarketData()"))

        for args, label in constructors:
            try:
                market = self._ad.MarketData(*args)
                logger.info("AmazingData %s 初始化成功", label)
                return market
            except Exception as exc:
                errors.append(f"{label}: {type(exc).__name__}: {exc}")
                continue

        raise RuntimeError("AmazingData MarketData 初始化失败: " + " | ".join(errors[-3:]))

    def close(self) -> None:
        if not self._connected or self._ad is None:
            return
        try:
            logout = getattr(self._ad, "logout", None)
            if callable(logout):
                logout(username=self.config.username)
        except Exception:
            logger.exception("AmazingData logout failed")
        finally:
            self._connected = False
            self._ad = None
            self._base = None
            self._info = None
            self._market = None
            self._calendar_cache.clear()
            self._raw_calendar_cache = None


class AmazingDataSDKProvider(BaseDataSyncProvider, InfoDataSyncProvider, MarketDataSyncProvider):
    """把 AmazingData 官方 SDK 返回值转换成我们的本地行模型."""

    def __init__(self, config: AmazingDataSDKConfig) -> None:
        self.config = config
        self.session = AmazingDataSDKSession(config)

    def close(self) -> None:
        self.session.close()

    def fetch_calendar(
        self,
        market: str,
        start_date=None,
        end_date=None,
    ) -> Iterable[TradeCalendarRow]:
        for raw_date in self.session.get_calendar_dates(market=market):
            trade_date = to_ch_date(raw_date)
            if start_date is not None and trade_date < start_date:
                continue
            if end_date is not None and trade_date > end_date:
                continue
            yield TradeCalendarRow(market=market, trade_date=trade_date)

    def fetch_code_info(
        self,
        security_type: str,
        start_date=None,
        end_date=None,
    ) -> Iterable[CodeInfoRow]:
        logger.info("AmazingData fetch_code_info start security_type=%s", security_type)
        frame = _ensure_dataframe(self.session.base.get_code_info(security_type=security_type), "get_code_info")
        logger.info("AmazingData fetch_code_info loaded rows=%s cols=%s", len(frame), len(frame.columns))
        snapshot_date = self.session.get_snapshot_date()
        if start_date is not None and snapshot_date < start_date:
            return
        if end_date is not None and snapshot_date > end_date:
            return

        for code, row in frame.iterrows():
            market_code = str(code).strip()
            if not market_code:
                continue
            yield CodeInfoRow(
                snapshot_date=snapshot_date,
                security_type=security_type,
                code=market_code,
                symbol=_as_str(_series_get(row, "symbol", "SYMBOL")),
                security_status_raw=_stringify(_series_get(row, "security_status", "SECURITY_STATUS")),
                pre_close=_as_float(_series_get(row, "pre_close", "PRECLOSE", "PRE_CLOSE")),
                high_limited=_as_float(_series_get(row, "high_limited", "HIGH_LIMITED")),
                low_limited=_as_float(_series_get(row, "low_limited", "LOW_LIMITED")),
                price_tick=_as_float(_series_get(row, "price_tick", "PRICE_TICK")),
            )

    def fetch_hist_code_daily(
        self,
        security_type: str,
        start_date=None,
        end_date=None,
    ) -> Iterable[HistCodeDailyRow]:
        logger.info(
            "AmazingData fetch_hist_code_daily start security_type=%s start_date=%s end_date=%s",
            security_type,
            start_date,
            end_date,
        )
        latest_trade_date = self.session.get_latest_trade_date(Market.SH)
        if latest_trade_date is None:
            return

        actual_end = end_date or latest_trade_date
        actual_start = start_date or actual_end
        if actual_start > actual_end:
            return

        calendar_dates = [to_ch_date(item) for item in self.session.get_calendar_dates(Market.SH)]
        iter_dates = [
            current_date
            for current_date in calendar_dates
            if actual_start <= current_date <= actual_end
        ]
        if not iter_dates:
            logger.warning(
                "未获取到可用交易日历，fetch_hist_code_daily 改为按自然日遍历: start=%s end=%s",
                actual_start,
                actual_end,
            )
            iter_dates = list(_iter_natural_dates(actual_start, actual_end))

        for trade_date in iter_dates:
            try:
                code_list = self.session.base.get_hist_code_list(
                    security_type=security_type,
                    start_date=to_yyyymmdd(trade_date),
                    end_date=to_yyyymmdd(trade_date),
                    local_path=self.config.local_path,
                )
            except Exception as exc:
                logger.warning(
                    "fetch_hist_code_daily 跳过 trade_date=%s security_type=%s: %s",
                    trade_date,
                    security_type,
                    exc,
                )
                continue
            for code in normalize_code_list(code_list or []):
                yield HistCodeDailyRow(
                    trade_date=trade_date,
                    security_type=security_type,
                    code=code,
                )

    def fetch_price_factor(
        self,
        factor_type: str,
        code_list: Sequence[str],
        start_date=None,
        end_date=None,
    ) -> Iterable[PriceFactorRow]:
        normalized_codes = normalize_code_list(code_list)
        if not normalized_codes:
            return

        logger.info(
            "AmazingData fetch_price_factor start factor_type=%s code_count=%s",
            factor_type,
            len(normalized_codes),
        )

        if factor_type == FactorType.ADJ:
            frame = self.session.base.get_adj_factor(
                code_list=normalized_codes,
                local_path=self.config.local_path,
                is_local=False,
            )
        elif factor_type == FactorType.BACKWARD:
            frame = self.session.base.get_backward_factor(
                code_list=normalized_codes,
                local_path=self.config.local_path,
                is_local=False,
            )
        else:
            raise ValueError(f"不支持的 factor_type: {factor_type}")

        frame = _ensure_dataframe(frame, f"fetch_price_factor({factor_type})")
        logger.info("AmazingData fetch_price_factor loaded rows=%s cols=%s", len(frame), len(frame.columns))
        for trade_date, row in frame.iterrows():
            current_date = to_ch_date(trade_date)
            if start_date is not None and current_date < start_date:
                continue
            if end_date is not None and current_date > end_date:
                continue
            for code, factor_value in row.items():
                numeric_value = _as_float(factor_value)
                if numeric_value is None:
                    continue
                yield PriceFactorRow(
                    factor_type=factor_type,
                    trade_date=current_date,
                    code=str(code).strip(),
                    factor_value=numeric_value,
                )

    def fetch_stock_basic(
        self,
        code_list: Sequence[str],
        start_date=None,
        end_date=None,
    ) -> Iterable[StockBasicRow]:
        normalized_codes = normalize_code_list(code_list)
        if not normalized_codes:
            return

        logger.info("AmazingData fetch_stock_basic start code_count=%s", len(normalized_codes))

        snapshot_date = self.session.get_snapshot_date()
        if start_date is not None and snapshot_date < start_date:
            return
        if end_date is not None and snapshot_date > end_date:
            return

        frame = _ensure_dataframe(self.session.info.get_stock_basic(normalized_codes), "get_stock_basic")
        logger.info("AmazingData fetch_stock_basic loaded rows=%s cols=%s", len(frame), len(frame.columns))
        for record in _frame_to_records(frame):
            market_code = _as_str(_record_get(record, "MARKET_CODE", "market_code", "CODE", "code"))
            if not market_code:
                continue
            yield StockBasicRow(
                snapshot_date=snapshot_date,
                market_code=market_code,
                security_name=_as_str(_record_get(record, "SECURITY_NAME", "security_name")),
                comp_name=_as_str(_record_get(record, "COMP_NAME", "comp_name")),
                pinyin=_as_str(_record_get(record, "PINYIN", "pinyin")),
                comp_name_eng=_as_str(_record_get(record, "COMP_NAME_ENG", "comp_name_eng")),
                list_date=_as_int(_record_get(record, "LISTDATE", "list_date")),
                delist_date=_as_int(_record_get(record, "DELISTDATE", "delist_date")),
                listplate_name=_as_str(_record_get(record, "LISTPLATE_NAME", "listplate_name")),
                comp_sname_eng=_as_str(_record_get(record, "COMP_SNAME_ENG", "comp_sname_eng")),
                is_listed=_as_int(_record_get(record, "IS_LISTED", "is_listed")),
            )

    def fetch_kline(
        self,
        code_list: Sequence[str],
        begin_date: date,
        end_date: date,
        period: str,
        begin_time: Optional[int] = None,
        end_time: Optional[int] = None,
    ) -> Iterable[MarketKlineRow]:
        normalized_codes = normalize_code_list(code_list)
        if not normalized_codes:
            return

        period_value = self.session.resolve_period_value(period)
        logger.info(
            "AmazingData fetch_kline start code_count=%s begin_date=%s end_date=%s period=%s",
            len(normalized_codes),
            begin_date,
            end_date,
            period,
        )
        base_kwargs: dict[str, Any] = {
            "begin_date": to_yyyymmdd(begin_date),
            "end_date": to_yyyymmdd(end_date),
        }
        if begin_time is not None:
            base_kwargs["begin_time"] = begin_time
        if end_time is not None:
            base_kwargs["end_time"] = end_time

        result = self._query_kline_with_variants(
            code_list=normalized_codes,
            period=period,
            period_value=period_value,
            base_kwargs=base_kwargs,
        )
        logger.info(
            "AmazingData fetch_kline loaded result_type=%s rows=%s",
            type(result).__name__,
            _count_sdk_result_rows(result),
        )
        for record in _iter_records_from_sdk_result(
            result,
            action="query_kline",
            injected_code_fields=("code", "CODE", "market_code", "MARKET_CODE"),
            index_field="trade_time",
        ):
            code = _as_str(_record_get(record, "code", "CODE", "market_code", "MARKET_CODE"))
            trade_time_value = _record_get(record, "trade_time", "TRADE_TIME")
            if not code or trade_time_value is None:
                continue
            trade_time = _to_datetime(trade_time_value)
            if trade_time is None:
                continue
            yield MarketKlineRow(
                trade_time=trade_time,
                trade_date=trade_time.date(),
                code=code,
                period=str(period),
                open=_as_float(_record_get(record, "open", "OPEN")),
                high=_as_float(_record_get(record, "high", "HIGH")),
                low=_as_float(_record_get(record, "low", "LOW")),
                close=_as_float(_record_get(record, "close", "CLOSE")),
                volume=_as_float(_record_get(record, "volume", "VOLUME")),
                amount=_as_float(_record_get(record, "amount", "AMOUNT")),
            )

    def _query_kline_with_variants(
        self,
        code_list: Sequence[str],
        period: str,
        period_value: int,
        base_kwargs: dict[str, Any],
    ):
        variants: list[dict[str, Any]] = []
        variant_with_resolved = dict(base_kwargs)
        variant_with_resolved["period"] = int(period_value)
        variants.append(variant_with_resolved)
        errors: list[str] = []
        for kwargs in variants:
            try:
                logger.info("AmazingData query_kline try kwargs=%s", kwargs)
                result = self.session.market.query_kline(code_list, **kwargs)
                if not _is_sdk_result_empty(result):
                    return result
                errors.append(f"kwargs={kwargs} -> empty_result")
            except Exception as exc:
                errors.append(f"kwargs={kwargs} -> {type(exc).__name__}: {exc}")
                continue

        logger.warning("AmazingData query_kline 所有尝试均未取到数据: %s", " | ".join(errors[-3:]))
        return {}

    def fetch_snapshot(
        self,
        code_list: Sequence[str],
        begin_date: date,
        end_date: date,
        begin_time: Optional[int] = None,
        end_time: Optional[int] = None,
    ) -> Iterable[MarketSnapshotRow]:
        normalized_codes = normalize_code_list(code_list)
        if not normalized_codes:
            return

        logger.info(
            "AmazingData fetch_snapshot start code_count=%s begin_date=%s end_date=%s",
            len(normalized_codes),
            begin_date,
            end_date,
        )
        kwargs: dict[str, Any] = {
            "begin_date": to_yyyymmdd(begin_date),
            "end_date": to_yyyymmdd(end_date),
        }
        if begin_time is not None:
            kwargs["begin_time"] = begin_time
        if end_time is not None:
            kwargs["end_time"] = end_time

        result = self.session.market.query_snapshot(normalized_codes, **kwargs)
        logger.info(
            "AmazingData fetch_snapshot loaded result_type=%s rows=%s",
            type(result).__name__,
            _count_sdk_result_rows(result),
        )
        for code, frame in _iter_code_frames_from_result(result, action="query_snapshot"):
            snapshot_kind = _detect_snapshot_kind(frame)
            for record in _iter_records_from_sdk_result(
                {code: frame},
                action="query_snapshot",
                injected_code_fields=("code", "CODE", "market_code", "MARKET_CODE"),
                index_field="trade_time",
            ):
                market_code = _as_str(_record_get(record, "code", "CODE", "market_code", "MARKET_CODE"))
                trade_time_value = _record_get(record, "trade_time", "TRADE_TIME")
                if not market_code or trade_time_value is None:
                    continue
                trade_time = _to_datetime(trade_time_value)
                if trade_time is None:
                    continue
                yield MarketSnapshotRow(
                    trade_time=trade_time,
                    trade_date=trade_time.date(),
                    code=market_code,
                    snapshot_kind=snapshot_kind,
                    pre_close=_as_float(_record_get(record, "pre_close", "PRECLOSE", "PRE_CLOSE")),
                    last=_as_float(_record_get(record, "last", "LAST")),
                    open=_as_float(_record_get(record, "open", "OPEN")),
                    high=_as_float(_record_get(record, "high", "HIGH")),
                    low=_as_float(_record_get(record, "low", "LOW")),
                    close=_as_float(_record_get(record, "close", "CLOSE")),
                    volume=_as_float(_record_get(record, "volume", "VOLUME")),
                    amount=_as_float(_record_get(record, "amount", "AMOUNT")),
                    num_trades=_as_float(_record_get(record, "num_trades", "NUM_TRADES")),
                    high_limited=_as_float(_record_get(record, "high_limited", "HIGH_LIMITED")),
                    low_limited=_as_float(_record_get(record, "low_limited", "LOW_LIMITED")),
                    ask_price1=_as_float(_record_get(record, "ask_price1", "ASK_PRICE1")),
                    ask_price2=_as_float(_record_get(record, "ask_price2", "ASK_PRICE2")),
                    ask_price3=_as_float(_record_get(record, "ask_price3", "ASK_PRICE3")),
                    ask_price4=_as_float(_record_get(record, "ask_price4", "ASK_PRICE4")),
                    ask_price5=_as_float(_record_get(record, "ask_price5", "ASK_PRICE5")),
                    ask_volume1=_as_int(_record_get(record, "ask_volume1", "ASK_VOLUME1")),
                    ask_volume2=_as_int(_record_get(record, "ask_volume2", "ASK_VOLUME2")),
                    ask_volume3=_as_int(_record_get(record, "ask_volume3", "ASK_VOLUME3")),
                    ask_volume4=_as_int(_record_get(record, "ask_volume4", "ASK_VOLUME4")),
                    ask_volume5=_as_int(_record_get(record, "ask_volume5", "ASK_VOLUME5")),
                    bid_price1=_as_float(_record_get(record, "bid_price1", "BID_PRICE1")),
                    bid_price2=_as_float(_record_get(record, "bid_price2", "BID_PRICE2")),
                    bid_price3=_as_float(_record_get(record, "bid_price3", "BID_PRICE3")),
                    bid_price4=_as_float(_record_get(record, "bid_price4", "BID_PRICE4")),
                    bid_price5=_as_float(_record_get(record, "bid_price5", "BID_PRICE5")),
                    bid_volume1=_as_int(_record_get(record, "bid_volume1", "BID_VOLUME1")),
                    bid_volume2=_as_int(_record_get(record, "bid_volume2", "BID_VOLUME2")),
                    bid_volume3=_as_int(_record_get(record, "bid_volume3", "BID_VOLUME3")),
                    bid_volume4=_as_int(_record_get(record, "bid_volume4", "BID_VOLUME4")),
                    bid_volume5=_as_int(_record_get(record, "bid_volume5", "BID_VOLUME5")),
                    iopv=_as_float(_record_get(record, "iopv", "IOPV")),
                    trading_phase_code=_as_str(_record_get(record, "trading_phase_code", "TRADING_PHASE_CODE")),
                    total_long_position=_as_int(_record_get(record, "total_long_position", "TOTAL_LONG_POSITION")),
                    pre_settle=_as_float(_record_get(record, "pre_settle", "PRE_SETTLE")),
                    auction_price=_as_float(_record_get(record, "auction_price", "AUCTION_PRICE")),
                    auction_volume=_as_int(_record_get(record, "auction_volume", "AUCTION_VOLUME")),
                    settle=_as_float(_record_get(record, "settle", "SETTLE")),
                    contract_type=_as_str(_record_get(record, "contract_type", "CONTRACT_TYPE")),
                    expire_date=_as_int(_record_get(record, "expire_date", "EXPIRE_DATE")),
                    underlying_security_code=_as_str(
                        _record_get(record, "underlying_security_code", "UNDERLYING_SECURITY_CODE")
                    ),
                    exercise_price=_as_float(_record_get(record, "exercise_price", "EXERCISE_PRICE")),
                    action_day=_as_str(_record_get(record, "action_day", "ACTION_DAY")),
                    trading_day=_as_str(_record_get(record, "trading_day", "TRADING_DAY")),
                    pre_open_interest=_as_int(_record_get(record, "pre_open_interest", "PRE_OPEN_INTEREST")),
                    open_interest=_as_int(_record_get(record, "open_interest", "OPEN_INTEREST")),
                    average_price=_as_float(_record_get(record, "average_price", "AVERAGE_PRICE")),
                    nominal_price=_as_float(_record_get(record, "nominal_price", "NOMINAL_PRICE")),
                    ref_price=_as_float(_record_get(record, "ref_price", "REF_PRICE")),
                    bid_price_limit_up=_as_float(_record_get(record, "bid_price_limit_up", "BID_PRICE_LIMIT_UP")),
                    bid_price_limit_down=_as_float(
                        _record_get(record, "bid_price_limit_down", "BID_PRICE_LIMIT_DOWN")
                    ),
                    offer_price_limit_up=_as_float(
                        _record_get(record, "offer_price_limit_up", "OFFER_PRICE_LIMIT_UP")
                    ),
                    offer_price_limit_down=_as_float(
                        _record_get(record, "offer_price_limit_down", "OFFER_PRICE_LIMIT_DOWN")
                    ),
                )

    def fetch_history_stock_status(
        self,
        code_list: Sequence[str],
        start_date=None,
        end_date=None,
    ) -> Iterable[HistoryStockStatusRow]:
        normalized_codes = normalize_code_list(code_list)
        if not normalized_codes:
            return

        logger.info(
            "AmazingData fetch_history_stock_status start code_count=%s start_date=%s end_date=%s",
            len(normalized_codes),
            start_date,
            end_date,
        )

        kwargs: dict[str, Any] = {
            "code_list": normalized_codes,
            "local_path": self.config.local_path,
            "is_local": False,
        }
        if start_date is not None:
            kwargs["begin_date"] = to_yyyymmdd(start_date)
        if end_date is not None:
            kwargs["end_date"] = to_yyyymmdd(end_date)

        result = self.session.info.get_history_stock_status(**kwargs)
        logger.info(
            "AmazingData fetch_history_stock_status loaded result_type=%s rows=%s",
            type(result).__name__,
            _count_sdk_result_rows(result),
        )
        for record in _iter_records_from_sdk_result(
            result,
            action="get_history_stock_status",
            injected_code_fields=("MARKET_CODE", "market_code", "CODE", "code"),
        ):
            market_code = _as_str(_record_get(record, "MARKET_CODE", "market_code", "CODE", "code"))
            trade_date_value = _record_get(record, "TRADE_DATE", "trade_date")
            if not market_code or trade_date_value is None:
                continue
            trade_date = to_ch_date(trade_date_value)
            if start_date is not None and trade_date < start_date:
                continue
            if end_date is not None and trade_date > end_date:
                continue
            yield HistoryStockStatusRow(
                trade_date=trade_date,
                market_code=market_code,
                preclose=_as_float(_record_get(record, "PRECLOSE", "preclose", "PRE_CLOSE")),
                high_limited=_as_float(_record_get(record, "HIGH_LIMITED", "high_limited")),
                low_limited=_as_float(_record_get(record, "LOW_LIMITED", "low_limited")),
                price_high_lmt_rate=_as_float(
                    _record_get(record, "PRICE_HIGH_LMT_RATE", "price_high_lmt_rate")
                ),
                price_low_lmt_rate=_as_float(
                    _record_get(record, "PRICE_LOW_LMT_RATE", "price_low_lmt_rate")
                ),
                is_st_sec=_as_str(_record_get(record, "IS_ST_SEC", "is_st_sec")),
                is_susp_sec=_as_str(_record_get(record, "IS_SUSP_SEC", "is_susp_sec")),
                is_wd_sec=_as_str(_record_get(record, "IS_WD_SEC", "is_wd_sec")),
                is_xr_sec=_as_str(_record_get(record, "IS_XR_SEC", "is_xr_sec")),
            )


def _normalize_local_path(local_path: str) -> str:
    path = str(Path(local_path).resolve()).replace("\\", "//")
    if not path.endswith("//"):
        path += "//"
    return path


def _normalize_calendar_result(result: Any) -> list:
    if result is None:
        return []
    if isinstance(result, list):
        normalized: list = []
        for item in result:
            try:
                normalized.append(to_yyyymmdd(item))
            except Exception:
                text = _as_str(item)
                if text is None:
                    continue
                normalized.append(text)
        return normalized
    return []


def _iter_natural_dates(start_date: date, end_date: date):
    current = start_date
    while current <= end_date:
        yield current
        current += timedelta(days=1)


def _ensure_dataframe(obj: Any, action: str):
    if pd is None:  # pragma: no cover
        raise RuntimeError("未安装 pandas，无法处理 SDK 返回的 DataFrame。")
    if not isinstance(obj, pd.DataFrame):
        raise TypeError(f"{action} 期望返回 DataFrame，实际得到 {type(obj).__name__}")
    return obj


def _frame_to_records(frame, index_field: str | None = None):
    normalized = frame.copy()
    if index_field is not None and index_field not in normalized.columns:
        normalized[index_field] = normalized.index
    return normalized.to_dict("records")


def _iter_records_from_sdk_result(
    obj: Any,
    action: str,
    injected_code_fields: Sequence[str] = ("MARKET_CODE", "market_code"),
    index_field: str | None = None,
):
    if pd is None:  # pragma: no cover
        raise RuntimeError("未安装 pandas，无法处理 SDK 返回的 DataFrame。")

    if isinstance(obj, pd.DataFrame):
        yield from _frame_to_records(obj, index_field=index_field)
        return

    if isinstance(obj, dict):
        for code, value in obj.items():
            if value is None:
                continue
            if not isinstance(value, pd.DataFrame):
                raise TypeError(
                    f"{action} 返回 dict 时，value 期望为 DataFrame，实际得到 {type(value).__name__}"
                )
            code_text = _as_str(code)
            for record in _frame_to_records(value, index_field=index_field):
                if code_text and all(_record_get(record, field) is None for field in injected_code_fields):
                    record[injected_code_fields[0]] = code_text
                yield record
        return

    raise TypeError(f"{action} 期望返回 DataFrame 或 dict，实际得到 {type(obj).__name__}")


def _iter_code_frames_from_result(obj: Any, action: str):
    if pd is None:  # pragma: no cover
        raise RuntimeError("未安装 pandas，无法处理 SDK 返回的 DataFrame。")

    yield from _iter_code_frames_from_result_inner(obj=obj, action=action, parent_code=None)


def _iter_code_frames_from_result_inner(obj: Any, action: str, parent_code: Optional[str]):
    if pd is None:  # pragma: no cover
        raise RuntimeError("未安装 pandas，无法处理 SDK 返回的 DataFrame。")

    if isinstance(obj, pd.DataFrame):
        code_column = None
        for candidate in ("code", "CODE", "market_code", "MARKET_CODE"):
            if candidate in obj.columns:
                code_column = candidate
                break
        if code_column is not None:
            for code, frame in obj.groupby(code_column):
                yield _as_str(code), frame.copy()
            return
        if parent_code is not None:
            yield parent_code, obj
            return
        raise TypeError(f"{action} 返回 DataFrame 时缺少 code 列，无法拆分为 dict[code, DataFrame]")

    if isinstance(obj, dict):
        for key, value in obj.items():
            if value is None:
                continue
            current_code = _as_str(key) or parent_code
            if isinstance(value, (dict, pd.DataFrame)):
                yield from _iter_code_frames_from_result_inner(
                    obj=value,
                    action=action,
                    parent_code=current_code,
                )
                continue
            raise TypeError(
                f"{action} 返回 dict 时，叶子节点期望为 DataFrame 或 dict，实际得到 {type(value).__name__}"
            )
        return

    raise TypeError(f"{action} 期望返回 DataFrame 或 dict，实际得到 {type(obj).__name__}")


def _count_sdk_result_rows(obj: Any) -> int:
    if pd is not None and isinstance(obj, pd.DataFrame):
        return int(len(obj))
    if isinstance(obj, dict):
        total = 0
        for value in obj.values():
            total += _count_sdk_result_rows(value)
        return total
    return 0


def _is_sdk_result_empty(obj: Any) -> bool:
    if obj is None:
        return True
    if pd is not None and isinstance(obj, pd.DataFrame):
        return obj.empty
    if isinstance(obj, dict):
        if not obj:
            return True
        return all(_is_sdk_result_empty(value) for value in obj.values())
    if isinstance(obj, (list, tuple, set)):
        return len(obj) == 0
    return False


def _detect_snapshot_kind(frame) -> str:
    columns = {str(col).strip().lower() for col in frame.columns}
    if {"total_long_position", "pre_settle", "exercise_price"} & columns:
        return SnapshotKind.SNAPSHOT_OPTION
    if {"nominal_price", "ref_price", "bid_price_limit_up", "offer_price_limit_up"} & columns:
        return SnapshotKind.SNAPSHOT_HKT
    if {"action_day", "trading_day", "open_interest"} & columns:
        return SnapshotKind.SNAPSHOT_FUTURE
    if columns.issubset(set(SNAPSHOT_INDEX_FIELDS)):
        return SnapshotKind.SNAPSHOT_INDEX
    if not ({"ask_price1", "bid_price1", "trading_phase_code"} & columns):
        if {"last", "pre_close", "open", "high", "low", "close", "volume", "amount"} <= columns:
            return SnapshotKind.SNAPSHOT_INDEX
    return SnapshotKind.SNAPSHOT


def _record_get(record: dict[str, Any], *candidates: str) -> Any:
    for candidate in candidates:
        if candidate in record:
            return record[candidate]
        upper = candidate.upper()
        lower = candidate.lower()
        if upper in record:
            return record[upper]
        if lower in record:
            return record[lower]
    return None


def _to_datetime(value: Any):
    if value is None:
        return None
    if isinstance(value, datetime):
        if value.tzinfo is None:
            return value.replace(tzinfo=SHANGHAI_TZ)
        return value
    if pd is not None:
        try:
            dt = pd.to_datetime(value)
            if pd.isna(dt):
                return None
            py_dt = dt.to_pydatetime()
            if py_dt.tzinfo is None:
                return py_dt.replace(tzinfo=SHANGHAI_TZ)
            return py_dt
        except Exception:
            return None
    return None


def _series_get(series, *candidates: str) -> Any:
    for candidate in candidates:
        if candidate in series:
            return series[candidate]
        upper = candidate.upper()
        lower = candidate.lower()
        if upper in series:
            return series[upper]
        if lower in series:
            return series[lower]
    return None


def _as_str(value: Any) -> Optional[str]:
    if value is None:
        return None
    if pd is not None and pd.isna(value):
        return None
    text = str(value).strip()
    return text or None


def _as_int(value: Any) -> Optional[int]:
    if value is None:
        return None
    if pd is not None and pd.isna(value):
        return None
    try:
        return int(value)
    except Exception:
        text = str(value).strip()
        if not text:
            return None
        return int(float(text))


def _as_float(value: Any) -> Optional[float]:
    if value is None:
        return None
    if pd is not None and pd.isna(value):
        return None
    try:
        return float(value)
    except Exception:
        text = str(value).strip()
        if not text:
            return None
        return float(text)


def _stringify(value: Any) -> Optional[str]:
    if value is None:
        return None
    if isinstance(value, (list, tuple, set)):
        return ",".join(str(item) for item in value)
    return _as_str(value)


__all__ = [
    "AmazingDataSDKConfig",
    "AmazingDataSDKProvider",
    "AmazingDataSDKSession",
]
