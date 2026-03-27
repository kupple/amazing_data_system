#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""AmazingData 官方 SDK 到本地增量同步框架的适配层."""

from __future__ import annotations

import logging
import os
from dataclasses import dataclass
from datetime import date, timedelta
from pathlib import Path
from typing import Any, Iterable, Optional, Sequence

try:
    import pandas as pd
except Exception:  # pragma: no cover
    pd = None  # type: ignore

try:
    from dotenv import load_dotenv
except Exception:  # pragma: no cover
    load_dotenv = None  # type: ignore

from amazingdata_constants import FactorType, Market
from base_data import BaseDataSyncProvider
from data_models import (
    CodeInfoRow,
    HistCodeDailyRow,
    HistoryStockStatusRow,
    PriceFactorRow,
    StockBasicRow,
    TradeCalendarRow,
    normalize_code_list,
    to_ch_date,
    to_yyyymmdd,
)
from info_data import InfoDataSyncProvider


logger = logging.getLogger(__name__)


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
        self._connected = False
        self._calendar_cache: dict[str, list] = {}

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
            self._calendar_cache.clear()


class AmazingDataSDKProvider(BaseDataSyncProvider, InfoDataSyncProvider):
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
        frame = _ensure_dataframe(self.session.base.get_code_info(security_type=security_type), "get_code_info")
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

        snapshot_date = self.session.get_snapshot_date()
        if start_date is not None and snapshot_date < start_date:
            return
        if end_date is not None and snapshot_date > end_date:
            return

        frame = _ensure_dataframe(self.session.info.get_stock_basic(normalized_codes), "get_stock_basic")
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

    def fetch_history_stock_status(
        self,
        code_list: Sequence[str],
        start_date=None,
        end_date=None,
    ) -> Iterable[HistoryStockStatusRow]:
        normalized_codes = normalize_code_list(code_list)
        if not normalized_codes:
            return

        kwargs: dict[str, Any] = {
            "code_list": normalized_codes,
            "local_path": self.config.local_path,
            "is_local": False,
        }
        if start_date is not None:
            kwargs["begin_date"] = to_yyyymmdd(start_date)
        if end_date is not None:
            kwargs["end_date"] = to_yyyymmdd(end_date)

        frame = _ensure_dataframe(self.session.info.get_history_stock_status(**kwargs), "get_history_stock_status")
        for record in _frame_to_records(frame):
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


def _frame_to_records(frame):
    return frame.to_dict("records")


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
