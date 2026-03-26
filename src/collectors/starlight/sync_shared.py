"""
Starlight 同步共享工具。
"""
from __future__ import annotations

import time
import traceback
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple

import pandas as pd

from src.common.logger import logger


class StarlightSyncSupport:
    """同步脚本与调度器共用的辅助能力。"""

    @staticmethod
    def _lowercase_columns(df: pd.DataFrame) -> pd.DataFrame:
        normalized = df.copy()
        normalized.columns = [str(col).lower() for col in normalized.columns]
        return normalized

    @staticmethod
    def _find_existing_column(df: pd.DataFrame, candidates: List[str]) -> str:
        for column in candidates:
            if column in df.columns:
                return column
        raise KeyError(f"未找到可用列，候选列: {candidates}")

    @staticmethod
    def _unwrap_retry_method(method):
        wrapped = getattr(method, "__wrapped__", None)
        bound_self = getattr(method, "__self__", None)
        if wrapped is not None and bound_self is not None:
            return wrapped.__get__(bound_self, type(bound_self))
        return method

    def _call_client_method(self, method, **kwargs):
        raw_method = self._unwrap_retry_method(method)
        return raw_method(**kwargs)

    def _log_sync_error(
        self,
        scope: str,
        error: Exception,
        method_name: Optional[str] = None,
        table_name: Optional[str] = None,
        batch_codes: Optional[List[str]] = None,
    ):
        try:
            self.db.save_sync_error(
                scope=scope,
                method_name=method_name,
                table_name=table_name,
                batch_codes=batch_codes,
                error_message=str(error),
                traceback_text=traceback.format_exc(),
            )
        except Exception as log_exc:
            logger.warning(f"写入同步错误日志失败: {log_exc}")

    def _mark_table_success(self, table_name: str, date_column: Optional[str] = None):
        self.db.update_table_sync_status(
            table_name=table_name,
            success=True,
            date_column=date_column,
            status="success",
        )

    def _mark_table_failed(self, table_name: str, error: Exception, date_column: Optional[str] = None):
        self.db.update_table_sync_status(
            table_name=table_name,
            success=False,
            date_column=date_column,
            status="failed",
            error_message=str(error),
        )

    def _get_latest_date_with_fallback(self, table_name: str, candidates: List[str]) -> Optional[str]:
        for column in candidates:
            try:
                latest_date = self.db.get_latest_date(table_name, column)
            except Exception:
                latest_date = None
            if latest_date and latest_date != "None":
                return latest_date
        return None

    def _build_incremental_date_range(
        self,
        table_name: str,
        date_columns: List[str],
        first_sync_days: int = 365,
        fallback_days: int = 30,
    ) -> Dict[str, int]:
        end_date = datetime.now()
        if not self.db.table_exists(table_name):
            start_date = end_date - timedelta(days=first_sync_days)
        else:
            latest_date = self._get_latest_date_with_fallback(table_name, date_columns)
            if latest_date:
                try:
                    start_date = datetime.strptime(str(latest_date)[:10], "%Y-%m-%d") - timedelta(days=1)
                except (TypeError, ValueError):
                    start_date = end_date - timedelta(days=fallback_days)
            else:
                start_date = end_date - timedelta(days=fallback_days)

        return {
            "begin_date": int(start_date.strftime("%Y%m%d")),
            "end_date": int(end_date.strftime("%Y%m%d")),
        }

    def _build_incremental_date_range_for_value(
        self,
        table_name: str,
        date_column: str,
        key_column: str,
        key_value: str,
        first_sync_days: int = 365,
        fallback_days: int = 30,
    ) -> Dict[str, int]:
        """按单个分组键生成增量日期范围。"""
        end_date = datetime.now()
        latest_date = self.db.get_latest_date_for_value(
            table_name=table_name,
            date_column=date_column,
            key_column=key_column,
            key_value=key_value,
        )
        if latest_date:
            try:
                start_date = datetime.strptime(str(latest_date)[:10], "%Y-%m-%d") - timedelta(days=1)
            except (TypeError, ValueError):
                start_date = end_date - timedelta(days=fallback_days)
        elif self.db.table_exists(table_name):
            start_date = end_date - timedelta(days=fallback_days)
        else:
            start_date = end_date - timedelta(days=first_sync_days)

        return {
            "begin_date": int(start_date.strftime("%Y%m%d")),
            "end_date": int(end_date.strftime("%Y%m%d")),
        }

    def _should_skip_table_sync(
        self,
        table_name: str,
        force: bool = False,
        checkpoint_keys: Optional[List[str]] = None,
    ) -> bool:
        """如果该表今天已经同步成功，则跳过。"""
        if force:
            return False
        try:
            if self.db.has_table_sync_success_today(table_name):
                logger.info(f"跳过 {table_name}，今日已同步完成")
                return True
        except Exception as exc:
            logger.warning(f"读取表同步状态失败，table={table_name}: {exc}")
            return False
        return False

    @staticmethod
    def _reshape_factor_dataframe(df: pd.DataFrame, value_column: str) -> pd.DataFrame:
        if df is None or df.empty:
            return pd.DataFrame(columns=["date", "code", value_column])

        normalized = df.copy()
        normalized.index = pd.to_datetime(normalized.index, errors="coerce")
        normalized = normalized[~normalized.index.isna()]
        normalized.index.name = "date"
        normalized.columns = [str(col) for col in normalized.columns]

        long_df = normalized.stack().rename(value_column).reset_index()
        long_df.columns = ["date", "code", value_column]
        long_df["date"] = pd.to_datetime(long_df["date"], errors="coerce").dt.normalize()
        return long_df.dropna(subset=["date"])

    @staticmethod
    def _chunk(items: List[str], batch_size: int) -> List[List[str]]:
        return [items[i:i + batch_size] for i in range(0, len(items), batch_size)]

    def _load_checkpoints(self) -> Dict[str, int]:
        return {}

    def _save_checkpoints(self, checkpoints: Dict[str, int]):
        return

    def _set_checkpoint(self, checkpoint_key: str, next_batch_index: int):
        return

    def _clear_checkpoint(self, checkpoint_key: Optional[str]):
        return

    def _has_active_checkpoint(self, checkpoint_keys: List[str]) -> bool:
        return False

    def _iter_batches(
        self,
        items: List[str],
        batch_size: int = 1,
        checkpoint_key: Optional[str] = None,
    ) -> List[Tuple[int, List[str]]]:
        batches = self._chunk(items, batch_size)
        return list(enumerate(batches, start=0))

    def _iter_code_batch_results(
        self,
        method,
        code_list: List[str],
        batch_size: int = 1,
        sleep_seconds: float = 0.0,
        checkpoint_key: Optional[str] = None,
        **kwargs,
    ):
        """按批请求并逐批返回结果，由调用方决定如何落库。"""
        batches = self._iter_batches(code_list, batch_size=batch_size, checkpoint_key=checkpoint_key)
        raw_method = self._unwrap_retry_method(method)
        for batch_index, batch_codes in batches:
            try:
                result = raw_method(code_list=batch_codes, **kwargs)
            except Exception as exc:
                logger.warning(f"批量请求失败，codes={batch_codes}: {exc}")
                self._log_sync_error(
                    scope="batch_request",
                    error=exc,
                    method_name=getattr(method, "__name__", str(method)),
                    batch_codes=batch_codes,
                )
                if len(batch_codes) <= 1:
                    raise
                logger.info(f"批量请求降级为单股票重试，原批次大小={len(batch_codes)}")
                for code in batch_codes:
                    try:
                        single_result = raw_method(code_list=[code], **kwargs)
                    except Exception as single_exc:
                        logger.warning(f"单股票请求失败，code={code}: {single_exc}")
                        self._log_sync_error(
                            scope="single_request",
                            error=single_exc,
                            method_name=getattr(method, "__name__", str(method)),
                            batch_codes=[code],
                        )
                        continue
                    yield batch_index, [code], single_result
                continue

            yield batch_index, batch_codes, result

            if sleep_seconds > 0:
                time.sleep(sleep_seconds)

    def _sync_grouped_table_by_key(
        self,
        method,
        keys: List[str],
        table_name: str,
        key_column: str,
        key_columns: List[str],
        request_arg: str = "code_list",
        date_column: Optional[str] = None,
        is_local: bool = True,
        first_sync_days: int = 365,
        fallback_days: int = 30,
        sleep_seconds: float = 0.0,
        extra_kwargs: Optional[Dict] = None,
    ) -> int:
        """按单个 key 逐次请求并逐次落库。"""
        total_rows = 0
        extra_kwargs = extra_kwargs or {}

        for key in keys:
            kwargs = dict(extra_kwargs)
            kwargs[request_arg] = [key]
            kwargs["is_local"] = is_local

            if date_column:
                date_range = self._build_incremental_date_range_for_value(
                    table_name=table_name,
                    date_column=date_column,
                    key_column=key_column,
                    key_value=key,
                    first_sync_days=first_sync_days,
                    fallback_days=fallback_days,
                )
                kwargs["begin_date"] = date_range["begin_date"]
                kwargs["end_date"] = date_range["end_date"]

            result = self._call_client_method(method, **kwargs)

            if isinstance(result, dict):
                for returned_key, df in result.items():
                    if df is None or df.empty:
                        continue
                    df = self._lowercase_columns(df)
                    if key_column not in df.columns:
                        df[key_column] = returned_key
                    self.db.incremental_update(
                        table_name,
                        df,
                        key_columns=key_columns,
                        date_column=date_column,
                    )
                    total_rows += len(df)
            elif isinstance(result, pd.DataFrame) and not result.empty:
                result = self._lowercase_columns(result)
                if key_column and key_column not in result.columns:
                    result[key_column] = key
                self.db.incremental_update(
                    table_name,
                    result,
                    key_columns=key_columns,
                    date_column=date_column,
                )
                total_rows += len(result)

            if sleep_seconds > 0:
                time.sleep(sleep_seconds)

        return total_rows
