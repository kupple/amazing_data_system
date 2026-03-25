"""
Starlight 同步共享工具。
"""
from __future__ import annotations

import json
import time
import traceback
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import pandas as pd

from src.common.logger import logger


class StarlightSyncSupport:
    """同步脚本与调度器共用的辅助能力。"""

    _checkpoint_file = (
        Path(__file__).resolve().parents[3]
        / "tmp"
        / "starlight_sync_checkpoints.json"
    )

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

    def _should_skip_table_sync(self, table_name: str, force: bool = False) -> bool:
        """如果该表今天已经同步成功，则跳过。"""
        if force:
            return False

        try:
            status = self.db.get_table_sync_status(table_name)
        except Exception as exc:
            logger.warning(f"读取表同步状态失败，table={table_name}: {exc}")
            return False

        if not isinstance(status, dict) or not status:
            return False

        if status.get("status") not in {"success", "noop"}:
            return False

        last_time = status.get("last_success_time") or status.get("last_sync_time")
        if not last_time:
            return False

        try:
            synced_date = pd.to_datetime(last_time).date()
        except Exception:
            return False

        today = datetime.now().date()
        if synced_date == today:
            logger.info(f"跳过 {table_name}，今日已同步完成")
            return True
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
        long_df["date"] = pd.to_datetime(long_df["date"], errors="coerce").dt.strftime("%Y-%m-%d")
        return long_df.dropna(subset=["date"])

    @staticmethod
    def _chunk(items: List[str], batch_size: int) -> List[List[str]]:
        return [items[i:i + batch_size] for i in range(0, len(items), batch_size)]

    def _load_checkpoints(self) -> Dict[str, int]:
        try:
            if self._checkpoint_file.exists():
                return json.loads(self._checkpoint_file.read_text(encoding="utf-8"))
        except Exception as exc:
            logger.warning(f"读取同步 checkpoint 失败: {exc}")
        return {}

    def _save_checkpoints(self, checkpoints: Dict[str, int]):
        try:
            self._checkpoint_file.parent.mkdir(parents=True, exist_ok=True)
            self._checkpoint_file.write_text(
                json.dumps(checkpoints, ensure_ascii=False, indent=2),
                encoding="utf-8",
            )
        except Exception as exc:
            logger.warning(f"写入同步 checkpoint 失败: {exc}")

    def _set_checkpoint(self, checkpoint_key: str, next_batch_index: int):
        checkpoints = self._load_checkpoints()
        checkpoints[checkpoint_key] = next_batch_index
        self._save_checkpoints(checkpoints)

    def _clear_checkpoint(self, checkpoint_key: Optional[str]):
        if not checkpoint_key:
            return
        checkpoints = self._load_checkpoints()
        if checkpoint_key in checkpoints:
            del checkpoints[checkpoint_key]
            self._save_checkpoints(checkpoints)

    def _iter_batches(
        self,
        items: List[str],
        batch_size: int = 1,
        checkpoint_key: Optional[str] = None,
    ) -> List[Tuple[int, List[str]]]:
        batches = self._chunk(items, batch_size)
        start_index = 0
        if checkpoint_key:
            start_index = int(self._load_checkpoints().get(checkpoint_key, 0) or 0)
            if start_index > 0:
                logger.info(f"从 checkpoint 恢复 {checkpoint_key}，跳过前 {start_index} 个批次")
        return list(enumerate(batches[start_index:], start=start_index))

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
