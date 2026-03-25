"""
Starlight 同步共享工具。
"""
from __future__ import annotations

import json
import time
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

    def _fetch_by_code_batches(
        self,
        method,
        code_list: List[str],
        batch_size: int = 1,
        sleep_seconds: float = 0.0,
        checkpoint_key: Optional[str] = None,
        **kwargs,
    ):
        merged_dict = None
        merged_frames = []
        batches = self._iter_batches(code_list, batch_size=batch_size, checkpoint_key=checkpoint_key)

        for batch_index, batch_codes in batches:
            try:
                result = method(code_list=batch_codes, **kwargs)
            except Exception as exc:
                logger.warning(f"批量请求失败，codes={batch_codes}: {exc}")
                break

            if isinstance(result, dict):
                if merged_dict is None:
                    merged_dict = {}
                for key, value in result.items():
                    if value is not None:
                        merged_dict[key] = value
            elif isinstance(result, pd.DataFrame):
                if not result.empty:
                    merged_frames.append(result)

            if checkpoint_key:
                self._set_checkpoint(checkpoint_key, batch_index + 1)
            if sleep_seconds > 0:
                time.sleep(sleep_seconds)

        if checkpoint_key:
            completed_batches = len(self._chunk(code_list, batch_size))
            current = self._load_checkpoints().get(checkpoint_key, 0)
            if current >= completed_batches:
                self._clear_checkpoint(checkpoint_key)

        if merged_dict is not None:
            return merged_dict
        if merged_frames:
            return pd.concat(merged_frames, ignore_index=True)
        return pd.DataFrame()
