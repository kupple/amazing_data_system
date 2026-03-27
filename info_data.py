#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""AmazingData 风格的 InfoData 实现.

当前已实现两个接口：
- `get_stock_basic`
- `get_history_stock_status`
"""

from __future__ import annotations

import hashlib
import logging
from datetime import date, datetime
from typing import Callable, Iterable, Optional, Protocol, Sequence

from amazingdata_constants import HISTORY_STOCK_STATUS_FIELDS, STOCK_BASIC_FIELDS, SyncStatus
from base_data import BaseDataCacheMissError, BaseDataParameterError
from clickhouse_client import ClickHouseConfig, create_clickhouse_client
from clickhouse_tables import AD_HISTORY_STOCK_STATUS_TABLE, AD_STOCK_BASIC_TABLE
from data_models import (
    HistoryStockStatusQuery,
    HistoryStockStatusRow,
    StockBasicQuery,
    StockBasicRow,
    SyncTaskLogRow,
    normalize_code_list,
    to_ch_date,
    utcnow,
)
from repositories.info_data_repository import InfoDataRepository


logger = logging.getLogger(__name__)


class InfoDataSyncProvider(Protocol):
    """InfoData 远端同步协议."""

    def fetch_stock_basic(
        self,
        code_list: Sequence[str],
        start_date: Optional[date] = None,
        end_date: Optional[date] = None,
    ) -> Iterable[StockBasicRow]:
        ...

    def fetch_history_stock_status(
        self,
        code_list: Sequence[str],
        start_date: Optional[date] = None,
        end_date: Optional[date] = None,
    ) -> Iterable[HistoryStockStatusRow]:
        ...


class InfoData:
    """InfoData SDK 兼容层."""

    def __init__(
        self,
        repository: InfoDataRepository,
        sync_provider: Optional[InfoDataSyncProvider] = None,
    ) -> None:
        self.repository = repository
        self.sync_provider = sync_provider

    @classmethod
    def from_clickhouse_config(
        cls,
        config: ClickHouseConfig,
        sync_provider: Optional[InfoDataSyncProvider] = None,
        ensure_tables: bool = True,
        insert_batch_size: int = 5000,
    ) -> "InfoData":
        connection = create_clickhouse_client(config)
        repository = InfoDataRepository(connection, insert_batch_size=insert_batch_size)
        instance = cls(repository=repository, sync_provider=sync_provider)
        if ensure_tables:
            instance.ensure_tables()
        return instance

    def ensure_tables(self) -> None:
        self.repository.ensure_tables()

    def close(self) -> None:
        self.repository.client.close()

    def get_stock_basic(self, code_list: Sequence[str]):
        """获取证券基础信息.

        SDK 返回列名保持文档原始大写风格；
        数据库存储字段统一使用小写 snake_case。
        """

        normalized_codes = normalize_code_list(code_list)
        if not normalized_codes:
            raise BaseDataParameterError("code_list 不能为空。")

        self.sync_stock_basic(normalized_codes, force=False)
        frame = self.repository.load_stock_basic_frame(StockBasicQuery(code_list=tuple(normalized_codes)))
        if frame.empty:
            raise BaseDataCacheMissError(
                f"未找到 code_count={len(normalized_codes)} 的 stock_basic 数据。"
            )
        return frame.loc[:, list(STOCK_BASIC_FIELDS)]

    def get_history_stock_status(
        self,
        code_list: Sequence[str],
        local_path: str,
        is_local: bool = True,
        begin_date: Optional[int] = None,
        end_date: Optional[int] = None,
    ):
        """获取历史证券状态信息.

        SDK 返回列名保持文档原始大写风格；
        数据库存储字段统一使用小写 snake_case。
        """

        normalized_codes = normalize_code_list(code_list)
        if not normalized_codes:
            raise BaseDataParameterError("code_list 不能为空。")
        self._validate_local_path(local_path)

        begin = to_ch_date(begin_date) if begin_date is not None else None
        end = to_ch_date(end_date) if end_date is not None else None
        self._validate_optional_date_range(begin, end)

        # 这里对日期 + code 的接口统一先尝试增量同步。
        # 即使 is_local=True，也会先做“当天成功跳过 + 增量补数”的同步判断。
        self.sync_history_stock_status(
            code_list=normalized_codes,
            begin_date=begin,
            end_date=end,
            force=False,
        )

        query = HistoryStockStatusQuery(
            code_list=tuple(normalized_codes),
            local_path=local_path,
            is_local=is_local,
            begin_date=begin,
            end_date=end,
        )
        frame = self.repository.load_history_stock_status_frame(query)
        if frame.empty:
            raise BaseDataCacheMissError(
                f"未找到 code_count={len(normalized_codes)} 的 history_stock_status 数据。"
            )
        return frame.loc[:, list(HISTORY_STOCK_STATUS_FIELDS)]

    def sync_stock_basic(self, code_list: Sequence[str], force: bool = False) -> int:
        """同步 `ad_stock_basic`."""

        normalized_codes = normalize_code_list(code_list)
        if not normalized_codes:
            raise BaseDataParameterError("code_list 不能为空。")

        scope_key = self._build_code_scope_key("get_stock_basic", normalized_codes)
        latest_date = self.repository.load_sync_checkpoint_date("get_stock_basic", scope_key)
        return self._run_sync_job(
            task_name="get_stock_basic",
            scope_key=scope_key,
            target_table=AD_STOCK_BASIC_TABLE,
            latest_date=latest_date,
            fetch_rows=lambda start_date: self._provider_fetch_stock_basic(normalized_codes, start_date),
            save_rows=self.repository.save_stock_basic_rows,
            row_date_getter=lambda row: row.snapshot_date,
            force=force,
        )

    def sync_history_stock_status(
        self,
        code_list: Sequence[str],
        begin_date: Optional[date | int | str] = None,
        end_date: Optional[date | int | str] = None,
        force: bool = False,
    ) -> int:
        """同步 `ad_history_stock_status`."""

        normalized_codes = normalize_code_list(code_list)
        if not normalized_codes:
            raise BaseDataParameterError("code_list 不能为空。")

        begin = to_ch_date(begin_date) if begin_date is not None else None
        end = to_ch_date(end_date) if end_date is not None else None
        self._validate_optional_date_range(begin, end)

        scope_key = self._build_code_scope_key(
            "get_history_stock_status",
            normalized_codes,
            begin_date=begin,
            end_date=end,
        )
        latest_date = self.repository.load_sync_checkpoint_date("get_history_stock_status", scope_key)
        sync_start = self._resolve_incremental_start_date(latest_date=latest_date, requested_begin_date=begin)
        return self._run_sync_job(
            task_name="get_history_stock_status",
            scope_key=scope_key,
            target_table=AD_HISTORY_STOCK_STATUS_TABLE,
            latest_date=latest_date,
            fetch_rows=lambda _latest_date: self._provider_fetch_history_stock_status(
                normalized_codes,
                start_date=sync_start,
                end_date=end,
            ),
            save_rows=self.repository.save_history_stock_status_rows,
            row_date_getter=lambda row: row.trade_date,
            force=force,
        )

    def _run_sync_job(
        self,
        task_name: str,
        scope_key: str,
        target_table: str,
        latest_date: Optional[date],
        fetch_rows: Callable[[Optional[date]], Iterable[object]],
        save_rows: Callable[[Iterable[object]], int],
        row_date_getter: Callable[[object], Optional[date]],
        force: bool,
    ) -> int:
        run_date = datetime.now().date()
        started_at = utcnow()

        if not force and self.repository.has_successful_sync_today(task_name, scope_key, run_date):
            message = f"{task_name} 已在 {run_date} 同步成功，本次跳过。"
            logger.info(message)
            self._write_sync_log(
                task_name=task_name,
                scope_key=scope_key,
                run_date=run_date,
                status=SyncStatus.SKIPPED,
                target_table=target_table,
                start_date=latest_date,
                end_date=latest_date,
                row_count=0,
                message=message,
                started_at=started_at,
                finished_at=utcnow(),
            )
            return 0

        if self.sync_provider is None:
            message = f"{task_name} 未配置 sync_provider，无法执行同步。"
            logger.warning(message)
            self._write_sync_log(
                task_name=task_name,
                scope_key=scope_key,
                run_date=run_date,
                status=SyncStatus.FAILED,
                target_table=target_table,
                start_date=latest_date,
                end_date=latest_date,
                row_count=0,
                message=message,
                started_at=started_at,
                finished_at=utcnow(),
            )
            return 0

        stats = {
            "row_count": 0,
            "min_date": None,
            "max_date": latest_date,
        }

        def tracked_rows() -> Iterable[object]:
            for row in fetch_rows(latest_date):
                stats["row_count"] += 1
                row_date = row_date_getter(row)
                if row_date is not None:
                    if stats["min_date"] is None or row_date < stats["min_date"]:
                        stats["min_date"] = row_date
                    if stats["max_date"] is None or row_date > stats["max_date"]:
                        stats["max_date"] = row_date
                yield row

        logger.info(
            "Start sync task=%s scope=%s target_table=%s latest_date=%s",
            task_name,
            scope_key,
            target_table,
            latest_date,
        )

        try:
            inserted_count = save_rows(tracked_rows())
        except Exception as exc:
            message = f"{task_name} 同步失败: {exc}"
            logger.exception(message)
            self._write_sync_log(
                task_name=task_name,
                scope_key=scope_key,
                run_date=run_date,
                status=SyncStatus.FAILED,
                target_table=target_table,
                start_date=latest_date or stats["min_date"],
                end_date=stats["max_date"],
                row_count=stats["row_count"],
                message=message,
                started_at=started_at,
                finished_at=utcnow(),
            )
            raise

        message = (
            f"{task_name} 同步完成，"
            f"latest_date={latest_date}, inserted_rows={inserted_count}, observed_rows={stats['row_count']}"
        )
        logger.info(message)
        self._write_sync_log(
            task_name=task_name,
            scope_key=scope_key,
            run_date=run_date,
            status=SyncStatus.SUCCESS,
            target_table=target_table,
            start_date=latest_date or stats["min_date"],
            end_date=stats["max_date"],
            row_count=inserted_count,
            message=message,
            started_at=started_at,
            finished_at=utcnow(),
        )
        return inserted_count

    def _provider_fetch_stock_basic(self, code_list: Sequence[str], start_date: Optional[date]):
        return self.sync_provider.fetch_stock_basic(  # type: ignore[union-attr]
            code_list=code_list,
            start_date=start_date,
        )

    def _provider_fetch_history_stock_status(
        self,
        code_list: Sequence[str],
        start_date: Optional[date],
        end_date: Optional[date],
    ):
        return self.sync_provider.fetch_history_stock_status(  # type: ignore[union-attr]
            code_list=code_list,
            start_date=start_date,
            end_date=end_date,
        )

    def _write_sync_log(
        self,
        task_name: str,
        scope_key: str,
        run_date: date,
        status: str,
        target_table: str,
        start_date: Optional[date],
        end_date: Optional[date],
        row_count: int,
        message: str,
        started_at: datetime,
        finished_at: datetime,
    ) -> None:
        try:
            self.repository.insert_sync_log(
                SyncTaskLogRow(
                    task_name=task_name,
                    scope_key=scope_key,
                    run_date=run_date,
                    status=status,
                    target_table=target_table,
                    start_date=start_date,
                    end_date=end_date,
                    row_count=max(0, int(row_count)),
                    message=message,
                    started_at=started_at,
                    finished_at=finished_at,
                )
            )
        except Exception:
            logger.exception("写入同步日志失败 task=%s scope=%s status=%s", task_name, scope_key, status)

    @staticmethod
    def _build_code_scope_key(
        task_name: str,
        code_list: Sequence[str],
        begin_date: Optional[date] = None,
        end_date: Optional[date] = None,
    ) -> str:
        digest = hashlib.sha1(",".join(sorted(code_list)).encode("utf-8")).hexdigest()[:12]
        begin_text = begin_date.isoformat() if begin_date is not None else ""
        end_text = end_date.isoformat() if end_date is not None else ""
        return (
            f"task={task_name}|code_count={len(code_list)}|codes_sha1={digest}"
            f"|begin_date={begin_text}|end_date={end_text}"
        )

    @staticmethod
    def _resolve_incremental_start_date(
        latest_date: Optional[date],
        requested_begin_date: Optional[date],
    ) -> Optional[date]:
        if latest_date is None:
            return requested_begin_date
        if requested_begin_date is None:
            return latest_date
        return max(latest_date, requested_begin_date)

    @staticmethod
    def _validate_optional_date_range(begin_date: Optional[date], end_date: Optional[date]) -> None:
        if begin_date is not None and end_date is not None and begin_date > end_date:
            raise BaseDataParameterError("begin_date 不能大于 end_date。")

    @staticmethod
    def _validate_local_path(local_path: str) -> None:
        if not isinstance(local_path, str):
            raise BaseDataParameterError("local_path 必须是字符串。")


__all__ = [
    "InfoData",
    "InfoDataSyncProvider",
]
