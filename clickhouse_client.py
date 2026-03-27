#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""ClickHouse 连接包装.

这里不直接把 SDK 代码绑死在某个驱动实现上，而是提供一个很薄的包装层：
- 上层 repository 只依赖少量稳定的方法：`command` / `query_rows` / `query_df` / `insert_rows`
- 真正的驱动创建逻辑集中在 `create_clickhouse_client`

当前默认对接 `clickhouse-connect`，并且做成延迟导入：
- 代码可以先被导入和编译
- 真正创建连接时才要求本地已安装驱动
"""

from __future__ import annotations

import os
from dataclasses import dataclass, field
from typing import Any, Mapping, MutableMapping, Optional, Sequence

try:
    import pandas as pd
except Exception:  # pragma: no cover
    pd = None  # type: ignore


@dataclass(frozen=True)
class ClickHouseConfig:
    """ClickHouse 连接配置."""

    host: str
    port: int = 8123
    username: str = "default"
    password: str = ""
    database: str = "default"
    secure: bool = False
    connect_timeout: int = 10
    send_receive_timeout: int = 30
    settings: Mapping[str, Any] = field(default_factory=dict)

    @classmethod
    def from_env(
        cls,
        prefix: str = "CLICKHOUSE_",
        fallback_prefix: str = "DB_",
        fallback_database_env: str = "DB_STARLIGHT",
    ) -> "ClickHouseConfig":
        """从环境变量构造配置.

        约定的环境变量：
        - `CLICKHOUSE_HOST`
        - `CLICKHOUSE_PORT`
        - `CLICKHOUSE_USERNAME`
        - `CLICKHOUSE_PASSWORD`
        - `CLICKHOUSE_DATABASE`
        - `CLICKHOUSE_SECURE`
        """

        host = os.getenv(f"{prefix}HOST", "").strip()
        use_fallback = not host and fallback_prefix
        if use_fallback:
            host = os.getenv(f"{fallback_prefix}HOST", "").strip()
        if not host:
            raise ValueError(f"缺少环境变量 {prefix}HOST")

        secure_value = os.getenv(f"{prefix}SECURE", "false").strip().lower()
        secure = secure_value in {"1", "true", "yes", "on"}

        if use_fallback:
            port = int(os.getenv(f"{fallback_prefix}PORT", "8123"))
            username = os.getenv(f"{fallback_prefix}USER", "default")
            password = os.getenv(f"{fallback_prefix}PASSWORD", "")
            database = os.getenv(f"{prefix}DATABASE", "").strip() or os.getenv(fallback_database_env, "default")
        else:
            port = int(os.getenv(f"{prefix}PORT", "8123"))
            username = os.getenv(f"{prefix}USERNAME", "default")
            password = os.getenv(f"{prefix}PASSWORD", "")
            database = os.getenv(f"{prefix}DATABASE", "default")

        return cls(
            host=host,
            port=port,
            username=username,
            password=password,
            database=database,
            secure=secure,
        )


class ClickHouseConnection:
    """对 `clickhouse-connect` client 的轻量包装."""

    def __init__(self, client: Any) -> None:
        self._client = client

    def command(self, sql: str, parameters: Optional[Mapping[str, Any]] = None) -> Any:
        return self._client.command(sql, parameters=parameters or {})

    def query_rows(self, sql: str, parameters: Optional[Mapping[str, Any]] = None) -> list[tuple[Any, ...]]:
        result = self._client.query(sql, parameters=parameters or {})
        return list(result.result_rows)

    def query_value(self, sql: str, parameters: Optional[Mapping[str, Any]] = None) -> Any:
        rows = self.query_rows(sql, parameters)
        if not rows:
            return None
        first_row = rows[0]
        if not first_row:
            return None
        return first_row[0]

    def query_df(self, sql: str, parameters: Optional[Mapping[str, Any]] = None):
        if pd is None:  # pragma: no cover
            raise RuntimeError("未安装 pandas，无法把 ClickHouse 查询结果转换为 DataFrame。")
        result = self._client.query(sql, parameters=parameters or {})
        return pd.DataFrame(result.result_rows, columns=result.column_names)

    def insert_rows(
        self,
        table: str,
        column_names: Sequence[str],
        rows: Sequence[Sequence[Any]],
    ) -> None:
        if not rows:
            return
        self._client.insert(table=table, data=list(rows), column_names=list(column_names))

    def close(self) -> None:
        close_fn = getattr(self._client, "close", None)
        if callable(close_fn):
            close_fn()


def create_clickhouse_client(config: ClickHouseConfig) -> ClickHouseConnection:
    """创建默认 ClickHouse 连接.

    当前实现使用 `clickhouse-connect`，因为它对 HTTP 协议和 pandas 互转都比较友好。
    """

    try:
        import clickhouse_connect
    except Exception as exc:  # pragma: no cover
        raise RuntimeError(
            "未安装 clickhouse-connect，请先在环境中安装该依赖。"
        ) from exc

    settings: MutableMapping[str, Any] = dict(config.settings)

    client = clickhouse_connect.get_client(
        host=config.host,
        port=config.port,
        username=config.username,
        password=config.password,
        database=config.database,
        secure=config.secure,
        connect_timeout=config.connect_timeout,
        send_receive_timeout=config.send_receive_timeout,
        settings=settings,
    )
    return ClickHouseConnection(client)


__all__ = [
    "ClickHouseConfig",
    "ClickHouseConnection",
    "create_clickhouse_client",
]
