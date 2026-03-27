# AmazingData Financial DB Runbook

## 当前正式同步入口

- 日线 K 线：`python run_sync.py daily_kline`
- 历史快照：`python run_sync.py market_snapshot`

默认行为：

- 默认读取项目根目录 `.env`
- 默认只同步 `EXTRA_STOCK_A`
- 默认起始日期：`20100101`
- 默认结束日期：最新交易日
- 默认按单只股票顺序同步

## 当前主表

- `ad_code_info`
- `ad_stock_basic`
- `ad_history_stock_status`
- `ad_market_kline_daily`
- `ad_market_snapshot`
- `ad_sync_task_log`
- `ad_sync_checkpoint`

说明：

- 业务表只保留业务字段
- `ad_sync_task_log` 记录同步日志
- `ad_sync_checkpoint` 记录断点和当天成功状态

## 当前增量逻辑

### code_info / code_list

- 先看 `ad_sync_checkpoint` 里今天是否成功
- 成功则直接从 ClickHouse 读取
- 否则先同步 `get_code_info`
- 再从 ClickHouse 投影出代码池

### daily_kline

- 先获取 A 股代码池
- 按单只股票逐个同步
- 每只股票同步前先查 `ad_market_kline_daily` 中该股票最新日期
- 如果没有历史数据，则从传入 `begin_date` 开始

### market_snapshot

- 先获取 A 股代码池
- 按单只股票逐个同步
- 断点和当天成功状态记录到 `ad_sync_checkpoint`

## 常用命令

### 小范围验证日线

```bash
python run_sync.py daily_kline --begin-date 20240101 --end-date 20240131 --limit 20 --force --log-level INFO
```

### 小范围验证快照

```bash
python run_sync.py market_snapshot --begin-date 20240115 --end-date 20240115 --limit 20 --force --log-level INFO
```

## 表结构变更后的注意事项

如果改过 ClickHouse 表结构或表名，需要先删除旧表再重建。

推荐按需删除：

```sql
DROP TABLE IF EXISTS ad_code_info;
DROP TABLE IF EXISTS ad_stock_basic;
DROP TABLE IF EXISTS ad_history_stock_status;
DROP TABLE IF EXISTS ad_market_kline_daily;
DROP TABLE IF EXISTS ad_market_snapshot;
DROP TABLE IF EXISTS ad_sync_task_log;
DROP TABLE IF EXISTS ad_sync_checkpoint;
```

如果不再使用历史代码池，也可删除：

```sql
DROP TABLE IF EXISTS ad_hist_code_daily;
```
