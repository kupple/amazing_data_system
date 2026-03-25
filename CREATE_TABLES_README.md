# Starlight 数据表创建脚本

## 功能说明

`create_starlight_tables.py` 是一个自动化脚本，用于在 ClickHouse 中创建 Starlight 数据同步系统所需的所有数据表。

## 功能特性

- ✅ 自动解析 `STARLIGHT_TABLE_DDL.md` 文件
- ✅ 创建 `starlight` 数据库
- ✅ 创建所有 47 个数据表
- ✅ 验证表创建结果
- ✅ 支持 Dry Run 模式（预览不执行）
- ✅ 详细的日志输出

## 使用方法

### 1. 预览模式（推荐先运行）

查看将要创建的表，但不实际执行：

```bash
python create_starlight_tables.py --dry-run
```

输出示例：
```
============================================================
开始创建 Starlight 数据表
============================================================

1. 解析 DDL 文件...
✓ 找到 47 个表定义

表名列表:
   1. stock_codes
   2. trading_calendar
   3. stock_basic
   ...
  47. daily_summary

============================================================
🔍 Dry Run 模式 - 未实际创建表
============================================================
将创建 47 个表

如需实际创建表，请运行:
  python create_starlight_tables.py
```

### 2. 实际创建表

```bash
python create_starlight_tables.py
```

输出示例：
```
============================================================
开始创建 Starlight 数据表
============================================================

1. 解析 DDL 文件...
✓ 找到 47 个表定义

2. 连接数据库...
✓ 已连接到 ClickHouse

3. 创建数据库...
✓ 数据库 starlight 已就绪

4. 创建数据表...
------------------------------------------------------------
  [1/47] ✓ stock_codes
  [2/47] ✓ trading_calendar
  [3/47] ✓ stock_basic
  ...
  [47/47] ✓ daily_summary

============================================================
创建结果汇总
============================================================
总计: 47 个表
成功: 47 个
失败: 0 个

============================================================
验证表创建
============================================================
✓ 数据库中共有 47 个表

前10个表:
  - stock_codes: 0 条记录
  - trading_calendar: 0 条记录
  - stock_basic: 0 条记录
  ...

============================================================
🎉 所有表创建成功！
============================================================
```

### 3. 指定 DDL 文件

如果 DDL 文件不在默认位置：

```bash
python create_starlight_tables.py --ddl-file /path/to/your/ddl.md
```

## 环境要求

### 1. Python 版本
- Python 3.7 或更高版本

### 2. 依赖包
```bash
pip install -r requirements.txt
```

主要依赖：
- `clickhouse-connect` - ClickHouse 客户端
- `pandas` - 数据处理
- `python-dotenv` - 环境变量管理

### 3. 配置文件

确保 `.env` 文件已正确配置：

```bash
# ClickHouse 配置
DB_HOST=100.93.115.99
DB_PORT=8123
DB_USER=default
DB_PASSWORD=your_password

# 数据库名称
DB_STARLIGHT=starlight
```

## 创建的表

脚本将创建以下 47 个表：

### 基础数据表 (5个)
1. stock_codes - 股票代码表
2. trading_calendar - 交易日历
3. stock_basic - 证券基础信息
4. backward_factor - 后复权因子
5. adj_factor - 前复权因子

### 行情数据表 (2个)
6. kline_daily - 日K线数据
7. snapshot - 快照数据

### 财务数据表 (5个)
8. balance_sheet - 资产负债表
9. cash_flow - 现金流量表
10. income - 利润表
11. profit_express - 业绩快报
12. profit_notice - 业绩预告

### 股东数据表 (3个)
13. share_holder - 十大股东
14. holder_num - 股东户数
15. equity_structure - 股本结构

### 其他数据表 (8个)
16. margin_summary - 融资融券汇总
17. margin_detail - 融资融券明细
18. dragon_tiger - 龙虎榜
19. block_trade - 大宗交易
20. equity_pledge_freeze - 股权质押冻结
21. equity_restricted - 限售股解禁
22. dividend - 分红送股
23. right_issue - 配股

### 指数数据表 (2个)
24. index_constituent - 指数成分股
25. index_weight - 指数权重

### 行业数据表 (4个)
26. industry_base_info - 行业基础信息
27. industry_constituent - 行业成分股
28. industry_weight - 行业权重
29. industry_daily - 行业日行情

### 可转债数据表 (8个)
30. kzz_issuance - 可转债发行
31. kzz_share - 可转债余额
32. kzz_conv - 可转债转股
33. kzz_conv_change - 转股价变动
34. kzz_corr - 可转债相关性
35. kzz_call - 可转债赎回
36. kzz_put - 可转债回售
37. kzz_suspend - 可转债停牌

### ETF 数据表 (3个)
38. etf_pcf - ETF申购赎回清单
39. fund_share - 基金份额
40. fund_iopv - 基金IOPV

### 期权数据表 (3个)
41. option_basic_info - 期权基础信息
42. option_std_ctr_specs - 期权标准合约规格
43. option_mon_ctr_specs - 期权月度合约规格

### 国债收益率表 (1个)
44. treasury_yield - 国债收益率

### 系统管理表 (3个)
45. fetch_records - 数据获取记录表
46. sync_status - 同步状态表
47. daily_summary - 每日数据汇总表

## 常见问题

### Q1: 表已存在怎么办？

脚本使用 `CREATE TABLE IF NOT EXISTS`，如果表已存在会跳过，不会报错。

### Q2: 如何删除所有表重新创建？

```sql
-- 连接到 ClickHouse
clickhouse-client --host 100.93.115.99 --port 9000

-- 删除数据库（谨慎操作！）
DROP DATABASE IF EXISTS starlight;

-- 然后重新运行脚本
python create_starlight_tables.py
```

### Q3: 部分表创建失败怎么办？

1. 查看错误日志，了解失败原因
2. 检查 ClickHouse 连接和权限
3. 检查 DDL 语法是否正确
4. 可以手动执行失败的 DDL 语句

### Q4: 如何验证表是否创建成功？

```bash
# 方法1: 使用脚本验证（已内置）
python create_starlight_tables.py

# 方法2: 使用 ClickHouse 客户端
clickhouse-client --host 100.93.115.99 --port 9000
USE starlight;
SHOW TABLES;
```

## 注意事项

1. ⚠️ 确保 ClickHouse 服务正在运行
2. ⚠️ 确保有足够的权限创建数据库和表
3. ⚠️ 首次运行建议使用 `--dry-run` 预览
4. ⚠️ 生产环境操作前请备份数据

## 相关文件

- `STARLIGHT_TABLE_DDL.md` - 表结构定义文件
- `create_starlight_tables.py` - 表创建脚本
- `sync_starlight.py` - 数据同步脚本
- `.env` - 环境配置文件

## 下一步

表创建完成后，可以运行数据同步脚本：

```bash
# 测试同步
python test_starlight_sync.py

# 实际同步
python sync_starlight.py
```

---

**版本**: v1.0  
**创建日期**: 2026-03-25  
**维护者**: AI Assistant
