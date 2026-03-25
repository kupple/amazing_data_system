# Sync_starlight.py 参数错误修复

## 发现的问题

### 1. get_calendar 方法参数错误
**错误**: 传递了 `security_type="EXTRA_STOCK_A"` 参数
**正确**: 应该传递 `data_type="str", market="SH"` 参数
**修复**: 已修复

### 2. 日期处理中的 None 值问题
**错误**: `get_latest_date()` 可能返回 `None`，导致后续的日期计算失败
**症状**: `'NoneType' object cannot be interpreted as an integer`
**修复**: 
- 添加了 `None` 值检查
- 添加了日期格式异常处理
- 添加了日期参数验证

### 3. 字符串 "None" 的处理
**错误**: 数据库可能返回字符串 "None" 而不是 Python 的 `None`
**修复**: 添加了 `latest_date != "None"` 检查

## 修复的代码位置

### sync_basic_data() 方法
```python
# 修复前
calendar = self.client.get_calendar(security_type="EXTRA_STOCK_A")

# 修复后  
calendar = self.client.get_calendar(data_type="str", market="SH")
```

### sync_kline_data() 方法
```python
# 修复前
if latest_date:
    default_start_date = datetime.strptime(latest_date[:10], "%Y-%m-%d") - timedelta(days=1)

# 修复后
if latest_date and latest_date != "None":
    try:
        default_start_date = datetime.strptime(latest_date[:10], "%Y-%m-%d") - timedelta(days=1)
        logger.info(f"✓ 增量同步模式，从最新日期 {latest_date[:10]} 开始")
    except (ValueError, TypeError):
        default_start_date = end_date - timedelta(days=30)
        logger.info("⚠ 最新日期格式异常，默认获取最近30天数据")
```

### 添加了日期参数验证
```python
# 验证日期参数
if not begin_date_int or not end_date_int:
    logger.error(f"日期参数无效: begin_date_int={begin_date_int}, end_date_int={end_date_int}")
    return
```

## 测试建议

1. 先运行基础数据同步测试：
   ```bash
   python test_sync_basic.py
   ```

2. 如果基础数据同步成功，再测试K线数据：
   ```bash
   python sync_starlight.py --basic
   python sync_starlight.py --kline
   ```

3. 逐步测试其他数据类型：
   ```bash
   python sync_starlight.py --financial
   python sync_starlight.py --holder
   python sync_starlight.py --other
   ```

## 预期结果

修复后应该解决以下错误：
- ✅ `cannot access local variable 'datetime' where it is not associated with a value`
- ✅ `AmazingDataClient.get_hist_code_list() missing 1 required positional argument: 'security_type'`
- ✅ `'NoneType' object cannot be interpreted as an integer`
- ✅ `Unrecognized column '000001.SZ' in table backward_factor`

## 其他改进

1. 添加了更详细的错误日志和异常处理
2. 改进了日期范围的智能判断逻辑
3. 添加了参数验证，防止无效值传递给API
4. 统一了所有数据同步方法的错误处理模式