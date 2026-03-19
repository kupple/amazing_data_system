# Baostock API 使用文档

## 启动服务

```bash
cd C:\Users\mubin\Desktop\amazing_data_system
python baostock_api.py
```

服务启动后访问：http://localhost:8000

## API 接口列表

### 1. 根路径
```
GET /
```
返回服务状态

### 2. 健康检查
```
GET /health
```

### 3. 获取统计信息
```
GET /api/stats
```
返回：
- stock_count: 股票总数
- kline_count: 日线数据总条数
- kline_stock_count: 有日线数据的股票数
- max_date: 最新日期

### 4. 获取股票列表
```
GET /api/stocks
```
参数：
- stock_type: 股票类型 (1=A股, 2=B股)
- limit: 返回条数 (默认100)
- offset: 偏移量 (默认0)

### 5. 获取单个股票信息
```
GET /api/stocks/{sec_code}
```
示例：`GET /api/stocks/600000.SH`

### 6. 获取日线数据
```
GET /api/kline/{sec_code}
```
参数：
- start_date: 开始日期 (YYYY-MM-DD)
- end_date: 结束日期 (YYYY-MM-DD)
- limit: 返回条数 (默认100)
- offset: 偏移量 (默认0)

示例：
```
GET /api/kline/600000.SH?start_date=2025-01-01&end_date=2025-12-31&limit=100
```

### 7. 获取所有表
```
GET /api/tables
```

## 响应格式

```json
{
  "code": 200,
  "message": "success",
  "data": [...],
  "total": 1000,
  "page": 1,
  "page_size": 100
}
```

## 错误响应

```json
{
  "detail": "错误信息"
}
```

## 示例代码

### Python 请求示例
```python
import requests

# 获取统计
resp = requests.get("http://localhost:8000/api/stats")
print(resp.json())

# 获取日线
resp = requests.get("http://localhost:8000/api/kline/600000.SH?limit=10")
data = resp.json()["data"]
for row in data:
    print(row["trade_date"], row["close"])
```

### curl 请求示例
```bash
# 获取统计
curl http://localhost:8000/api/stats

# 获取日线数据
curl "http://localhost:8000/api/kline/600000.SH?start_date=2025-01-01&limit=10"

# 获取股票列表
curl "http://localhost:8000/api/stocks?stock_type=1&limit=10"
```

## 端口

默认端口：**8000**
