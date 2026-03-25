# AmazingData API 使用指南

## 快速开始

### 1. 环境准备

```bash
# 安装依赖
pip install -r requirements.txt

# 配置环境变量
cp .env.example .env
# 编辑 .env 文件，填入你的 AmazingData 账号信息
```

### 2. 启动 API 服务

```bash
# 直接启动 API 服务
python src/services/api.py
```

### 3. 验证服务状态

访问 http://localhost:8000/health 检查服务是否正常运行。

### 4. 查看 API 文档

访问 http://localhost:8000/docs 查看完整的 API 文档。

## 测试工具

### 1. Python 测试脚本

```bash
python test_amazingdata_api.py
```

这个脚本会自动测试主要的 API 功能，包括：
- 获取可用方法列表
- 获取股票代码列表
- 获取交易日历
- 获取股票基础信息
- 获取K线数据
- 获取财务数据

### 2. HTML 测试页面

打开 `amazingdata_api_test.html` 文件，可以在浏览器中交互式测试 API。

## 主要接口

### 1. 调用 AmazingData 方法

**POST** `/api/amazingdata/call`

```json
{
  "method": "get_code_list",
  "parameters": {
    "security_type": "EXTRA_STOCK_A"
  }
}
```

### 2. 获取可用方法

**GET** `/api/amazingdata/methods`

返回所有可用的 AmazingData 方法，按类别分组。

## 常用示例

### 获取A股代码列表

```bash
curl -X POST "http://localhost:8000/api/amazingdata/call" \
  -H "Content-Type: application/json" \
  -d '{
    "method": "get_code_list",
    "parameters": {
      "security_type": "EXTRA_STOCK_A"
    }
  }'
```

### 获取股票基础信息

```bash
curl -X POST "http://localhost:8000/api/amazingdata/call" \
  -H "Content-Type: application/json" \
  -d '{
    "method": "get_stock_basic",
    "parameters": {
      "code_list": ["000001.SZ", "000002.SZ"]
    }
  }'
```

### 获取K线数据

```bash
curl -X POST "http://localhost:8000/api/amazingdata/call" \
  -H "Content-Type: application/json" \
  -d '{
    "method": "query_kline",
    "parameters": {
      "code_list": ["000001.SZ"],
      "begin_date": 20240101,
      "end_date": 20241231,
      "period": 1440
    }
  }'
```

## Python 客户端示例

```python
import requests

def call_amazingdata(method, parameters=None):
    """调用 AmazingData API"""
    url = "http://localhost:8000/api/amazingdata/call"
    data = {
        "method": method,
        "parameters": parameters or {}
    }
    response = requests.post(url, json=data)
    return response.json()

# 获取股票代码列表
result = call_amazingdata("get_code_list", {
    "security_type": "EXTRA_STOCK_A"
})
print(f"获取到 {len(result['data'])} 只股票")

# 获取K线数据
result = call_amazingdata("query_kline", {
    "code_list": ["000001.SZ"],
    "begin_date": 20240101,
    "end_date": 20241231,
    "period": 1440
})
print(f"获取到K线数据")
```

## 支持的数据类型

### 证券类型 (security_type)
- `EXTRA_STOCK_A` - A股
- `EXTRA_INDEX` - 指数
- `EXTRA_ETF` - ETF
- `EXTRA_KZZ` - 可转债
- `EXTRA_ETF_OP` - ETF期权
- `EXTRA_FUTURE` - 期货

### K线周期 (period)
- `1` - 1分钟
- `5` - 5分钟
- `15` - 15分钟
- `30` - 30分钟
- `60` - 1小时
- `1440` - 日线
- `10080` - 周线
- `43200` - 月线

### 市场代码 (market)
- `SH` - 上海
- `SZ` - 深圳

## 注意事项

1. **连接状态**: API 会自动管理 AmazingData 连接
2. **数据格式**: 返回的 DataFrame 会自动转换为 JSON
3. **缓存机制**: 支持 `is_local` 参数控制本地缓存
4. **错误处理**: API 包含完整的错误处理和重试机制
5. **并发限制**: 建议控制并发请求数量

## 故障排除

### 1. 服务无法启动
- 检查端口 8000 是否被占用
- 检查配置文件是否正确
- 检查 AmazingData 客户端配置

### 2. 连接失败
- 检查 AmazingData 用户名和密码
- 检查网络连接
- 查看日志文件获取详细错误信息

### 3. 数据获取失败
- 检查方法名是否正确
- 检查参数格式是否正确
- 检查股票代码格式（如 000001.SZ）

## 更多信息

详细的 API 文档请参考 `AMAZINGDATA_API_DOCS.md` 文件。