# AmazingData API 接口文档

## 概述

本 API 提供了直接调用 AmazingData 数据接口的功能，支持所有 47 个数据接口的实时调用。

**基础 URL**: `http://localhost:8000`

## 接口列表

### 1. 直接调用 AmazingData 方法

**POST** `/api/amazingdata/call`

直接调用 AmazingData 的任意方法获取实时数据。

#### 请求参数

```json
{
  "method": "方法名",
  "parameters": {
    "参数名1": "参数值1",
    "参数名2": "参数值2"
  }
}
```

#### 响应格式

```json
{
  "code": 200,
  "message": "调用成功",
  "data": "返回的数据",
  "total": "数据条数"
}
```

### 2. 获取可用方法列表

**GET** `/api/amazingdata/methods`

获取所有可用的 AmazingData 方法及其说明。

#### 响应示例

```json
{
  "code": 200,
  "message": "success",
  "data": {
    "基础数据": [
      {
        "name": "get_code_list",
        "description": "获取股票代码列表"
      }
    ],
    "行情数据": [...],
    "财务数据": [...]
  },
  "total": 47
}
```

## 使用示例

### 1. 获取股票代码列表

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

### 2. 获取股票基础信息

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

### 3. 获取K线数据

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

### 4. 获取财务数据

```bash
curl -X POST "http://localhost:8000/api/amazingdata/call" \
  -H "Content-Type: application/json" \
  -d '{
    "method": "get_balance_sheet",
    "parameters": {
      "code_list": ["000001.SZ"],
      "is_local": false
    }
  }'
```

### 5. 获取交易日历

```bash
curl -X POST "http://localhost:8000/api/amazingdata/call" \
  -H "Content-Type: application/json" \
  -d '{
    "method": "get_calendar",
    "parameters": {
      "data_type": "str",
      "market": "SH"
    }
  }'
```

## 支持的方法分类

### 基础数据 (5个方法)
- `get_code_list` - 获取代码列表
- `get_calendar` - 获取交易日历
- `get_stock_basic` - 获取股票基础信息
- `get_backward_factor` - 获取后复权因子
- `get_adj_factor` - 获取前复权因子

### 行情数据 (2个方法)
- `query_kline` - 获取K线数据
- `query_snapshot` - 获取快照数据

### 财务数据 (5个方法)
- `get_balance_sheet` - 资产负债表
- `get_cash_flow` - 现金流量表
- `get_income` - 利润表
- `get_profit_express` - 业绩快报
- `get_profit_notice` - 业绩预告

### 股东数据 (3个方法)
- `get_share_holder` - 十大股东
- `get_holder_num` - 股东户数
- `get_equity_structure` - 股本结构

### 其他数据 (8个方法)
- `get_margin_summary` - 融资融券汇总
- `get_margin_detail` - 融资融券明细
- `get_long_hu_bang` - 龙虎榜
- `get_block_trading` - 大宗交易
- `get_equity_pledge_freeze` - 股权质押冻结
- `get_equity_restricted` - 限售股解禁
- `get_dividend` - 分红送股
- `get_right_issue` - 配股

### 指数数据 (2个方法)
- `get_index_constituent` - 指数成分股
- `get_index_weight` - 指数权重

### 行业数据 (4个方法)
- `get_industry_base_info` - 行业基本信息
- `get_industry_constituent` - 行业成分股
- `get_industry_weight` - 行业权重
- `get_industry_daily` - 行业日线

### 可转债数据 (8个方法)
- `get_kzz_issuance` - 可转债发行
- `get_kzz_share` - 可转债份额
- `get_kzz_conv` - 可转债转股
- `get_kzz_conv_change` - 可转债转股变动
- `get_kzz_corr` - 可转债修正
- `get_kzz_call` - 可转债赎回
- `get_kzz_put` - 可转债回售
- `get_kzz_suspend` - 可转债停复牌

### ETF数据 (3个方法)
- `get_etf_pcf` - ETF申赎数据
- `get_fund_share` - 基金份额
- `get_fund_iopv` - 基金IOPV

### 期权数据 (3个方法)
- `get_option_basic_info` - 期权基本资料
- `get_option_std_ctr_specs` - 期权标准合约
- `get_option_mon_ctr_specs` - 期权月合约

### 国债数据 (1个方法)
- `get_treasury_yield` - 国债收益率

## 常用参数说明

### 通用参数
- `code_list`: 股票代码列表，如 `["000001.SZ", "000002.SZ"]`
- `security_type`: 证券类型
  - `"EXTRA_STOCK_A"` - A股
  - `"EXTRA_INDEX"` - 指数
  - `"EXTRA_ETF"` - ETF
  - `"EXTRA_KZZ"` - 可转债
  - `"EXTRA_ETF_OP"` - ETF期权
- `is_local`: 是否使用本地缓存，`true`/`false`

### 时间参数
- `begin_date`: 开始日期，整数格式如 `20240101`
- `end_date`: 结束日期，整数格式如 `20241231`
- `period`: K线周期
  - `1` - 1分钟
  - `5` - 5分钟
  - `15` - 15分钟
  - `30` - 30分钟
  - `60` - 1小时
  - `1440` - 日线
  - `10080` - 周线
  - `43200` - 月线

### 市场参数
- `market`: 市场代码
  - `"SH"` - 上海
  - `"SZ"` - 深圳

## 错误处理

### 错误响应格式

```json
{
  "detail": "错误描述"
}
```

### 常见错误

- `400 Bad Request`: 方法不存在或参数错误
- `500 Internal Server Error`: 服务器内部错误或 AmazingData 连接失败

## Python 客户端示例

```python
import requests
import json

# API 基础 URL
base_url = "http://localhost:8000"

def call_amazingdata(method, parameters=None):
    """调用 AmazingData 方法"""
    url = f"{base_url}/api/amazingdata/call"
    data = {
        "method": method,
        "parameters": parameters or {}
    }
    
    response = requests.post(url, json=data)
    return response.json()

# 示例：获取股票代码列表
result = call_amazingdata("get_code_list", {
    "security_type": "EXTRA_STOCK_A"
})
print(f"获取到 {result['total']} 只股票")

# 示例：获取K线数据
result = call_amazingdata("query_kline", {
    "code_list": ["000001.SZ"],
    "begin_date": 20240101,
    "end_date": 20241231,
    "period": 1440
})
print(f"获取到K线数据: {len(result['data']['000001.SZ'])} 条")
```

## JavaScript 客户端示例

```javascript
// 调用 AmazingData 方法
async function callAmazingData(method, parameters = {}) {
    const response = await fetch('http://localhost:8000/api/amazingdata/call', {
        method: 'POST',
        headers: {
            'Content-Type': 'application/json',
        },
        body: JSON.stringify({
            method: method,
            parameters: parameters
        })
    });
    
    return await response.json();
}

// 示例：获取股票基础信息
callAmazingData('get_stock_basic', {
    code_list: ['000001.SZ', '000002.SZ']
}).then(result => {
    console.log('股票基础信息:', result.data);
});
```

## 注意事项

1. **连接状态**: API 会自动管理 AmazingData 连接，首次调用时会自动连接
2. **数据格式**: 返回的 DataFrame 会自动转换为 JSON 格式
3. **缓存机制**: 支持 `is_local` 参数控制是否使用本地缓存
4. **并发限制**: 建议控制并发请求数量，避免对 AmazingData 服务造成压力
5. **错误重试**: API 内部已实现重试机制，但建议客户端也实现适当的重试逻辑

## 性能建议

1. **批量查询**: 尽量使用 `code_list` 批量查询多个股票，而不是单独查询
2. **时间范围**: 合理设置时间范围，避免查询过大的数据集
3. **本地缓存**: 对于不经常变化的数据（如基础信息），建议使用 `is_local: true`
4. **分页查询**: 对于大量数据，建议分批次查询

## 更新日志

- **v1.0.0**: 初始版本，支持所有 47 个 AmazingData 接口