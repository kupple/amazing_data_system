# AmazingData API 全部48个接口详细文档

## 概述

本文档详细介绍了 AmazingData API 支持的所有48个接口方法，包含完整的参数说明和调用示例。

**API 基础地址**: `http://localhost:8000`

**调用接口**: `POST /api/amazingdata/call`

## 接口分类

### 1. 基础数据接口 (9个)
### 2. 行情数据接口 (2个)
### 3. 财务数据接口 (5个)
### 4. 股东数据接口 (7个)
### 5. 其他数据接口 (4个)
### 6. 指数数据接口 (2个)
### 7. 行业数据接口 (4个)
### 8. 可转债数据接口 (11个)
### 9. ETF数据接口 (3个)
### 10. 期权数据接口 (5个)
### 11. 国债数据接口 (1个)

---

## 1. 基础数据接口 (9个)

### 1.1 get_code_list - 获取代码列表

**功能**: 获取指定证券类型的代码列表

**参数**:
- `security_type` (str): 证券类型

**证券类型说明**:
- `EXTRA_STOCK_A`: A股
- `EXTRA_INDEX`: 指数
- `EXTRA_ETF`: ETF
- `EXTRA_KZZ`: 可转债
- `EXTRA_ETF_OP`: ETF期权
- `EXTRA_FUTURE`: 期货

**示例**:
```json
{
  "method": "get_code_list",
  "parameters": {
    "security_type": "EXTRA_STOCK_A"
  }
}
```

**返回**: 股票代码列表 `["000001.SZ", "000002.SZ", ...]`

---

### 1.2 get_code_info - 获取代码信息

**功能**: 获取指定证券类型的详细代码信息

**参数**:
- `security_type` (str): 证券类型

**示例**:
```json
{
  "method": "get_code_info",
  "parameters": {
    "security_type": "EXTRA_STOCK_A"
  }
}
```

**返回**: DataFrame 包含代码、名称、上市日期等信息

---

### 1.3 get_calendar - 获取交易日历

**功能**: 获取交易日历信息

**参数**:
- `data_type` (str): 数据类型，默认 "str"
- `market` (str): 市场代码，"SH"(上海) 或 "SZ"(深圳)

**示例**:
```json
{
  "method": "get_calendar",
  "parameters": {
    "data_type": "str",
    "market": "SH"
  }
}
```

**返回**: 交易日期列表 `["20240101", "20240102", ...]`

---

### 1.4 get_stock_basic - 获取股票基础信息

**功能**: 获取股票的基础信息

**参数**:
- `code_list` (List[str]): 股票代码列表

**示例**:
```json
{
  "method": "get_stock_basic",
  "parameters": {
    "code_list": ["000001.SZ", "000002.SZ", "600000.SH"]
  }
}
```

**返回**: DataFrame 包含股票名称、行业、市值等基础信息

---

### 1.5 get_backward_factor - 获取后复权因子

**功能**: 获取股票的后复权因子

**参数**:
- `code_list` (List[str]): 股票代码列表
- `is_local` (bool): 是否使用本地缓存，默认 true

**示例**:
```json
{
  "method": "get_backward_factor",
  "parameters": {
    "code_list": ["000001.SZ", "000002.SZ"],
    "is_local": true
  }
}
```

**返回**: DataFrame 包含复权因子数据

---