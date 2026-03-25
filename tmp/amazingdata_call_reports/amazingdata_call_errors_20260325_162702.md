# AmazingData Call 接口报错汇总

- URL: `http://100.93.115.99:8000/api/amazingdata/call`
- 总数: `23`
- 成功: `13`
- 失败: `10`

## get_adj_factor

- 标题方法名: `get_adj_factor`
- 状态码: `None`
- 错误信息: `timed out`

### 请求体

```json
{
  "method": "get_adj_factor",
  "parameters": {
    "code_list": [
      "000001.SZ",
      "000002.SZ"
    ],
    "is_local": true
  }
}
```

## get_hist_code_list

- 标题方法名: `get_hist_code_list`
- 状态码: `None`
- 错误信息: `timed out`

### 请求体

```json
{
  "method": "get_hist_code_list",
  "parameters": {
    "security_type": "EXTRA_STOCK_A",
    "start_date": 20240101,
    "end_date": 20241231
  }
}
```

## get_history_stock_status

- 标题方法名: `get_history_stock_status`
- 状态码: `None`
- 错误信息: `timed out`

### 请求体

```json
{
  "method": "get_history_stock_status",
  "parameters": {
    "code_list": [
      "000001.SZ",
      "000002.SZ"
    ],
    "is_local": true,
    "begin_date": 20240101,
    "end_date": 20241231
  }
}
```

## query_snapshot

- 标题方法名: `query_snapshot`
- 状态码: `None`
- 错误信息: `timed out`

### 请求体

```json
{
  "method": "query_snapshot",
  "parameters": {
    "code_list": [
      "000001.SZ"
    ],
    "begin_date": 20240101,
    "end_date": 20240131,
    "begin_time": 93000000,
    "end_time": 150000000
  }
}
```

## query_kline

- 标题方法名: `query_kline`
- 状态码: `None`
- 错误信息: `timed out`

### 请求体

```json
{
  "method": "query_kline",
  "parameters": {
    "code_list": [
      "000001.SZ"
    ],
    "begin_date": 20240101,
    "end_date": 20240131,
    "period": 1440,
    "begin_time": 0,
    "end_time": 0
  }
}
```

## get_industry_weight

- 标题方法名: `get_industry_weight`
- 状态码: `500`
- 错误信息: `查询失败`

### 请求体

```json
{
  "method": "get_industry_weight",
  "parameters": {
    "code_list": [
      "CI005001.WI"
    ],
    "is_local": true,
    "begin_date": 20240101,
    "end_date": 20241231
  }
}
```

### 响应 JSON

```json
{
  "code": 500,
  "message": "调用失败",
  "data": {
    "error": "查询失败"
  },
  "total": 0,
  "page": 1,
  "page_size": 100
}
```

## get_industry_daily

- 标题方法名: `get_industry_daily`
- 状态码: `500`
- 错误信息: `查询失败`

### 请求体

```json
{
  "method": "get_industry_daily",
  "parameters": {
    "code_list": [
      "CI005001.WI"
    ],
    "is_local": true,
    "begin_date": 20240101,
    "end_date": 20241231
  }
}
```

### 响应 JSON

```json
{
  "code": 500,
  "message": "调用失败",
  "data": {
    "error": "查询失败"
  },
  "total": 0,
  "page": 1,
  "page_size": 100
}
```

## get_kzz_put_call_item

- 标题方法名: `get_kzz_put_call_item`
- 状态码: `500`
- 错误信息: `type object 'LocalDataFolder' has no attribute 'Kzz_Put_call_item'`

### 请求体

```json
{
  "method": "get_kzz_put_call_item",
  "parameters": {
    "code_list": [
      "110059.SH"
    ],
    "is_local": true
  }
}
```

### 响应 JSON

```json
{
  "code": 500,
  "message": "调用失败",
  "data": {
    "error": "type object 'LocalDataFolder' has no attribute 'Kzz_Put_call_item'"
  },
  "total": 0,
  "page": 1,
  "page_size": 100
}
```

## get_kzz_put_explanation

- 标题方法名: `get_kzz_put_explanation`
- 状态码: `500`
- 错误信息: `type object 'LocalDataFolder' has no attribute 'Kzz_Put_explanation'`

### 请求体

```json
{
  "method": "get_kzz_put_explanation",
  "parameters": {
    "code_list": [
      "110059.SH"
    ],
    "is_local": true
  }
}
```

### 响应 JSON

```json
{
  "code": 500,
  "message": "调用失败",
  "data": {
    "error": "type object 'LocalDataFolder' has no attribute 'Kzz_Put_explanation'"
  },
  "total": 0,
  "page": 1,
  "page_size": 100
}
```

## get_kzz_call_explanation

- 标题方法名: `get_kzz_call_explanation`
- 状态码: `500`
- 错误信息: `type object 'LocalDataFolder' has no attribute 'Kzz_Call_explanation'`

### 请求体

```json
{
  "method": "get_kzz_call_explanation",
  "parameters": {
    "code_list": [
      "110059.SH"
    ],
    "is_local": true
  }
}
```

### 响应 JSON

```json
{
  "code": 500,
  "message": "调用失败",
  "data": {
    "error": "type object 'LocalDataFolder' has no attribute 'Kzz_Call_explanation'"
  },
  "total": 0,
  "page": 1,
  "page_size": 100
}
```
