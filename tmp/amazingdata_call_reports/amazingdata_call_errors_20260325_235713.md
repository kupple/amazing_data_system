# AmazingData Call 接口报错汇总

- URL: `http://192.168.2.32:8000/api/amazingdata/call`
- 总数: `17`
- 成功: `12`
- 失败: `5`

## get_backward_factor

- 标题方法名: `get_backward_factor`
- 状态码: `500`
- 错误信息: `unrecognized index type datetime64[us]`

### 请求体

```json
{
  "method": "get_backward_factor",
  "parameters": {
    "code_list": [
      "000001.SZ",
      "000002.SZ"
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
    "error": "unrecognized index type datetime64[us]"
  },
  "total": 0,
  "page": 1,
  "page_size": 100
}
```

## get_adj_factor

- 标题方法名: `get_adj_factor`
- 状态码: `500`
- 错误信息: `unrecognized index type datetime64[us]`

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

### 响应 JSON

```json
{
  "code": 500,
  "message": "调用失败",
  "data": {
    "error": "unrecognized index type datetime64[us]"
  },
  "total": 0,
  "page": 1,
  "page_size": 100
}
```

## query_kline

- 标题方法名: `query_kline`
- 状态码: `400`
- 错误信息: `方法 query_kline 参数错误: 'NoneType' object cannot be interpreted as an integer`

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
    "period": 1440
  }
}
```

### 响应 JSON

```json
{
  "code": 400,
  "message": "调用失败",
  "data": {
    "error": "方法 query_kline 参数错误: 'NoneType' object cannot be interpreted as an integer"
  },
  "total": 0,
  "page": 1,
  "page_size": 100
}
```

## get_industry_weight

- 标题方法名: `get_industry_weight`
- 状态码: `500`
- 错误信息: `invalid literal for int() with base 10: 'None'`

### 请求体

```json
{
  "method": "get_industry_weight",
  "parameters": {
    "code_list": [
      "851783.SI"
    ],
    "is_local": false
  }
}
```

### 响应 JSON

```json
{
  "code": 500,
  "message": "调用失败",
  "data": {
    "error": "invalid literal for int() with base 10: 'None'"
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
      "851783.SI"
    ],
    "is_local": false
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
