# AmazingData Call 接口报错汇总

- URL: `http://192.168.2.32:8000/api/amazingdata/call`
- 总数: `17`
- 成功: `0`
- 失败: `17`

## get_backward_factor

- 标题方法名: `get_backward_factor`
- 状态码: `None`
- 错误信息: `<urlopen error [Errno 61] Connection refused>`

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

## get_adj_factor

- 标题方法名: `get_adj_factor`
- 状态码: `None`
- 错误信息: `<urlopen error [Errno 61] Connection refused>`

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

## get_bj_code_mapping

- 标题方法名: `get_bj_code_mapping`
- 状态码: `None`
- 错误信息: `<urlopen error [Errno 61] Connection refused>`

### 请求体

```json
{
  "method": "get_bj_code_mapping",
  "parameters": {
    "is_local": true
  }
}
```

## query_kline

- 标题方法名: `query_kline`
- 状态码: `None`
- 错误信息: `<urlopen error [Errno 61] Connection refused>`

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

## get_industry_base_info

- 标题方法名: `get_industry_base_info`
- 状态码: `None`
- 错误信息: `<urlopen error [Errno 61] Connection refused>`

### 请求体

```json
{
  "method": "get_industry_base_info",
  "parameters": {
    "is_local": true
  }
}
```

## get_industry_constituent

- 标题方法名: `get_industry_constituent`
- 状态码: `None`
- 错误信息: `<urlopen error [Errno 61] Connection refused>`

### 请求体

```json
{
  "method": "get_industry_constituent",
  "parameters": {
    "code_list": [
      "851783.SI"
    ],
    "is_local": false
  }
}
```

## get_industry_weight

- 标题方法名: `get_industry_weight`
- 状态码: `None`
- 错误信息: `<urlopen error [Errno 61] Connection refused>`

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

## get_industry_daily

- 标题方法名: `get_industry_daily`
- 状态码: `None`
- 错误信息: `<urlopen error [Errno 61] Connection refused>`

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

## get_kzz_issuance

- 标题方法名: `get_kzz_issuance`
- 状态码: `None`
- 错误信息: `<urlopen error [Errno 61] Connection refused>`

### 请求体

```json
{
  "method": "get_kzz_issuance",
  "parameters": {
    "code_list": [
      "110059.SH"
    ],
    "is_local": true
  }
}
```

## get_kzz_share

- 标题方法名: `get_kzz_share`
- 状态码: `None`
- 错误信息: `<urlopen error [Errno 61] Connection refused>`

### 请求体

```json
{
  "method": "get_kzz_share",
  "parameters": {
    "code_list": [
      "110059.SH"
    ],
    "is_local": true
  }
}
```

## get_kzz_conv

- 标题方法名: `get_kzz_conv`
- 状态码: `None`
- 错误信息: `<urlopen error [Errno 61] Connection refused>`

### 请求体

```json
{
  "method": "get_kzz_conv",
  "parameters": {
    "code_list": [
      "110059.SH"
    ],
    "is_local": true
  }
}
```

## get_kzz_conv_change

- 标题方法名: `get_kzz_conv_change`
- 状态码: `None`
- 错误信息: `<urlopen error [Errno 61] Connection refused>`

### 请求体

```json
{
  "method": "get_kzz_conv_change",
  "parameters": {
    "code_list": [
      "110059.SH"
    ],
    "is_local": true
  }
}
```

## get_kzz_corr

- 标题方法名: `get_kzz_corr`
- 状态码: `None`
- 错误信息: `<urlopen error [Errno 61] Connection refused>`

### 请求体

```json
{
  "method": "get_kzz_corr",
  "parameters": {
    "code_list": [
      "110059.SH"
    ],
    "is_local": true
  }
}
```

## get_kzz_call

- 标题方法名: `get_kzz_call`
- 状态码: `None`
- 错误信息: `<urlopen error [Errno 61] Connection refused>`

### 请求体

```json
{
  "method": "get_kzz_call",
  "parameters": {
    "code_list": [
      "110059.SH"
    ],
    "is_local": true
  }
}
```

## get_kzz_put_call_item

- 标题方法名: `get_kzz_put_call_item`
- 状态码: `None`
- 错误信息: `<urlopen error [Errno 61] Connection refused>`

### 请求体

```json
{
  "method": "get_kzz_put_call_item",
  "parameters": {
    "code_list": [
      "110059.SH"
    ],
    "is_local": false
  }
}
```

## get_option_basic_info

- 标题方法名: `get_option_basic_info`
- 状态码: `None`
- 错误信息: `<urlopen error [Errno 61] Connection refused>`

### 请求体

```json
{
  "method": "get_option_basic_info",
  "parameters": {
    "code_list": [
      "10007254.SH"
    ],
    "is_local": true
  }
}
```

## get_option_std_ctr_specs

- 标题方法名: `get_option_std_ctr_specs`
- 状态码: `None`
- 错误信息: `<urlopen error [Errno 61] Connection refused>`

### 请求体

```json
{
  "method": "get_option_std_ctr_specs",
  "parameters": {
    "code_list": [
      "510050.SH"
    ],
    "is_local": true
  }
}
```
