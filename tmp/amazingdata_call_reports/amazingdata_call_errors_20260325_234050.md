# AmazingData Call 接口报错汇总

- URL: `http://100.93.115.99:8000/api/amazingdata/call`
- 总数: `53`
- 成功: `0`
- 失败: `53`

## get_code_list

- 标题方法名: `get_code_list`
- 状态码: `502`
- 错误信息: `HTTP 502`

### 请求体

```json
{
  "method": "get_code_list",
  "parameters": {
    "security_type": "EXTRA_STOCK_A"
  }
}
```

## get_code_info

- 标题方法名: `get_code_info`
- 状态码: `502`
- 错误信息: `HTTP 502`

### 请求体

```json
{
  "method": "get_code_info",
  "parameters": {
    "security_type": "EXTRA_STOCK_A"
  }
}
```

## get_calendar

- 标题方法名: `get_calendar`
- 状态码: `502`
- 错误信息: `HTTP 502`

### 请求体

```json
{
  "method": "get_calendar",
  "parameters": {
    "data_type": "str",
    "market": "SH"
  }
}
```

## get_stock_basic

- 标题方法名: `get_stock_basic`
- 状态码: `502`
- 错误信息: `HTTP 502`

### 请求体

```json
{
  "method": "get_stock_basic",
  "parameters": {
    "code_list": [
      "000001.SZ",
      "000002.SZ",
      "600000.SH"
    ]
  }
}
```

## get_backward_factor

- 标题方法名: `get_backward_factor`
- 状态码: `502`
- 错误信息: `HTTP 502`

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
- 状态码: `502`
- 错误信息: `HTTP 502`

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
- 状态码: `502`
- 错误信息: `HTTP 502`

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
- 状态码: `502`
- 错误信息: `HTTP 502`

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

## get_bj_code_mapping

- 标题方法名: `get_bj_code_mapping`
- 状态码: `502`
- 错误信息: `HTTP 502`

### 请求体

```json
{
  "method": "get_bj_code_mapping",
  "parameters": {
    "is_local": true
  }
}
```

## get_future_code_list

- 标题方法名: `get_future_code_list`
- 状态码: `502`
- 错误信息: `HTTP 502`

### 请求体

```json
{
  "method": "get_future_code_list",
  "parameters": {
    "security_type": "EXTRA_FUTURE"
  }
}
```

## get_option_code_list

- 标题方法名: `get_option_code_list`
- 状态码: `502`
- 错误信息: `HTTP 502`

### 请求体

```json
{
  "method": "get_option_code_list",
  "parameters": {
    "security_type": "EXTRA_ETF_OP"
  }
}
```

## query_snapshot

- 标题方法名: `query_snapshot`
- 状态码: `502`
- 错误信息: `HTTP 502`

### 请求体

```json
{
  "method": "query_snapshot",
  "parameters": {
    "code_list": [
      "000001.SZ"
    ],
    "begin_date": 20240101,
    "end_date": 20240131
  }
}
```

## query_kline

- 标题方法名: `query_kline`
- 状态码: `502`
- 错误信息: `HTTP 502`

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

## get_balance_sheet

- 标题方法名: `get_balance_sheet`
- 状态码: `502`
- 错误信息: `HTTP 502`

### 请求体

```json
{
  "method": "get_balance_sheet",
  "parameters": {
    "code_list": [
      "000001.SZ"
    ],
    "is_local": true,
    "begin_date": 20230101,
    "end_date": 20241231
  }
}
```

## get_cash_flow

- 标题方法名: `get_cash_flow`
- 状态码: `502`
- 错误信息: `HTTP 502`

### 请求体

```json
{
  "method": "get_cash_flow",
  "parameters": {
    "code_list": [
      "000001.SZ"
    ],
    "is_local": true,
    "begin_date": 20230101,
    "end_date": 20241231
  }
}
```

## get_income

- 标题方法名: `get_income`
- 状态码: `502`
- 错误信息: `HTTP 502`

### 请求体

```json
{
  "method": "get_income",
  "parameters": {
    "code_list": [
      "000001.SZ"
    ],
    "is_local": true,
    "begin_date": 20230101,
    "end_date": 20241231
  }
}
```

## get_profit_express

- 标题方法名: `get_profit_express`
- 状态码: `502`
- 错误信息: `HTTP 502`

### 请求体

```json
{
  "method": "get_profit_express",
  "parameters": {
    "code_list": [
      "000001.SZ"
    ],
    "is_local": true,
    "begin_date": 20230101,
    "end_date": 20241231
  }
}
```

## get_profit_notice

- 标题方法名: `get_profit_notice`
- 状态码: `502`
- 错误信息: `HTTP 502`

### 请求体

```json
{
  "method": "get_profit_notice",
  "parameters": {
    "code_list": [
      "000001.SZ"
    ],
    "is_local": true,
    "begin_date": 20230101,
    "end_date": 20241231
  }
}
```

## get_share_holder

- 标题方法名: `get_share_holder`
- 状态码: `502`
- 错误信息: `HTTP 502`

### 请求体

```json
{
  "method": "get_share_holder",
  "parameters": {
    "code_list": [
      "000001.SZ"
    ],
    "is_local": true,
    "begin_date": 20230101,
    "end_date": 20241231
  }
}
```

## get_holder_num

- 标题方法名: `get_holder_num`
- 状态码: `502`
- 错误信息: `HTTP 502`

### 请求体

```json
{
  "method": "get_holder_num",
  "parameters": {
    "code_list": [
      "000001.SZ"
    ],
    "is_local": true,
    "begin_date": 20230101,
    "end_date": 20241231
  }
}
```

## get_equity_structure

- 标题方法名: `get_equity_structure`
- 状态码: `502`
- 错误信息: `HTTP 502`

### 请求体

```json
{
  "method": "get_equity_structure",
  "parameters": {
    "code_list": [
      "000001.SZ"
    ],
    "is_local": true,
    "begin_date": 20230101,
    "end_date": 20241231
  }
}
```

## get_equity_pledge_freeze

- 标题方法名: `get_equity_pledge_freeze`
- 状态码: `502`
- 错误信息: `HTTP 502`

### 请求体

```json
{
  "method": "get_equity_pledge_freeze",
  "parameters": {
    "code_list": [
      "000001.SZ"
    ],
    "is_local": true,
    "begin_date": 20230101,
    "end_date": 20241231
  }
}
```

## get_equity_restricted

- 标题方法名: `get_equity_restricted`
- 状态码: `502`
- 错误信息: `HTTP 502`

### 请求体

```json
{
  "method": "get_equity_restricted",
  "parameters": {
    "code_list": [
      "000001.SZ"
    ],
    "is_local": true,
    "begin_date": 20230101,
    "end_date": 20241231
  }
}
```

## get_dividend

- 标题方法名: `get_dividend`
- 状态码: `502`
- 错误信息: `HTTP 502`

### 请求体

```json
{
  "method": "get_dividend",
  "parameters": {
    "code_list": [
      "000001.SZ"
    ],
    "is_local": true,
    "begin_date": 20230101,
    "end_date": 20241231
  }
}
```

## get_right_issue

- 标题方法名: `get_right_issue`
- 状态码: `502`
- 错误信息: `HTTP 502`

### 请求体

```json
{
  "method": "get_right_issue",
  "parameters": {
    "code_list": [
      "000001.SZ"
    ],
    "is_local": true,
    "begin_date": 20230101,
    "end_date": 20241231
  }
}
```

## get_margin_summary

- 标题方法名: `get_margin_summary`
- 状态码: `502`
- 错误信息: `HTTP 502`

### 请求体

```json
{
  "method": "get_margin_summary",
  "parameters": {
    "is_local": true,
    "begin_date": 20240101,
    "end_date": 20241231
  }
}
```

## get_margin_detail

- 标题方法名: `get_margin_detail`
- 状态码: `502`
- 错误信息: `HTTP 502`

### 请求体

```json
{
  "method": "get_margin_detail",
  "parameters": {
    "code_list": [
      "000001.SZ"
    ],
    "is_local": true,
    "begin_date": 20240101,
    "end_date": 20241231
  }
}
```

## get_long_hu_bang

- 标题方法名: `get_long_hu_bang`
- 状态码: `502`
- 错误信息: `HTTP 502`

### 请求体

```json
{
  "method": "get_long_hu_bang",
  "parameters": {
    "code_list": [
      "000001.SZ"
    ],
    "is_local": true,
    "begin_date": 20240101,
    "end_date": 20241231
  }
}
```

## get_block_trading

- 标题方法名: `get_block_trading`
- 状态码: `502`
- 错误信息: `HTTP 502`

### 请求体

```json
{
  "method": "get_block_trading",
  "parameters": {
    "code_list": [
      "000001.SZ"
    ],
    "is_local": true,
    "begin_date": 20240101,
    "end_date": 20241231
  }
}
```

## get_index_constituent

- 标题方法名: `get_index_constituent`
- 状态码: `502`
- 错误信息: `HTTP 502`

### 请求体

```json
{
  "method": "get_index_constituent",
  "parameters": {
    "code_list": [
      "000300.SH"
    ],
    "is_local": true
  }
}
```

## get_index_weight

- 标题方法名: `get_index_weight`
- 状态码: `502`
- 错误信息: `HTTP 502`

### 请求体

```json
{
  "method": "get_index_weight",
  "parameters": {
    "code_list": [
      "000300.SH"
    ],
    "is_local": true,
    "begin_date": 20240101,
    "end_date": 20241231
  }
}
```

## get_industry_base_info

- 标题方法名: `get_industry_base_info`
- 状态码: `502`
- 错误信息: `HTTP 502`

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
- 状态码: `502`
- 错误信息: `HTTP 502`

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
- 状态码: `502`
- 错误信息: `HTTP 502`

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
- 状态码: `502`
- 错误信息: `HTTP 502`

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
- 状态码: `502`
- 错误信息: `HTTP 502`

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
- 状态码: `502`
- 错误信息: `HTTP 502`

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
- 状态码: `502`
- 错误信息: `HTTP 502`

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
- 状态码: `502`
- 错误信息: `HTTP 502`

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
- 状态码: `502`
- 错误信息: `HTTP 502`

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
- 状态码: `502`
- 错误信息: `HTTP 502`

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

## get_kzz_put

- 标题方法名: `get_kzz_put`
- 状态码: `502`
- 错误信息: `HTTP 502`

### 请求体

```json
{
  "method": "get_kzz_put",
  "parameters": {
    "code_list": [
      "110059.SH"
    ],
    "is_local": true
  }
}
```

## get_kzz_suspend

- 标题方法名: `get_kzz_suspend`
- 状态码: `502`
- 错误信息: `HTTP 502`

### 请求体

```json
{
  "method": "get_kzz_suspend",
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
- 状态码: `502`
- 错误信息: `HTTP 502`

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

## get_kzz_put_explanation

- 标题方法名: `get_kzz_put_explanation`
- 状态码: `502`
- 错误信息: `HTTP 502`

### 请求体

```json
{
  "method": "get_kzz_put_explanation",
  "parameters": {
    "code_list": [
      "110059.SH"
    ],
    "is_local": false
  }
}
```

## get_kzz_call_explanation

- 标题方法名: `get_kzz_call_explanation`
- 状态码: `502`
- 错误信息: `HTTP 502`

### 请求体

```json
{
  "method": "get_kzz_call_explanation",
  "parameters": {
    "code_list": [
      "110059.SH"
    ],
    "is_local": false
  }
}
```

## get_etf_pcf

- 标题方法名: `get_etf_pcf`
- 状态码: `502`
- 错误信息: `HTTP 502`

### 请求体

```json
{
  "method": "get_etf_pcf",
  "parameters": {
    "code_list": [
      "510300.SH"
    ]
  }
}
```

## get_fund_share

- 标题方法名: `get_fund_share`
- 状态码: `502`
- 错误信息: `HTTP 502`

### 请求体

```json
{
  "method": "get_fund_share",
  "parameters": {
    "code_list": [
      "510300.SH"
    ],
    "is_local": true,
    "begin_date": 20240101,
    "end_date": 20241231
  }
}
```

## get_fund_iopv

- 标题方法名: `get_fund_iopv`
- 状态码: `502`
- 错误信息: `HTTP 502`

### 请求体

```json
{
  "method": "get_fund_iopv",
  "parameters": {
    "code_list": [
      "510300.SH"
    ],
    "is_local": true,
    "begin_date": 20240101,
    "end_date": 20241231
  }
}
```

## get_option_basic_info

- 标题方法名: `get_option_basic_info`
- 状态码: `502`
- 错误信息: `HTTP 502`

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
- 状态码: `502`
- 错误信息: `HTTP 502`

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

## get_option_mon_ctr_specs

- 标题方法名: `get_option_mon_ctr_specs`
- 状态码: `502`
- 错误信息: `HTTP 502`

### 请求体

```json
{
  "method": "get_option_mon_ctr_specs",
  "parameters": {
    "code_list": [
      "10007254.SH"
    ],
    "is_local": true
  }
}
```

## get_treasury_yield

- 标题方法名: `get_treasury_yield`
- 状态码: `502`
- 错误信息: `HTTP 502`

### 请求体

```json
{
  "method": "get_treasury_yield",
  "parameters": {
    "term_list": [
      "m3",
      "m6",
      "y1",
      "y3",
      "y5",
      "y10"
    ],
    "is_local": true,
    "begin_date": 20240101,
    "end_date": 20241231
  }
}
```
