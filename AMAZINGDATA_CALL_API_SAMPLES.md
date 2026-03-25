# AmazingData Call API 样例文档

本文档对应接口:

```python
@app.post("/api/amazingdata/call")
```

接口地址:

```text
POST /api/amazingdata/call
Content-Type: application/json
```

## 请求格式

```json
{
  "method": "get_code_list",
  "parameters": {
    "security_type": "EXTRA_STOCK_A"
  }
}
```

## method 支持写法

接口内部会自动归一化，以下写法都可以:

```json
{
  "method": "get_stock_basic",
  "parameters": {
    "code_list": ["000001.SZ"]
  }
}
```

```json
{
  "method": "InfoData.get_stock_basic",
  "parameters": {
    "code_list": ["000001.SZ"]
  }
}
```

```json
{
  "method": "ad.InfoData.get_stock_basic",
  "parameters": {
    "code_list": ["000001.SZ"]
  }
}
```

建议优先使用标准写法，也就是直接传方法名，如 `get_stock_basic`。

## 成功返回格式

```json
{
  "code": 200,
  "message": "调用成功",
  "data": [],
  "total": 0,
  "page": 1,
  "page_size": 100
}
```

## 失败返回格式

```json
{
  "code": 400,
  "message": "调用失败",
  "data": {
    "error": "具体错误信息"
  },
  "total": 0,
  "page": 1,
  "page_size": 100
}
```

## 1. 基础数据

### 1.1 get_code_list

```json
{
  "method": "get_code_list",
  "parameters": {
    "security_type": "EXTRA_STOCK_A"
  }
}
```

### 1.2 get_code_info

```json
{
  "method": "get_code_info",
  "parameters": {
    "security_type": "EXTRA_STOCK_A"
  }
}
```

### 1.3 get_calendar

```json
{
  "method": "get_calendar",
  "parameters": {
    "data_type": "str",
    "market": "SH"
  }
}
```

### 1.4 get_stock_basic

```json
{
  "method": "get_stock_basic",
  "parameters": {
    "code_list": ["000001.SZ", "000002.SZ", "600000.SH"]
  }
}
```

### 1.5 get_backward_factor

```json
{
  "method": "get_backward_factor",
  "parameters": {
    "code_list": ["000001.SZ", "000002.SZ"],
    "is_local": true
  }
}
```

### 1.6 get_adj_factor

```json
{
  "method": "get_adj_factor",
  "parameters": {
    "code_list": ["000001.SZ", "000002.SZ"],
    "is_local": true
  }
}
```

### 1.7 get_hist_code_list

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

### 1.8 get_history_stock_status

```json
{
  "method": "get_history_stock_status",
  "parameters": {
    "code_list": ["000001.SZ", "000002.SZ"],
    "is_local": true,
    "begin_date": 20240101,
    "end_date": 20241231
  }
}
```

### 1.9 get_bj_code_mapping

```json
{
  "method": "get_bj_code_mapping",
  "parameters": {
    "is_local": true
  }
}
```

### 1.10 get_future_code_list

```json
{
  "method": "get_future_code_list",
  "parameters": {
    "security_type": "EXTRA_FUTURE"
  }
}
```

### 1.11 get_option_code_list

```json
{
  "method": "get_option_code_list",
  "parameters": {
    "security_type": "EXTRA_ETF_OP"
  }
}
```

## 2. 行情数据

### 2.1 query_snapshot

```json
{
  "method": "query_snapshot",
  "parameters": {
    "code_list": ["000001.SZ"],
    "begin_date": 20240101,
    "end_date": 20240131,
    "begin_time": 93000000,
    "end_time": 150000000
  }
}
```

### 2.2 query_kline

```json
{
  "method": "query_kline",
  "parameters": {
    "code_list": ["000001.SZ"],
    "begin_date": 20240101,
    "end_date": 20240131,
    "period": 1440,
    "begin_time": 0,
    "end_time": 0
  }
}
```

## 3. 财务数据

### 3.1 get_balance_sheet

```json
{
  "method": "get_balance_sheet",
  "parameters": {
    "code_list": ["000001.SZ"],
    "is_local": true,
    "begin_date": 20230101,
    "end_date": 20241231
  }
}
```

### 3.2 get_cash_flow

```json
{
  "method": "get_cash_flow",
  "parameters": {
    "code_list": ["000001.SZ"],
    "is_local": true,
    "begin_date": 20230101,
    "end_date": 20241231
  }
}
```

### 3.3 get_income

```json
{
  "method": "get_income",
  "parameters": {
    "code_list": ["000001.SZ"],
    "is_local": true,
    "begin_date": 20230101,
    "end_date": 20241231
  }
}
```

### 3.4 get_profit_express

```json
{
  "method": "get_profit_express",
  "parameters": {
    "code_list": ["000001.SZ"],
    "is_local": true,
    "begin_date": 20230101,
    "end_date": 20241231
  }
}
```

### 3.5 get_profit_notice

```json
{
  "method": "get_profit_notice",
  "parameters": {
    "code_list": ["000001.SZ"],
    "is_local": true,
    "begin_date": 20230101,
    "end_date": 20241231
  }
}
```

## 4. 股东数据

### 4.1 get_share_holder

```json
{
  "method": "get_share_holder",
  "parameters": {
    "code_list": ["000001.SZ"],
    "is_local": true,
    "begin_date": 20230101,
    "end_date": 20241231
  }
}
```

### 4.2 get_holder_num

```json
{
  "method": "get_holder_num",
  "parameters": {
    "code_list": ["000001.SZ"],
    "is_local": true,
    "begin_date": 20230101,
    "end_date": 20241231
  }
}
```

### 4.3 get_equity_structure

```json
{
  "method": "get_equity_structure",
  "parameters": {
    "code_list": ["000001.SZ"],
    "is_local": true,
    "begin_date": 20230101,
    "end_date": 20241231
  }
}
```

### 4.4 get_equity_pledge_freeze

```json
{
  "method": "get_equity_pledge_freeze",
  "parameters": {
    "code_list": ["000001.SZ"],
    "is_local": true,
    "begin_date": 20230101,
    "end_date": 20241231
  }
}
```

### 4.5 get_equity_restricted

```json
{
  "method": "get_equity_restricted",
  "parameters": {
    "code_list": ["000001.SZ"],
    "is_local": true,
    "begin_date": 20230101,
    "end_date": 20241231
  }
}
```

### 4.6 get_dividend

```json
{
  "method": "get_dividend",
  "parameters": {
    "code_list": ["000001.SZ"],
    "is_local": true,
    "begin_date": 20230101,
    "end_date": 20241231
  }
}
```

### 4.7 get_right_issue

```json
{
  "method": "get_right_issue",
  "parameters": {
    "code_list": ["000001.SZ"],
    "is_local": true,
    "begin_date": 20230101,
    "end_date": 20241231
  }
}
```

## 5. 其他数据

### 5.1 get_margin_summary

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

### 5.2 get_margin_detail

```json
{
  "method": "get_margin_detail",
  "parameters": {
    "code_list": ["000001.SZ"],
    "is_local": true,
    "begin_date": 20240101,
    "end_date": 20241231
  }
}
```

### 5.3 get_long_hu_bang

```json
{
  "method": "get_long_hu_bang",
  "parameters": {
    "code_list": ["000001.SZ"],
    "is_local": true,
    "begin_date": 20240101,
    "end_date": 20241231
  }
}
```

### 5.4 get_block_trading

```json
{
  "method": "get_block_trading",
  "parameters": {
    "code_list": ["000001.SZ"],
    "is_local": true,
    "begin_date": 20240101,
    "end_date": 20241231
  }
}
```

## 6. 指数数据

### 6.1 get_index_constituent

```json
{
  "method": "get_index_constituent",
  "parameters": {
    "code_list": ["000300.SH"],
    "is_local": true
  }
}
```

### 6.2 get_index_weight

```json
{
  "method": "get_index_weight",
  "parameters": {
    "code_list": ["000300.SH"],
    "is_local": true,
    "begin_date": 20240101,
    "end_date": 20241231
  }
}
```

## 7. 行业数据

### 7.1 get_industry_base_info

```json
{
  "method": "get_industry_base_info",
  "parameters": {
    "is_local": true
  }
}
```

### 7.2 get_industry_constituent

```json
{
  "method": "get_industry_constituent",
  "parameters": {
    "code_list": ["CI005001.WI"],
    "is_local": true
  }
}
```

### 7.3 get_industry_weight

```json
{
  "method": "get_industry_weight",
  "parameters": {
    "code_list": ["CI005001.WI"],
    "is_local": true,
    "begin_date": 20240101,
    "end_date": 20241231
  }
}
```

### 7.4 get_industry_daily

```json
{
  "method": "get_industry_daily",
  "parameters": {
    "code_list": ["CI005001.WI"],
    "is_local": true,
    "begin_date": 20240101,
    "end_date": 20241231
  }
}
```

## 8. 可转债数据

### 8.1 get_kzz_issuance

```json
{
  "method": "get_kzz_issuance",
  "parameters": {
    "code_list": ["110059.SH"],
    "is_local": true
  }
}
```

### 8.2 get_kzz_share

```json
{
  "method": "get_kzz_share",
  "parameters": {
    "code_list": ["110059.SH"],
    "is_local": true
  }
}
```

### 8.3 get_kzz_conv

```json
{
  "method": "get_kzz_conv",
  "parameters": {
    "code_list": ["110059.SH"],
    "is_local": true
  }
}
```

### 8.4 get_kzz_conv_change

```json
{
  "method": "get_kzz_conv_change",
  "parameters": {
    "code_list": ["110059.SH"],
    "is_local": true
  }
}
```

### 8.5 get_kzz_corr

```json
{
  "method": "get_kzz_corr",
  "parameters": {
    "code_list": ["110059.SH"],
    "is_local": true
  }
}
```

### 8.6 get_kzz_call

```json
{
  "method": "get_kzz_call",
  "parameters": {
    "code_list": ["110059.SH"],
    "is_local": true
  }
}
```

### 8.7 get_kzz_put

```json
{
  "method": "get_kzz_put",
  "parameters": {
    "code_list": ["110059.SH"],
    "is_local": true
  }
}
```

### 8.8 get_kzz_suspend

```json
{
  "method": "get_kzz_suspend",
  "parameters": {
    "code_list": ["110059.SH"],
    "is_local": true
  }
}
```

### 8.9 get_kzz_put_call_item

```json
{
  "method": "get_kzz_put_call_item",
  "parameters": {
    "code_list": ["110059.SH"],
    "is_local": true
  }
}
```

### 8.10 get_kzz_put_explanation

```json
{
  "method": "get_kzz_put_explanation",
  "parameters": {
    "code_list": ["110059.SH"],
    "is_local": true
  }
}
```

### 8.11 get_kzz_call_explanation

```json
{
  "method": "get_kzz_call_explanation",
  "parameters": {
    "code_list": ["110059.SH"],
    "is_local": true
  }
}
```

## 9. ETF 数据

### 9.1 get_etf_pcf

```json
{
  "method": "get_etf_pcf",
  "parameters": {
    "code_list": ["510300.SH"]
  }
}
```

### 9.2 get_fund_share

```json
{
  "method": "get_fund_share",
  "parameters": {
    "code_list": ["510300.SH"],
    "is_local": true,
    "begin_date": 20240101,
    "end_date": 20241231
  }
}
```

### 9.3 get_fund_iopv

```json
{
  "method": "get_fund_iopv",
  "parameters": {
    "code_list": ["510300.SH"],
    "is_local": true,
    "begin_date": 20240101,
    "end_date": 20241231
  }
}
```

## 10. 期权数据

### 10.1 get_option_basic_info

```json
{
  "method": "get_option_basic_info",
  "parameters": {
    "code_list": ["10007254.SH"],
    "is_local": true
  }
}
```

### 10.2 get_option_std_ctr_specs

```json
{
  "method": "get_option_std_ctr_specs",
  "parameters": {
    "code_list": ["510050.SH"],
    "is_local": true
  }
}
```

### 10.3 get_option_mon_ctr_specs

```json
{
  "method": "get_option_mon_ctr_specs",
  "parameters": {
    "code_list": ["10007254.SH"],
    "is_local": true
  }
}
```

## 11. 国债数据

### 11.1 get_treasury_yield

```json
{
  "method": "get_treasury_yield",
  "parameters": {
    "term_list": ["m3", "m6", "y1", "y3", "y5", "y10"],
    "is_local": true,
    "begin_date": 20240101,
    "end_date": 20241231
  }
}
```

## 12. 常用 curl 示例

### 12.1 获取 A 股代码列表

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

### 12.2 获取股票基础信息

```bash
curl -X POST "http://localhost:8000/api/amazingdata/call" \
  -H "Content-Type: application/json" \
  -d '{
    "method": "get_stock_basic",
    "parameters": {
      "code_list": ["000001.SZ", "000002.SZ", "600000.SH"]
    }
  }'
```

### 12.3 用文档风格方法名调用

```bash
curl -X POST "http://localhost:8000/api/amazingdata/call" \
  -H "Content-Type: application/json" \
  -d '{
    "method": "ad.InfoData.get_stock_basic",
    "parameters": {
      "code_list": ["000001.SZ"]
    }
  }'
```

## 13. 说明

- 当前接口用于查询型方法，不适用于 `SubscribeData` 这类回调式订阅接口。
- 文档中的 `BaseData.xxx`、`InfoData.xxx`、`MarketData.xxx` 写法可以直接传给 `method`。
- 如果返回数据中存在空值，接口会自动转成 JSON 可接受的 `null`。
- 如果参数名或参数数量不对，接口会返回统一错误 JSON。
