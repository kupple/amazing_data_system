# AmazingData API 设置完成

## 已创建的文件

### 1. API 文档和测试文件
- `AMAZINGDATA_API_DOCS.md` - 完整的 API 接口文档
- `test_amazingdata_api.py` - Python 测试脚本
- `amazingdata_api_test.html` - HTML 测试页面
- `API_README.md` - 快速使用指南
- `start_api.py` - API 启动脚本

### 2. API 服务更新
- 修复了 `src/services/api.py` 中的连接检查逻辑
- 更新了方法列表接口，提供准确的方法分类
- 添加了完整的错误处理

## 使用步骤

### 1. 启动 API 服务

```bash
# 推荐方式
python start_api.py

# 或者直接启动
python src/services/api.py
```

### 2. 验证服务

访问 http://localhost:8000/health 检查服务状态

### 3. 查看 API 文档

访问 http://localhost:8000/docs 查看 Swagger 文档

### 4. 运行测试

```bash
# Python 测试脚本
python test_amazingdata_api.py

# 或打开 HTML 测试页面
open amazingdata_api_test.html
```

## API 功能

### 支持的 AmazingData 方法 (47个)

#### 基础数据 (9个)
- get_code_list, get_code_info, get_calendar
- get_stock_basic, get_backward_factor, get_adj_factor
- get_hist_code_list, get_history_stock_status, get_bj_code_mapping

#### 行情数据 (2个)
- query_kline, query_snapshot

#### 财务数据 (5个)
- get_balance_sheet, get_cash_flow, get_income
- get_profit_express, get_profit_notice

#### 股东数据 (7个)
- get_share_holder, get_holder_num, get_equity_structure
- get_equity_pledge_freeze, get_equity_restricted
- get_dividend, get_right_issue

#### 其他数据 (4个)
- get_margin_summary, get_margin_detail
- get_long_hu_bang, get_block_trading

#### 指数数据 (2个)
- get_index_constituent, get_index_weight

#### 行业数据 (4个)
- get_industry_base_info, get_industry_constituent
- get_industry_weight, get_industry_daily

#### 可转债数据 (11个)
- get_kzz_issuance, get_kzz_share, get_kzz_conv
- get_kzz_conv_change, get_kzz_corr, get_kzz_call
- get_kzz_put, get_kzz_suspend, get_kzz_put_call_item
- get_kzz_put_explanation, get_kzz_call_explanation

#### ETF数据 (3个)
- get_etf_pcf, get_fund_share, get_fund_iopv

#### 期权数据 (5个)
- get_option_basic_info, get_option_std_ctr_specs
- get_option_mon_ctr_specs, get_future_code_list, get_option_code_list

#### 国债数据 (1个)
- get_treasury_yield

## 主要接口

### 1. 调用 AmazingData 方法
**POST** `/api/amazingdata/call`

### 2. 获取可用方法列表
**GET** `/api/amazingdata/methods`

### 3. 健康检查
**GET** `/health`

## 示例调用

### 获取A股代码列表
```json
{
  "method": "get_code_list",
  "parameters": {
    "security_type": "EXTRA_STOCK_A"
  }
}
```

### 获取K线数据
```json
{
  "method": "query_kline",
  "parameters": {
    "code_list": ["000001.SZ"],
    "begin_date": 20240101,
    "end_date": 20241231,
    "period": 1440
  }
}
```

### 获取财务数据
```json
{
  "method": "get_balance_sheet",
  "parameters": {
    "code_list": ["000001.SZ"],
    "is_local": true
  }
}
```

## 特性

1. **自动连接管理** - API 自动管理 AmazingData 连接
2. **数据格式转换** - DataFrame 自动转换为 JSON
3. **错误处理** - 完整的错误处理和重试机制
4. **方法分类** - 47个方法按功能分类展示
5. **测试工具** - 提供 Python 和 HTML 测试工具
6. **文档完整** - 包含详细的 API 文档和使用指南

## 注意事项

1. 确保 AmazingData 客户端配置正确
2. 检查网络连接和权限
3. 合理控制并发请求数量
4. 使用 `is_local` 参数优化性能

API 设置已完成，可以开始使用了！