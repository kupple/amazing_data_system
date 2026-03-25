# -*- coding: utf-8 -*-
"""
创建 Starlight 数据表脚本
自动在 ClickHouse 中创建所有表

使用方法:
    python create_starlight_tables.py
"""
import sys
from pathlib import Path

# 添加项目路径
sys.path.insert(0, str(Path(__file__).parent))

try:
    from src.common.database import get_db
    from src.common.logger import logger
except ImportError as e:
    print(f"错误: 无法导入依赖模块: {e}")
    print("\n请确保:")
    print("1. 已安装依赖: pip install -r requirements.txt")
    print("2. 配置了 .env 文件")
    sys.exit(1)


# 所有表的 DDL 语句
TABLE_DDLS = [
    # 1. 基础数据表
    """
    CREATE TABLE IF NOT EXISTS starlight.stock_codes (
        code String,
        update_time DateTime DEFAULT now()
    ) ENGINE = MergeTree()
    ORDER BY code
    """,
    
    """
    CREATE TABLE IF NOT EXISTS starlight.trading_calendar (
        trade_date Date
    ) ENGINE = MergeTree()
    ORDER BY trade_date
    """,
    
    """
    CREATE TABLE IF NOT EXISTS starlight.stock_basic (
        market_code String,
        sec_name String,
        sec_type String,
        list_date Nullable(Date),
        delist_date Nullable(Date),
        status String,
        exchange String
    ) ENGINE = MergeTree()
    ORDER BY market_code
    """,
    
    """
    CREATE TABLE IF NOT EXISTS starlight.backward_factor (
        code String,
        date Date,
        factor Float64
    ) ENGINE = ReplacingMergeTree()
    ORDER BY (code, date)
    """,
    
    """
    CREATE TABLE IF NOT EXISTS starlight.adj_factor (
        code String,
        date Date,
        factor Float64
    ) ENGINE = ReplacingMergeTree()
    ORDER BY (code, date)
    """,
    
    # 2. 行情数据表
    """
    CREATE TABLE IF NOT EXISTS starlight.kline_daily (
        code String,
        trade_time DateTime,
        open Float64,
        high Float64,
        low Float64,
        close Float64,
        volume Float64,
        amount Float64,
        pre_close Nullable(Float64),
        change_rate Nullable(Float64),
        turnover_rate Nullable(Float64)
    ) ENGINE = ReplacingMergeTree()
    ORDER BY (code, trade_time)
    """,
    
    """
    CREATE TABLE IF NOT EXISTS starlight.snapshot (
        code String,
        time DateTime,
        last_price Float64,
        open Float64,
        high Float64,
        low Float64,
        volume Float64,
        amount Float64,
        bid_price1 Nullable(Float64),
        bid_volume1 Nullable(Float64),
        ask_price1 Nullable(Float64),
        ask_volume1 Nullable(Float64)
    ) ENGINE = ReplacingMergeTree()
    ORDER BY (code, time)
    """,
    
    # 3. 财务数据表
    """
    CREATE TABLE IF NOT EXISTS starlight.balance_sheet (
        market_code String,
        reporting_period Date,
        report_type String,
        total_assets Nullable(Float64),
        total_liabilities Nullable(Float64),
        total_equity Nullable(Float64),
        current_assets Nullable(Float64),
        non_current_assets Nullable(Float64),
        current_liabilities Nullable(Float64),
        non_current_liabilities Nullable(Float64)
    ) ENGINE = ReplacingMergeTree()
    ORDER BY (market_code, reporting_period)
    """,
    
    """
    CREATE TABLE IF NOT EXISTS starlight.cash_flow (
        market_code String,
        reporting_period Date,
        report_type String,
        operating_cash_flow Nullable(Float64),
        investing_cash_flow Nullable(Float64),
        financing_cash_flow Nullable(Float64),
        net_cash_flow Nullable(Float64)
    ) ENGINE = ReplacingMergeTree()
    ORDER BY (market_code, reporting_period)
    """,
    
    """
    CREATE TABLE IF NOT EXISTS starlight.income (
        market_code String,
        reporting_period Date,
        report_type String,
        total_revenue Nullable(Float64),
        operating_revenue Nullable(Float64),
        total_costs Nullable(Float64),
        operating_profit Nullable(Float64),
        total_profit Nullable(Float64),
        net_profit Nullable(Float64),
        net_profit_parent Nullable(Float64),
        basic_eps Nullable(Float64),
        diluted_eps Nullable(Float64)
    ) ENGINE = ReplacingMergeTree()
    ORDER BY (market_code, reporting_period)
    """,
    
    """
    CREATE TABLE IF NOT EXISTS starlight.profit_express (
        market_code String,
        reporting_period Date,
        announce_date Nullable(Date),
        total_revenue Nullable(Float64),
        net_profit Nullable(Float64),
        net_profit_parent Nullable(Float64),
        total_assets Nullable(Float64),
        basic_eps Nullable(Float64),
        roe Nullable(Float64)
    ) ENGINE = ReplacingMergeTree()
    ORDER BY (market_code, reporting_period)
    """,
    
    """
    CREATE TABLE IF NOT EXISTS starlight.profit_notice (
        market_code String,
        reporting_period Date,
        announce_date Nullable(Date),
        notice_type String,
        net_profit_min Nullable(Float64),
        net_profit_max Nullable(Float64),
        change_rate_min Nullable(Float64),
        change_rate_max Nullable(Float64),
        summary Nullable(String)
    ) ENGINE = ReplacingMergeTree()
    ORDER BY (market_code, reporting_period)
    """,
    
    # 4. 股东数据表
    """
    CREATE TABLE IF NOT EXISTS starlight.share_holder (
        market_code String,
        holder_enddate Date,
        qty_num Int32,
        holder_name String,
        holder_type String,
        hold_num Nullable(Float64),
        hold_ratio Nullable(Float64),
        change_num Nullable(Float64)
    ) ENGINE = ReplacingMergeTree()
    ORDER BY (market_code, holder_enddate, qty_num)
    """,
    
    """
    CREATE TABLE IF NOT EXISTS starlight.holder_num (
        market_code String,
        holder_enddate Date,
        holder_num Nullable(Int64),
        holder_num_change Nullable(Float64),
        holder_num_change_rate Nullable(Float64),
        avg_hold_num Nullable(Float64),
        avg_hold_amount Nullable(Float64)
    ) ENGINE = ReplacingMergeTree()
    ORDER BY (market_code, holder_enddate)
    """,
    
    """
    CREATE TABLE IF NOT EXISTS starlight.equity_structure (
        market_code String,
        change_date Date,
        total_share Nullable(Float64),
        float_share Nullable(Float64),
        restricted_share Nullable(Float64),
        state_share Nullable(Float64),
        legal_person_share Nullable(Float64),
        a_share Nullable(Float64),
        b_share Nullable(Float64),
        h_share Nullable(Float64)
    ) ENGINE = ReplacingMergeTree()
    ORDER BY (market_code, change_date)
    """,
    
    # 5. 其他数据表
    """
    CREATE TABLE IF NOT EXISTS starlight.margin_summary (
        trade_date Date,
        market String,
        margin_balance Nullable(Float64),
        short_balance Nullable(Float64),
        total_balance Nullable(Float64),
        margin_buy_amount Nullable(Float64),
        short_sell_amount Nullable(Float64)
    ) ENGINE = ReplacingMergeTree()
    ORDER BY (trade_date, market)
    """,
    
    """
    CREATE TABLE IF NOT EXISTS starlight.margin_detail (
        market_code String,
        trade_date Date,
        margin_balance Nullable(Float64),
        margin_buy_amount Nullable(Float64),
        margin_repay_amount Nullable(Float64),
        short_balance Nullable(Float64),
        short_sell_volume Nullable(Float64),
        short_repay_volume Nullable(Float64)
    ) ENGINE = ReplacingMergeTree()
    ORDER BY (market_code, trade_date)
    """,
    
    """
    CREATE TABLE IF NOT EXISTS starlight.dragon_tiger (
        market_code String,
        trade_date Date,
        trader_name String,
        buy_amount Nullable(Float64),
        sell_amount Nullable(Float64),
        net_amount Nullable(Float64),
        reason String,
        close_price Nullable(Float64),
        change_rate Nullable(Float64)
    ) ENGINE = ReplacingMergeTree()
    ORDER BY (market_code, trade_date, trader_name)
    """,
    
    """
    CREATE TABLE IF NOT EXISTS starlight.block_trade (
        market_code String,
        trade_date Date,
        trade_price Nullable(Float64),
        trade_volume Nullable(Float64),
        trade_amount Nullable(Float64),
        buyer String,
        seller String,
        close_price Nullable(Float64),
        premium_rate Nullable(Float64)
    ) ENGINE = ReplacingMergeTree()
    ORDER BY (market_code, trade_date)
    """,
    
    """
    CREATE TABLE IF NOT EXISTS starlight.equity_pledge_freeze (
        market_code String,
        announce_date Date,
        holder_name String,
        pledge_num Nullable(Float64),
        pledge_ratio Nullable(Float64),
        pledgee String,
        start_date Nullable(Date),
        end_date Nullable(Date)
    ) ENGINE = ReplacingMergeTree()
    ORDER BY (market_code, announce_date, holder_name)
    """,
    
    """
    CREATE TABLE IF NOT EXISTS starlight.equity_restricted (
        market_code String,
        lift_date Date,
        lift_num Nullable(Float64),
        lift_ratio Nullable(Float64),
        lift_type String,
        holder_name Nullable(String)
    ) ENGINE = ReplacingMergeTree()
    ORDER BY (market_code, lift_date)
    """,
    
    """
    CREATE TABLE IF NOT EXISTS starlight.dividend (
        market_code String,
        announce_date Date,
        record_date Nullable(Date),
        ex_dividend_date Nullable(Date),
        dividend_date Nullable(Date),
        dividend_ratio Nullable(Float64),
        bonus_ratio Nullable(Float64),
        transfer_ratio Nullable(Float64),
        dividend_amount Nullable(Float64)
    ) ENGINE = ReplacingMergeTree()
    ORDER BY (market_code, announce_date)
    """,
    
    """
    CREATE TABLE IF NOT EXISTS starlight.right_issue (
        market_code String,
        announce_date Date,
        record_date Nullable(Date),
        ex_right_date Nullable(Date),
        listing_date Nullable(Date),
        right_ratio Nullable(Float64),
        right_price Nullable(Float64),
        right_num Nullable(Float64)
    ) ENGINE = ReplacingMergeTree()
    ORDER BY (market_code, announce_date)
    """,
    
    # 6. 指数数据表
    """
    CREATE TABLE IF NOT EXISTS starlight.index_constituent (
        index_code String,
        market_code String,
        in_date Nullable(Date),
        out_date Nullable(Date)
    ) ENGINE = ReplacingMergeTree()
    ORDER BY (index_code, market_code)
    """,
    
    """
    CREATE TABLE IF NOT EXISTS starlight.index_weight (
        index_code String,
        market_code String,
        trade_date Date,
        weight Nullable(Float64)
    ) ENGINE = ReplacingMergeTree()
    ORDER BY (index_code, market_code, trade_date)
    """,
    
    # 7. 行业数据表
    """
    CREATE TABLE IF NOT EXISTS starlight.industry_base_info (
        industry_code String,
        industry_name String,
        industry_level Int32,
        parent_code Nullable(String)
    ) ENGINE = MergeTree()
    ORDER BY industry_code
    """,
    
    """
    CREATE TABLE IF NOT EXISTS starlight.industry_constituent (
        industry_code String,
        market_code String,
        in_date Nullable(Date),
        out_date Nullable(Date)
    ) ENGINE = ReplacingMergeTree()
    ORDER BY (industry_code, market_code)
    """,
    
    """
    CREATE TABLE IF NOT EXISTS starlight.industry_weight (
        industry_code String,
        market_code String,
        trade_date Date,
        weight Nullable(Float64)
    ) ENGINE = ReplacingMergeTree()
    ORDER BY (industry_code, market_code, trade_date)
    """,
    
    """
    CREATE TABLE IF NOT EXISTS starlight.industry_daily (
        industry_code String,
        trade_date Date,
        open Nullable(Float64),
        high Nullable(Float64),
        low Nullable(Float64),
        close Nullable(Float64),
        volume Nullable(Float64),
        amount Nullable(Float64),
        change_rate Nullable(Float64)
    ) ENGINE = MergeTree()
    ORDER BY (industry_code, trade_date)
    """,
    
    # 8. 可转债数据表
    """
    CREATE TABLE IF NOT EXISTS starlight.kzz_issuance (
        market_code String,
        bond_name String,
        stock_code String,
        issue_date Nullable(Date),
        listing_date Nullable(Date),
        maturity_date Nullable(Date),
        issue_price Nullable(Float64),
        issue_scale Nullable(Float64),
        coupon_rate Nullable(String)
    ) ENGINE = ReplacingMergeTree()
    ORDER BY market_code
    """,
    
    """
    CREATE TABLE IF NOT EXISTS starlight.kzz_share (
        market_code String,
        trade_date Date,
        remain_balance Nullable(Float64),
        remain_ratio Nullable(Float64)
    ) ENGINE = ReplacingMergeTree()
    ORDER BY (market_code, trade_date)
    """,
    
    """
    CREATE TABLE IF NOT EXISTS starlight.kzz_conv (
        market_code String,
        trade_date Date,
        conv_price Nullable(Float64),
        conv_value Nullable(Float64),
        conv_premium_rate Nullable(Float64)
    ) ENGINE = ReplacingMergeTree()
    ORDER BY (market_code, trade_date)
    """,
    
    """
    CREATE TABLE IF NOT EXISTS starlight.kzz_conv_change (
        market_code String,
        change_date Date,
        old_conv_price Nullable(Float64),
        new_conv_price Nullable(Float64),
        change_reason String
    ) ENGINE = ReplacingMergeTree()
    ORDER BY (market_code, change_date)
    """,
    
    """
    CREATE TABLE IF NOT EXISTS starlight.kzz_corr (
        market_code String,
        trade_date Date,
        bond_price Nullable(Float64),
        stock_price Nullable(Float64),
        correlation Nullable(Float64)
    ) ENGINE = ReplacingMergeTree()
    ORDER BY (market_code, trade_date)
    """,
    
    """
    CREATE TABLE IF NOT EXISTS starlight.kzz_call (
        market_code String,
        announce_date Date,
        call_type String,
        call_price Nullable(Float64),
        call_start_date Nullable(Date),
        call_end_date Nullable(Date)
    ) ENGINE = ReplacingMergeTree()
    ORDER BY (market_code, announce_date)
    """,
    
    """
    CREATE TABLE IF NOT EXISTS starlight.kzz_put (
        market_code String,
        announce_date Date,
        put_price Nullable(Float64),
        put_start_date Nullable(Date),
        put_end_date Nullable(Date)
    ) ENGINE = ReplacingMergeTree()
    ORDER BY (market_code, announce_date)
    """,
    
    """
    CREATE TABLE IF NOT EXISTS starlight.kzz_suspend (
        market_code String,
        suspend_date Date,
        resume_date Nullable(Date),
        suspend_reason String
    ) ENGINE = ReplacingMergeTree()
    ORDER BY (market_code, suspend_date)
    """,
    
    # 9. ETF 数据表
    """
    CREATE TABLE IF NOT EXISTS starlight.etf_pcf (
        etf_code String,
        trade_date Date,
        cash_component Nullable(Float64),
        must_cash Nullable(Float64),
        creation_unit Nullable(Int64)
    ) ENGINE = ReplacingMergeTree()
    ORDER BY (etf_code, trade_date)
    """,
    
    """
    CREATE TABLE IF NOT EXISTS starlight.fund_share (
        fund_code String,
        trade_date Date,
        total_share Nullable(Float64),
        change_share Nullable(Float64)
    ) ENGINE = ReplacingMergeTree()
    ORDER BY (fund_code, trade_date)
    """,
    
    """
    CREATE TABLE IF NOT EXISTS starlight.fund_iopv (
        fund_code String,
        trade_time DateTime,
        iopv Nullable(Float64)
    ) ENGINE = MergeTree()
    ORDER BY (fund_code, trade_time)
    """,
    
    # 10. 期权数据表
    """
    CREATE TABLE IF NOT EXISTS starlight.option_basic_info (
        option_code String,
        option_name String,
        underlying_code String,
        option_type String,
        exercise_type String,
        strike_price Nullable(Float64),
        contract_unit Nullable(Float64),
        listing_date Nullable(Date),
        expiry_date Nullable(Date),
        exercise_date Nullable(Date)
    ) ENGINE = ReplacingMergeTree()
    ORDER BY option_code
    """,
    
    """
    CREATE TABLE IF NOT EXISTS starlight.option_std_ctr_specs (
        option_code String,
        contract_id String,
        underlying_code String,
        strike_price Nullable(Float64),
        contract_multiplier Nullable(Float64),
        tick_size Nullable(Float64)
    ) ENGINE = ReplacingMergeTree()
    ORDER BY option_code
    """,
    
    """
    CREATE TABLE IF NOT EXISTS starlight.option_mon_ctr_specs (
        option_code String,
        contract_month String,
        underlying_code String,
        strike_price Nullable(Float64),
        open_interest Nullable(Float64)
    ) ENGINE = ReplacingMergeTree()
    ORDER BY (option_code, contract_month)
    """,
    
    # 11. 国债收益率表
    """
    CREATE TABLE IF NOT EXISTS starlight.treasury_yield (
        trade_date Date,
        term String,
        yield Nullable(Float64)
    ) ENGINE = ReplacingMergeTree()
    ORDER BY (trade_date, term)
    """,
    
    # 12. 系统管理表
    """
    CREATE TABLE IF NOT EXISTS starlight.fetch_records (
        id UInt64,
        data_type String,
        fetch_time DateTime DEFAULT now(),
        success UInt8 DEFAULT 0,
        record_count UInt32 DEFAULT 0,
        error_message Nullable(String),
        retry_count UInt32 DEFAULT 0,
        start_date Nullable(String),
        end_date Nullable(String)
    ) ENGINE = MergeTree()
    ORDER BY (data_type, fetch_time)
    """,
    
    """
    CREATE TABLE IF NOT EXISTS starlight.sync_status (
        data_type String,
        last_sync_time Nullable(DateTime),
        last_success_time Nullable(DateTime),
        record_count UInt32 DEFAULT 0,
        status String DEFAULT 'pending',
        error_message Nullable(String),
        created_at DateTime DEFAULT now(),
        updated_at DateTime DEFAULT now()
    ) ENGINE = ReplacingMergeTree(updated_at)
    ORDER BY data_type
    """,
    
    """
    CREATE TABLE IF NOT EXISTS starlight.daily_summary (
        date String,
        data_type String,
        success_count UInt32 DEFAULT 0,
        failed_count UInt32 DEFAULT 0,
        total_records UInt32 DEFAULT 0
    ) ENGINE = ReplacingMergeTree()
    ORDER BY (date, data_type)
    """,
]


def create_database(db):
    """创建数据库"""
    try:
        db.ensure_database("starlight")
        logger.info("✓ 数据库 starlight 已就绪")
        return True
    except Exception as e:
        logger.error(f"✗ 创建数据库失败: {e}")
        return False


def create_table(db, create_sql: str, index: int, total: int) -> tuple:
    """
    创建单个表
    
    Returns:
        tuple: (table_name, success, error_message)
    """
    import re
    
    # 提取表名
    match = re.search(r'CREATE TABLE IF NOT EXISTS starlight\.(\w+)', create_sql)
    if not match:
        return (None, False, "无法解析表名")
    
    table_name = match.group(1)
    
    try:
        db.execute(create_sql)
        logger.info(f"  [{index}/{total}] ✓ {table_name}")
        return (table_name, True, None)
    except Exception as e:
        logger.error(f"  [{index}/{total}] ✗ {table_name}: {e}")
        return (table_name, False, str(e))


def main():
    """主函数"""
    logger.info("=" * 60)
    logger.info("开始创建 Starlight 数据表")
    logger.info("=" * 60)
    
    # 1. 显示表数量
    logger.info(f"\n将创建 {len(TABLE_DDLS)} 个表")
    
    # 2. 连接数据库
    logger.info("\n1. 连接数据库...")
    try:
        db = get_db("starlight")
        logger.info("✓ 已连接到 ClickHouse")
    except Exception as e:
        logger.error(f"✗ 连接数据库失败: {e}")
        return
    
    # 3. 创建数据库
    logger.info("\n2. 创建数据库...")
    if not create_database(db):
        return
    
    # 4. 创建表
    logger.info("\n3. 创建数据表...")
    logger.info("-" * 60)
    
    success_count = 0
    failed_count = 0
    failed_tables = []
    
    for i, create_sql in enumerate(TABLE_DDLS, 1):
        table_name, success, error = create_table(db, create_sql, i, len(TABLE_DDLS))
        
        if success:
            success_count += 1
        else:
            failed_count += 1
            failed_tables.append((table_name, error))
    
    # 5. 汇总结果
    logger.info("\n" + "=" * 60)
    logger.info("创建结果汇总")
    logger.info("=" * 60)
    logger.info(f"总计: {len(TABLE_DDLS)} 个表")
    logger.info(f"成功: {success_count} 个")
    logger.info(f"失败: {failed_count} 个")
    
    if failed_tables:
        logger.info("\n失败的表:")
        for table_name, error in failed_tables:
            logger.error(f"  - {table_name}: {error}")
    
    # 6. 验证表创建
    logger.info("\n" + "=" * 60)
    logger.info("验证表创建")
    logger.info("=" * 60)
    
    try:
        tables = db.get_tables()
        logger.info(f"✓ 数据库中共有 {len(tables)} 个表")
        
        # 显示所有表
        logger.info("\n所有表:")
        for i, table in enumerate(tables, 1):
            logger.info(f"  {i:2d}. {table}")
    
    except Exception as e:
        logger.error(f"✗ 验证失败: {e}")
    
    logger.info("\n" + "=" * 60)
    if failed_count == 0:
        logger.info("🎉 所有表创建成功！")
    else:
        logger.warning(f"⚠️  有 {failed_count} 个表创建失败")
    logger.info("=" * 60)


if __name__ == "__main__":
    main()
