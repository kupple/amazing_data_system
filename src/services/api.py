"""
FastAPI 服务模块
"""
import inspect
import json
import math
import sys
from pathlib import Path

# 添加项目根目录到 Python 路径
project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root))

from typing import Optional, List, Dict, Any, Union
from datetime import datetime, date
from fastapi import FastAPI, Query, HTTPException, Depends, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse, Response
from pydantic import BaseModel, Field
import pandas as pd
from starlette.concurrency import run_in_threadpool

from src.common.config import config
from src.common.logger import logger
from src.common.database import get_db, ClickHouseManager
from src.common.models import APIResponse, DataSource


# 创建 FastAPI 应用
app = FastAPI(
    title="AmazingData API",
    description="AmazingData 金融数据 API 服务",
    version="1.0.0"
)

# CORS 中间件
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


# ==================== Pydantic 模型 ====================

class QueryRequest(BaseModel):
    """通用查询请求"""
    table_name: str = Field(..., description="表名")
    columns: Optional[str] = Field(None, description="查询列，逗号分隔")
    where: Optional[str] = Field(None, description="WHERE 条件")
    order_by: Optional[str] = Field(None, description="排序字段")
    limit: int = Field(100, ge=1, le=5000, description="返回条数")
    offset: int = Field(0, ge=0, description="偏移量")


class DataResponse(BaseModel):
    """数据响应"""
    code: int = 200
    message: str = "success"
    data: List[Dict] = []
    total: int = 0
    page: int = 1
    page_size: int = 100


# ==================== 依赖 ====================

def get_database() -> ClickHouseManager:
    """获取数据库实例"""
    return get_db()


@app.exception_handler(HTTPException)
async def http_exception_handler(request: Request, exc: HTTPException):
    """统一 HTTP 异常响应。"""
    detail = exc.detail if isinstance(exc.detail, str) else str(exc.detail)
    return JSONResponse(
        status_code=exc.status_code,
        content=APIResponse(
            code=exc.status_code,
            message="请求失败",
            data={"error": detail},
            total=0
        ).to_dict()
    )


@app.exception_handler(Exception)
async def unhandled_exception_handler(request: Request, exc: Exception):
    """统一未处理异常响应，避免只在服务端打印栈。"""
    logger.exception(f"未处理异常: {exc}")
    return JSONResponse(
        status_code=500,
        content=APIResponse(
            code=500,
            message="服务异常",
            data={"error": str(exc)},
            total=0
        ).to_dict()
    )


# ==================== 根路径 ====================

@app.get("/")
async def root():
    """API 根路径"""
    return {
        "name": "AmazingData API",
        "version": "1.0.0",
        "status": "running"
    }


@app.get("/health")
async def health():
    """健康检查"""
    return {"status": "healthy"}


# ==================== 表结构接口 ====================

@app.get("/api/tables")
async def list_tables(db: ClickHouseManager = Depends(get_database)):
    """获取所有表"""
    try:
        tables = db.get_tables()
        table_info = []
        
        for table in tables:
            count = db.get_table_count(table)
            table_info.append({
                "name": table,
                "record_count": count
            })
        
        return APIResponse(
            code=200,
            message="success",
            data=table_info,
            total=len(table_info)
        ).to_dict()
    except Exception as e:
        logger.error(f"获取表列表失败: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/tables/{table_name}/info")
async def get_table_info(table_name: str, db: ClickHouseManager = Depends(get_database)):
    """获取表结构信息"""
    try:
        # 获取表记录数
        count = db.get_table_count(table_name)
        
        # 获取列信息
        columns = db.query(f"PRAGMA table_info('{table_name}')")
        
        return APIResponse(
            code=200,
            message="success",
            data={
                "table_name": table_name,
                "record_count": count,
                "columns": columns.to_dict(orient="records") if not columns.empty else []
            }
        ).to_dict()
    except Exception as e:
        logger.error(f"获取表信息失败: {e}")
        raise HTTPException(status_code=500, detail=str(e))


# ==================== 数据查询接口 ====================

@app.get("/api/data/{table_name}")
async def query_data(
    table_name: str,
    columns: Optional[str] = Query(None, description="查询列，逗号分隔"),
    where: Optional[str] = Query(None, description="WHERE 条件"),
    order_by: Optional[str] = Query(None, description="排序字段"),
    limit: int = Query(100, ge=1, le=5000, description="返回条数"),
    offset: int = Query(0, ge=0, description="偏移量"),
    db: ClickHouseManager = Depends(get_database)
):
    """查询数据"""
    try:
        # 验证表是否存在
        if not db.table_exists(table_name):
            raise HTTPException(status_code=404, detail=f"表 {table_name} 不存在")
        
        # 构建查询
        cols = columns or "*"
        sql = f"SELECT {cols} FROM {table_name}"
        
        if where:
            sql += f" WHERE {where}"
        
        if order_by:
            sql += f" ORDER BY {order_by}"
        
        sql += f" LIMIT {limit} OFFSET {offset}"
        
        # 获取总数
        count_sql = f"SELECT COUNT(*) as total FROM ({sql.split('LIMIT')[0]})"
        total = db.query(count_sql).iloc[0]["total"] if not db.query(count_sql).empty else 0
        
        # 查询数据
        df = db.query(sql)
        
        return APIResponse(
            code=200,
            message="success",
            data=df.to_dict(orient="records"),
            total=total,
            page=offset // limit + 1,
            page_size=limit
        ).to_dict()
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"查询数据失败: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/api/data/query")
async def advanced_query(
    request: QueryRequest,
    db: ClickHouseManager = Depends(get_database)
):
    """高级查询"""
    try:
        if not db.table_exists(request.table_name):
            raise HTTPException(status_code=404, detail=f"表 {request.table_name} 不存在")
        
        # 构建查询
        cols = request.columns or "*"
        sql = f"SELECT {cols} FROM {request.table_name}"
        
        if request.where:
            sql += f" WHERE {request.where}"
        
        if request.order_by:
            sql += f" ORDER BY {request.order_by}"
        
        sql += f" LIMIT {request.limit} OFFSET {request.offset}"
        
        # 查询数据
        df = db.query(sql)
        
        return APIResponse(
            code=200,
            message="success",
            data=df.to_dict(orient="records"),
            total=len(df),
            page=request.page,
            page_size=request.limit
        ).to_dict()
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"高级查询失败: {e}")
        raise HTTPException(status_code=500, detail=str(e))


# ==================== 同步状态接口 ====================

@app.get("/api/sync/status")
async def get_sync_status(
    data_type: Optional[str] = Query(None, description="数据类型"),
    db: ClickHouseManager = Depends(get_database)
):
    """获取同步状态"""
    try:
        status = db.get_sync_status(data_type)
        
        return APIResponse(
            code=200,
            message="success",
            data=status if isinstance(status, list) else [status]
        ).to_dict()
    except Exception as e:
        logger.error(f"获取同步状态失败: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/sync/logs")
async def get_sync_logs(
    date: Optional[str] = Query(None, description="日期 YYYY-MM-DD"),
    data_type: Optional[str] = Query(None, description="数据类型"),
    limit: int = Query(100, ge=1, le=1000, description="返回条数")
):
    """获取同步日志"""
    try:
        logs = logger.get_fetch_logs(date, data_type, limit)
        
        return APIResponse(
            code=200,
            message="success",
            data=logs,
            total=len(logs)
        ).to_dict()
    except Exception as e:
        logger.error(f"获取同步日志失败: {e}")
        raise HTTPException(status_code=500, detail=str(e))


class SyncTriggerRequest(BaseModel):
    """同步触发请求"""
    data_type: str = Field(..., description="数据类型，如 security_info, kline_1D 等")
    force: bool = Field(False, description="是否强制全量更新")


@app.post("/api/sync/trigger")
async def trigger_sync(
    request: SyncTriggerRequest,
    db: ClickHouseManager = Depends(get_database)
):
    """触发数据同步"""
    try:
        from src.scheduler import get_scheduler
        from src.client import get_client
        
        # 验证数据类型是否有效
        try:
            data_source = DataSource(request.data_type)
        except ValueError:
            raise HTTPException(status_code=400, detail=f"无效的数据类型: {request.data_type}")
        
        # 获取调度器和客户端
        scheduler = get_scheduler()
        client = get_client()
        
        # 确保客户端已登录
        if not client.login():
            raise HTTPException(status_code=500, detail="数据源连接失败")
        
        # 执行同步
        logger.info(f"API 触发同步: {request.data_type}, force={request.force}")
        
        # 根据 force 参数决定是增量还是全量
        if request.force:
            result = scheduler.fetcher.fetch_and_save(data_source, force_full=True)
        else:
            result = scheduler.fetcher.fetch_and_save(data_source)
        
        return APIResponse(
            code=200,
            message="同步触发成功",
            data={
                "data_type": request.data_type,
                "force": request.force,
                "result": result
            }
        ).to_dict()
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"同步触发失败: {e}")
        raise HTTPException(status_code=500, detail=str(e))


# ==================== 原生 SQL 接口 ====================

@app.post("/api/sql")
async def execute_sql(
    sql: str = Query(..., description="SQL 语句"),
    db: ClickHouseManager = Depends(get_database)
):
    """执行原生 SQL（只读查询）"""
    try:
        # 安全检查
        sql_lower = sql.lower().strip()
        if not sql_lower.startswith("select"):
            raise HTTPException(status_code=400, detail="只支持 SELECT 查询")
        
        # 危险操作检查
        dangerous_keywords = ["drop", "delete", "update", "insert", "alter", "create"]
        for keyword in dangerous_keywords:
            if sql_lower.startswith(keyword):
                raise HTTPException(status_code=400, detail=f"不支持 {keyword} 操作")
        
        df = db.query(sql)
        
        return APIResponse(
            code=200,
            message="success",
            data=df.to_dict(orient="records"),
            total=len(df)
        ).to_dict()
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"SQL 执行失败: {e}")
        raise HTTPException(status_code=500, detail=str(e))


# ==================== 统计接口 ====================

@app.get("/api/stats/overview")
async def get_stats_overview(db: ClickHouseManager = Depends(get_database)):
    """获取系统统计概览"""
    try:
        tables = db.get_tables()
        
        overview = {
            "total_tables": len(tables),
            "total_records": sum(db.get_table_count(t) for t in tables),
            "tables": []
        }
        
        for table in tables:
            count = db.get_table_count(table)
            overview["tables"].append({
                "name": table,
                "record_count": count
            })
        
        # 获取同步状态
        sync_status = db.get_sync_status()
        overview["sync_status"] = sync_status
        
        return APIResponse(
            code=200,
            message="success",
            data=overview
        ).to_dict()
    except Exception as e:
        logger.error(f"获取统计概览失败: {e}")
        raise HTTPException(status_code=500, detail=str(e))


# ==================== 便捷查询接口 ====================

@app.get("/api/quote/{sec_code}")
async def get_quote(
    sec_code: str,
    db: ClickHouseManager = Depends(get_database)
):
    """获取行情数据（便捷接口）"""
    try:
        # 尝试从快照表查询
        tables = ["stock_snapshot", "index_snapshot", "etf_snapshot", "cb_snapshot"]
        
        for table in tables:
            if db.table_exists(table):
                df = db.query(f"SELECT * FROM {table} WHERE sec_code = '{sec_code}' LIMIT 1")
                if not df.empty:
                    return APIResponse(
                        code=200,
                        message="success",
                        data=df.to_dict(orient="records")
                    ).to_dict()
        
        raise HTTPException(status_code=404, detail=f"未找到 {sec_code} 的行情数据")
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"获取行情失败: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/kline/{sec_code}")
async def get_kline(
    sec_code: str,
    kline_type: str = Query("1D", description="K线类型: 1K, 5K, 15K, 30K, 1H, 1D, 1W, 1M"),
    limit: int = Query(100, ge=1, le=5000),
    db: ClickHouseManager = Depends(get_database)
):
    """获取K线数据"""
    try:
        table_name = f"kline_{kline_type}"
        
        if not db.table_exists(table_name):
            raise HTTPException(status_code=404, detail=f"K线类型 {kline_type} 不存在")
        
        df = db.query(f"""
            SELECT * FROM {table_name} 
            WHERE sec_code = '{sec_code}'
            ORDER BY trade_time DESC
            LIMIT {limit}
        """)
        
        return APIResponse(
            code=200,
            message="success",
            data=df.to_dict(orient="records"),
            total=len(df)
        ).to_dict()
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"获取K线失败: {e}")
        raise HTTPException(status_code=500, detail=str(e))


# ==================== AmazingData 直接调用接口 ====================

class AmazingDataRequest(BaseModel):
    """AmazingData 接口调用请求"""
    method: str = Field(
        ...,
        description="方法名，支持 get_code_list、BaseData.get_code_list、ad.InfoData.get_stock_basic 等写法"
    )
    parameters: Dict[str, Any] = Field(default_factory=dict, description="方法参数")


AMAZINGDATA_METHODS_INFO = {
    "基础数据": [
        {"name": "get_code_list", "description": "获取股票代码列表"},
        {"name": "get_code_info", "description": "获取代码信息"},
        {"name": "get_calendar", "description": "获取交易日历"},
        {"name": "get_stock_basic", "description": "获取股票基础信息"},
        {"name": "get_backward_factor", "description": "获取后复权因子"},
        {"name": "get_adj_factor", "description": "获取前复权因子"},
        {"name": "get_hist_code_list", "description": "获取历史代码列表"},
        {"name": "get_history_stock_status", "description": "获取历史股票状态"},
        {"name": "get_bj_code_mapping", "description": "获取北交所代码映射"},
    ],
    "行情数据": [
        {"name": "query_kline", "description": "获取K线数据"},
        {"name": "query_snapshot", "description": "获取快照数据"},
    ],
    "财务数据": [
        {"name": "get_balance_sheet", "description": "获取资产负债表"},
        {"name": "get_cash_flow", "description": "获取现金流量表"},
        {"name": "get_income", "description": "获取利润表"},
        {"name": "get_profit_express", "description": "获取业绩快报"},
        {"name": "get_profit_notice", "description": "获取业绩预告"},
    ],
    "股东数据": [
        {"name": "get_share_holder", "description": "获取十大股东"},
        {"name": "get_holder_num", "description": "获取股东户数"},
        {"name": "get_equity_structure", "description": "获取股本结构"},
        {"name": "get_equity_pledge_freeze", "description": "获取股权质押冻结"},
        {"name": "get_equity_restricted", "description": "获取限售股解禁"},
        {"name": "get_dividend", "description": "获取分红送股"},
        {"name": "get_right_issue", "description": "获取配股"},
    ],
    "其他数据": [
        {"name": "get_margin_summary", "description": "获取融资融券汇总"},
        {"name": "get_margin_detail", "description": "获取融资融券明细"},
        {"name": "get_long_hu_bang", "description": "获取龙虎榜"},
        {"name": "get_block_trading", "description": "获取大宗交易"},
    ],
    "指数数据": [
        {"name": "get_index_constituent", "description": "获取指数成分股"},
        {"name": "get_index_weight", "description": "获取指数权重"},
    ],
    "行业数据": [
        {"name": "get_industry_base_info", "description": "获取行业基本信息"},
        {"name": "get_industry_constituent", "description": "获取行业成分股"},
        {"name": "get_industry_weight", "description": "获取行业权重"},
        {"name": "get_industry_daily", "description": "获取行业日线"},
    ],
    "可转债数据": [
        {"name": "get_kzz_issuance", "description": "获取可转债发行"},
        {"name": "get_kzz_share", "description": "获取可转债份额"},
        {"name": "get_kzz_conv", "description": "获取可转债转股"},
        {"name": "get_kzz_conv_change", "description": "获取可转债转股变动"},
        {"name": "get_kzz_corr", "description": "获取可转债修正"},
        {"name": "get_kzz_call", "description": "获取可转债赎回"},
        {"name": "get_kzz_put", "description": "获取可转债回售"},
        {"name": "get_kzz_suspend", "description": "获取可转债停复牌"},
        {"name": "get_kzz_put_call_item", "description": "获取可转债回售赎回条款"},
        {"name": "get_kzz_put_explanation", "description": "获取可转债回售说明"},
        {"name": "get_kzz_call_explanation", "description": "获取可转债赎回说明"},
    ],
    "ETF数据": [
        {"name": "get_etf_pcf", "description": "获取ETF申赎数据"},
        {"name": "get_fund_share", "description": "获取基金份额"},
        {"name": "get_fund_iopv", "description": "获取基金IOPV"},
    ],
    "期权数据": [
        {"name": "get_option_basic_info", "description": "获取期权基本资料"},
        {"name": "get_option_std_ctr_specs", "description": "获取期权标准合约"},
        {"name": "get_option_mon_ctr_specs", "description": "获取期权月合约"},
        {"name": "get_future_code_list", "description": "获取期货代码列表"},
        {"name": "get_option_code_list", "description": "获取期权代码列表"},
    ],
    "国债数据": [
        {"name": "get_treasury_yield", "description": "获取国债收益率"},
    ],
}

ALLOWED_AMAZINGDATA_METHODS = {
    item["name"]
    for methods in AMAZINGDATA_METHODS_INFO.values()
    for item in methods
}

AMAZINGDATA_METHOD_CLASSES = {
    "get_code_info": "BaseData",
    "get_code_list": "BaseData",
    "get_future_code_list": "BaseData",
    "get_option_code_list": "BaseData",
    "get_backward_factor": "BaseData",
    "get_adj_factor": "BaseData",
    "get_hist_code_list": "BaseData",
    "get_calendar": "BaseData",
    "get_etf_pcf": "BaseData",
    "get_stock_basic": "InfoData",
    "get_history_stock_status": "InfoData",
    "get_bj_code_mapping": "InfoData",
    "get_balance_sheet": "InfoData",
    "get_cash_flow": "InfoData",
    "get_income": "InfoData",
    "get_profit_express": "InfoData",
    "get_profit_notice": "InfoData",
    "get_share_holder": "InfoData",
    "get_holder_num": "InfoData",
    "get_equity_structure": "InfoData",
    "get_equity_pledge_freeze": "InfoData",
    "get_equity_restricted": "InfoData",
    "get_dividend": "InfoData",
    "get_right_issue": "InfoData",
    "get_margin_summary": "InfoData",
    "get_margin_detail": "InfoData",
    "get_long_hu_bang": "InfoData",
    "get_block_trading": "InfoData",
    "get_option_basic_info": "InfoData",
    "get_option_std_ctr_specs": "InfoData",
    "get_option_mon_ctr_specs": "InfoData",
    "get_fund_share": "InfoData",
    "get_fund_iopv": "InfoData",
    "get_index_constituent": "InfoData",
    "get_index_weight": "InfoData",
    "get_industry_base_info": "InfoData",
    "get_industry_constituent": "InfoData",
    "get_industry_weight": "InfoData",
    "get_industry_daily": "InfoData",
    "get_kzz_issuance": "InfoData",
    "get_kzz_share": "InfoData",
    "get_kzz_conv": "InfoData",
    "get_kzz_conv_change": "InfoData",
    "get_kzz_corr": "InfoData",
    "get_kzz_call": "InfoData",
    "get_kzz_put": "InfoData",
    "get_kzz_put_call_item": "InfoData",
    "get_kzz_put_explanation": "InfoData",
    "get_kzz_call_explanation": "InfoData",
    "get_kzz_suspend": "InfoData",
    "get_treasury_yield": "InfoData",
    "query_snapshot": "MarketData",
    "query_kline": "MarketData",
}


def _build_amazingdata_method_aliases() -> Dict[str, str]:
    """构建文档方法别名到内部封装方法的映射。"""
    alias_map: Dict[str, str] = {}
    for method_name in ALLOWED_AMAZINGDATA_METHODS:
        class_name = AMAZINGDATA_METHOD_CLASSES.get(method_name)
        candidates = {
            method_name,
            method_name.lower(),
        }
        if class_name:
            candidates.update({
                f"{class_name}.{method_name}",
                f"{class_name.lower()}.{method_name}",
                f"ad.{class_name}.{method_name}",
                f"ad.{class_name.lower()}.{method_name}",
                f"AmazingData.{class_name}.{method_name}",
                f"AmazingData.{class_name.lower()}.{method_name}",
            })

        for candidate in candidates:
            alias_map[candidate] = method_name
            alias_map[candidate.replace(" ", "")] = method_name

    return alias_map


AMAZINGDATA_METHOD_ALIASES = _build_amazingdata_method_aliases()


def _serialize_amazingdata_result(result: Any) -> Any:
    """将 AmazingData 返回结果转换为可 JSON 序列化的数据。"""
    if isinstance(result, pd.DataFrame):
        return [
            {
                key: _sanitize_amazingdata_value(value)
                for key, value in record.items()
            }
            for record in result.to_dict(orient="records")
        ]
    if isinstance(result, dict):
        return {
            key: _serialize_amazingdata_result(value)
            for key, value in result.items()
        }
    if isinstance(result, list):
        return [_serialize_amazingdata_result(item) for item in result]
    if isinstance(result, tuple):
        return [_serialize_amazingdata_result(item) for item in result]
    return _sanitize_amazingdata_value(result)


def _sanitize_amazingdata_value(value: Any) -> Any:
    """清理 JSON 不兼容的标量值，如 NaN/Inf/NaT。"""
    if value is None:
        return None
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    if isinstance(value, float):
        if math.isnan(value) or math.isinf(value):
            return None
        return value
    if pd.isna(value):
        return None
    if hasattr(value, "item") and callable(value.item):
        try:
            return _sanitize_amazingdata_value(value.item())
        except Exception:
            return str(value)
    return value


def _count_amazingdata_result(result: Any) -> int:
    """统计返回结果的记录数。"""
    if result is None:
        return 0
    if isinstance(result, (pd.DataFrame, list)):
        return len(result)
    if isinstance(result, dict):
        return sum(_count_amazingdata_result(value) for value in result.values())
    return 1


def _amazingdata_error_response(status_code: int, message: str, error: str) -> JSONResponse:
    """AmazingData 接口统一错误响应。"""
    payload = APIResponse(
        code=status_code,
        message=message,
        data={"error": error},
        total=0
    ).to_dict()
    return _safe_json_response(payload, status_code=status_code)


def _safe_json_response(content: Dict[str, Any], status_code: int = 200) -> Response:
    """先执行 JSON 序列化校验，再返回响应，避免渲染阶段才抛 500。"""
    body = json.dumps(
        content,
        ensure_ascii=False,
        allow_nan=False,
        default=str,
        separators=(",", ":")
    )
    return Response(content=body, status_code=status_code, media_type="application/json")


def _normalize_amazingdata_method(method_name: str) -> Optional[str]:
    """将文档中的方法写法归一化为内部封装方法名。"""
    if not method_name:
        return None

    normalized = method_name.strip().replace("()", "").replace(" ", "")
    if normalized in AMAZINGDATA_METHOD_ALIASES:
        return AMAZINGDATA_METHOD_ALIASES[normalized]

    lowered = normalized.lower()
    if lowered in AMAZINGDATA_METHOD_ALIASES:
        return AMAZINGDATA_METHOD_ALIASES[lowered]

    return None


def _unwrap_retry_method(method: Any) -> Any:
    """优先使用被 retry 装饰前的原始方法，避免 API 层重复重试。"""
    wrapped = getattr(method, "__wrapped__", None)
    bound_self = getattr(method, "__self__", None)
    if wrapped is not None and bound_self is not None:
        return wrapped.__get__(bound_self, type(bound_self))
    return method


@app.post("/api/amazingdata/call")
async def call_amazingdata_method(request: AmazingDataRequest):
    """直接调用 AmazingData 方法"""
    try:
        from src.collectors.starlight.client import get_client

        client = get_client()
        if not client.is_connected:
            if not await run_in_threadpool(client.connect):
                return _amazingdata_error_response(
                    status_code=500,
                    message="调用失败",
                    error="无法连接到 AmazingData 服务"
                )

        normalized_method = _normalize_amazingdata_method(request.method)
        if normalized_method is None:
            return _amazingdata_error_response(
                status_code=400,
                message="调用失败",
                error=(
                    f"方法 {request.method} 不在允许调用的 AmazingData 接口列表中。"
                    "支持传 get_code_list、BaseData.get_code_list、ad.InfoData.get_stock_basic 这类写法"
                )
            )

        method = getattr(client, normalized_method)
        raw_method = _unwrap_retry_method(method)
        if not callable(raw_method):
            return _amazingdata_error_response(
                status_code=400,
                message="调用失败",
                error=f"属性 {normalized_method} 不是可调用的方法"
            )

        try:
            inspect.signature(raw_method).bind(**request.parameters)
        except TypeError as e:
            return _amazingdata_error_response(
                status_code=400,
                message="调用失败",
                error=f"方法 {normalized_method} 参数错误: {e}"
            )

        logger.info(
            f"API 调用: original_method={request.method}, "
            f"normalized_method={normalized_method}, parameters={request.parameters}"
        )
        try:
            result = await run_in_threadpool(raw_method, **request.parameters)
        except TypeError as e:
            return _amazingdata_error_response(
                status_code=400,
                message="调用失败",
                error=f"方法 {normalized_method} 参数错误: {e}"
            )

        payload = APIResponse(
            code=200,
            message="调用成功",
            data=_serialize_amazingdata_result(result),
            total=_count_amazingdata_result(result)
        ).to_dict()
        return _safe_json_response(payload, status_code=200)

    except Exception as e:
        logger.error(f"AmazingData 方法调用失败: {e}")
        return _amazingdata_error_response(
            status_code=500,
            message="调用失败",
            error=str(e)
        )


@app.get("/api/amazingdata/methods")
async def list_amazingdata_methods():
    """列出所有可用的 AmazingData 方法"""
    try:
        total_methods = sum(len(methods) for methods in AMAZINGDATA_METHODS_INFO.values())

        return APIResponse(
            code=200,
            message="success",
            data=AMAZINGDATA_METHODS_INFO,
            total=total_methods
        ).to_dict()
        
    except Exception as e:
        logger.error(f"获取方法列表失败: {e}")
        raise HTTPException(status_code=500, detail=str(e))


# ==================== QMT API 路由 ====================

# 导入 QMT 模块
try:
    from src.qmt.database import QMTDatabase, get_qmt_db
    from src.qmt.client import QMTClient, get_qmt_client
    from src.qmt.scheduler import QMTScheduler, get_qmt_scheduler
    QMT_AVAILABLE = True
except ImportError:
    QMT_AVAILABLE = False


# 创建独立的 QMT 数据库路由
qmt_app = FastAPI(
    title="QMT Data API",
    description="QMT 数据查询 API (独立数据库)",
    version="1.0.0"
)

# CORS
qmt_app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


@qmt_app.get("/")
async def qmt_root():
    """QMT API 根路径"""
    if not QMT_AVAILABLE:
        return {"error": "QMT 模块不可用"}
    return {"name": "QMT Data API", "version": "1.0.0", "status": "running"}


@qmt_app.get("/health")
async def qmt_health():
    """健康检查"""
    if not QMT_AVAILABLE:
        return {"status": "unavailable", "error": "QMT 模块不可用"}
    return {"status": "healthy"}


@qmt_app.get("/tables")
async def qmt_list_tables():
    """列出所有表"""
    if not QMT_AVAILABLE:
        raise HTTPException(status_code=503, detail="QMT 模块不可用")
    db = get_qmt_db()
    return {"tables": db.get_tables()}


@qmt_app.get("/stats")
async def qmt_get_stats():
    """获取统计信息"""
    if not QMT_AVAILABLE:
        raise HTTPException(status_code=503, detail="QMT 模块不可用")
    db = get_qmt_db()
    tables = db.get_tables()
    stats = {table: db.get_table_count(table) for table in tables}
    return {"code": 200, "stats": stats, "total_tables": len(tables)}


@qmt_app.get("/data/{table_name}")
async def qmt_query_data(
    table_name: str,
    limit: int = Query(100, ge=1, le=5000),
    offset: int = Query(0, ge=0),
    columns: Optional[str] = None,
    where: Optional[str] = None
):
    """查询 QMT 数据"""
    if not QMT_AVAILABLE:
        raise HTTPException(status_code=503, detail="QMT 模块不可用")
    
    db = get_qmt_db()
    tables = db.get_tables()
    if table_name not in tables:
        raise HTTPException(status_code=404, detail=f"表 {table_name} 不存在")
    
    cols = columns if columns else "*"
    sql = f"SELECT {cols} FROM {table_name}"
    if where:
        sql += f" WHERE {where}"
    sql += f" LIMIT {limit} OFFSET {offset}"
    
    try:
        df = db.query(sql)
        return {"code": 200, "data": df.to_dict(orient="records"), "total": len(df)}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# 启动应用
if __name__ == "__main__":
    import uvicorn
    
    print("=" * 60)
    print("启动 AmazingData API 服务")
    print("=" * 60)
    
    # 检查环境
    try:
        print(f"✓ 配置加载成功")
        print(f"  - 主机: {config.api.host}")
        print(f"  - 端口: {config.api.port}")
        print(f"  - 调试模式: {config.api.reload}")
    except Exception as e:
        print(f"✗ 配置加载失败: {e}")
        sys.exit(1)
    
    # 检查 AmazingData 客户端
    try:
        from src.collectors.starlight.client import get_client
        client = get_client()
        print(f"✓ AmazingData 客户端初始化成功")
        print(f"  - 用户名: {client.username}")
    except Exception as e:
        print(f"✗ AmazingData 客户端初始化失败: {e}")
        print("  请检查配置文件中的用户名和密码")
        print("  提示：请确保 .env 文件已正确配置")
    
    print("\n" + "=" * 60)
    print("API 服务启动中...")
    print("=" * 60)
    print(f"访问地址: http://{config.api.host}:{config.api.port}")
    print(f"API 文档: http://{config.api.host}:{config.api.port}/docs")
    print(f"健康检查: http://{config.api.host}:{config.api.port}/health")
    print(f"测试页面: amazingdata_api_test.html")
    print("按 Ctrl+C 停止服务")
    print("=" * 60)
    
    try:
        uvicorn.run(
            app,
            host=config.api.host,
            port=config.api.port,
            reload=config.api.reload,
            log_level="info"
        )
    except KeyboardInterrupt:
        print("\n服务已停止")
    except Exception as e:
        print(f"服务启动失败: {e}")
        sys.exit(1)
