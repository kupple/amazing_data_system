"""
FastAPI 服务模块
"""
from typing import Optional, List, Dict, Any, Union
from datetime import datetime, date
from fastapi import FastAPI, Query, HTTPException, Depends
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field
import pandas as pd
import psycopg2
import psycopg2.extras

from src.common.config import config
from src.common.logger import logger
from src.common.database import get_db, DuckDBManager
from src.common.models import APIResponse, DataSource

# Baostock psycopg2 连接配置
BAOSTOCK_DSN = "host=0.0.0.0 port=5433 dbname=main"


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

def get_database() -> DuckDBManager:
    """获取数据库实例"""
    return get_db()


def get_baostock_conn():
    """获取 Baostock psycopg2 连接"""
    conn = psycopg2.connect(BAOSTOCK_DSN)
    try:
        yield conn
    finally:
        conn.close()


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
async def list_tables(db: DuckDBManager = Depends(get_database)):
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
async def get_table_info(table_name: str, db: DuckDBManager = Depends(get_database)):
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
    db: DuckDBManager = Depends(get_database)
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
    db: DuckDBManager = Depends(get_database)
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
    db: DuckDBManager = Depends(get_database)
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
    db: DuckDBManager = Depends(get_database)
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
    db: DuckDBManager = Depends(get_database)
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
async def get_stats_overview(db: DuckDBManager = Depends(get_database)):
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
    db: DuckDBManager = Depends(get_database)
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
    db: DuckDBManager = Depends(get_database)
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


# ==================== Baostock 数据接口 ====================

@app.get("/api/baostock/stocks")
async def baostock_get_stocks(
    stock_type: Optional[str] = Query(None, description="股票类型: 1=A股, 2=B股"),
    limit: int = Query(100, ge=1, le=5000),
    offset: int = Query(0, ge=0),
    conn=Depends(get_baostock_conn)
):
    """获取 Baostock 股票列表"""
    try:
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        where = ""
        params = []
        if stock_type:
            where = "WHERE stock_type = %s"
            params.append(stock_type)

        cur.execute(f"SELECT COUNT(*) as cnt FROM stock_list {where}", params)
        total = cur.fetchone()["cnt"]

        cur.execute(f"SELECT * FROM stock_list {where} LIMIT %s OFFSET %s", params + [limit, offset])
        rows = cur.fetchall()
        cur.close()

        return {"code": 200, "message": "success", "data": rows, "total": total, "page": offset // limit + 1, "page_size": limit}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/baostock/stocks/{sec_code}")
async def baostock_get_stock(sec_code: str, conn=Depends(get_baostock_conn)):
    """获取单个 Baostock 股票信息"""
    try:
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        cur.execute("SELECT * FROM stock_list WHERE sec_code = %s", (sec_code,))
        row = cur.fetchone()
        cur.close()
        if not row:
            raise HTTPException(status_code=404, detail=f"股票 {sec_code} 不存在")
        return {"code": 200, "message": "success", "data": row}
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/baostock/kline/{sec_code}")
async def baostock_get_kline(
    sec_code: str,
    start_date: Optional[str] = Query(None, description="开始日期: YYYY-MM-DD"),
    end_date: Optional[str] = Query(None, description="结束日期: YYYY-MM-DD"),
    limit: int = Query(100, ge=1, le=10000),
    offset: int = Query(0, ge=0),
    conn=Depends(get_baostock_conn)
):
    """获取 Baostock 日线数据"""
    try:
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        where = "WHERE sec_code = %s"
        params = [sec_code]
        if start_date:
            where += " AND trade_date >= %s"
            params.append(start_date)
        if end_date:
            where += " AND trade_date <= %s"
            params.append(end_date)

        cur.execute(f"SELECT COUNT(*) as cnt FROM daily_kline {where}", params)
        total = cur.fetchone()["cnt"]

        cur.execute(f"SELECT * FROM daily_kline {where} ORDER BY trade_date DESC LIMIT %s OFFSET %s", params + [limit, offset])
        rows = cur.fetchall()
        cur.close()

        return {"code": 200, "message": "success", "data": rows, "total": total, "page": offset // limit + 1, "page_size": limit}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/baostock/stats")
async def baostock_get_stats(conn=Depends(get_baostock_conn)):
    """获取 Baostock 统计信息"""
    try:
        cur = conn.cursor()
        cur.execute("SELECT COUNT(*) FROM stock_list")
        stock_count = cur.fetchone()[0]
        cur.execute("SELECT COUNT(*) FROM daily_kline")
        kline_count = cur.fetchone()[0]
        cur.execute("SELECT COUNT(DISTINCT sec_code) FROM daily_kline")
        kline_stock_count = cur.fetchone()[0]
        cur.execute("SELECT MAX(trade_date) FROM daily_kline")
        max_date = cur.fetchone()[0]
        cur.close()

        return {
            "code": 200,
            "message": "success",
            "data": {
                "stock_count": stock_count,
                "kline_count": kline_count,
                "kline_stock_count": kline_stock_count,
                "max_date": str(max_date) if max_date else None
            }
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/baostock/tables")
async def baostock_get_tables(conn=Depends(get_baostock_conn)):
    """获取 Baostock 所有表"""
    try:
        cur = conn.cursor()
        cur.execute("SHOW TABLES")
        tables = cur.fetchall()
        table_list = []
        for t in tables:
            name = t[0]
            cur.execute(f"SELECT COUNT(*) FROM {name}")
            count = cur.fetchone()[0]
            table_list.append({"name": name, "count": count})
        cur.close()
        return {"code": 200, "message": "success", "data": table_list}
    except Exception as e:
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
    uvicorn.run(
        app,
        host=config.api.host,
        port=config.api.port,
        reload=config.api.reload
    )
