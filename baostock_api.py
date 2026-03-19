"""
Baostock API 服务
独立查询 baostock_full.duckdb 数据
"""
from fastapi import FastAPI, Query, HTTPException, Depends
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field
import duckdb
from typing import Optional, List, Dict

app = FastAPI(
    title="Baostock Data API",
    description="Baostock 数据查询 API",
    version="1.0.0"
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# 数据库路径
DB_PATH = r"C:\Users\mubin\Desktop\amazing_data_system\data\baostock_full.duckdb"


def get_db():
    """获取数据库连接"""
    return duckdb.connect(DB_PATH, read_only=True)


@app.get("/")
async def root():
    return {"name": "Baostock API", "version": "1.0.0", "status": "running"}


@app.get("/health")
async def health():
    return {"status": "healthy"}


# ==================== 股票列表接口 ====================

@app.get("/api/stocks")
async def get_stocks(
    stock_type: Optional[str] = Query(None, description="股票类型: 1=A股, 2=B股"),
    limit: int = Query(100, ge=1, le=5000),
    offset: int = Query(0, ge=0),
    db = Depends(get_db)
):
    """获取股票列表"""
    try:
        where = ""
        if stock_type:
            where = f"WHERE stock_type = '{stock_type}'"
        
        # 查询总数
        total = db.execute(f"SELECT COUNT(*) as cnt FROM stock_list {where}").fetchone()[0]
        
        # 查询数据
        df = db.execute(f"""
            SELECT * FROM stock_list 
            {where}
            LIMIT {limit} OFFSET {offset}
        """).df()
        
        return {
            "code": 200,
            "message": "success",
            "data": df.to_dict(orient="records"),
            "total": total,
            "page": offset // limit + 1,
            "page_size": limit
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/stocks/{sec_code}")
async def get_stock(sec_code: str, db = Depends(get_db)):
    """获取单个股票信息"""
    try:
        df = db.execute(f"""
            SELECT * FROM stock_list WHERE sec_code = '{sec_code}'
        """).df()
        
        if df.empty:
            raise HTTPException(status_code=404, detail=f"股票 {sec_code} 不存在")
        
        return {
            "code": 200,
            "message": "success",
            "data": df.to_dict(orient="records")[0]
        }
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# ==================== 日线数据接口 ====================

@app.get("/api/kline/{sec_code}")
async def get_kline(
    sec_code: str,
    start_date: Optional[str] = Query(None, description="开始日期: YYYY-MM-DD"),
    end_date: Optional[str] = Query(None, description="结束日期: YYYY-MM-DD"),
    limit: int = Query(100, ge=1, le=10000),
    offset: int = Query(0, ge=0),
    db = Depends(get_db)
):
    """获取日线数据"""
    try:
        where = f"WHERE sec_code = '{sec_code}'"
        if start_date:
            where += f" AND trade_date >= '{start_date}'"
        if end_date:
            where += f" AND trade_date <= '{end_date}'"
        
        # 查询总数
        total = db.execute(f"SELECT COUNT(*) as cnt FROM daily_kline {where}").fetchone()[0]
        
        # 查询数据
        df = db.execute(f"""
            SELECT * FROM daily_kline 
            {where}
            ORDER BY trade_date DESC
            LIMIT {limit} OFFSET {offset}
        """).df()
        
        return {
            "code": 200,
            "message": "success",
            "data": df.to_dict(orient="records"),
            "total": total,
            "page": offset // limit + 1,
            "page_size": limit
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# ==================== 统计接口 ====================

@app.get("/api/stats")
async def get_stats(db = Depends(get_db)):
    """获取统计信息"""
    try:
        stock_count = db.execute("SELECT COUNT(*) FROM stock_list").fetchone()[0]
        kline_count = db.execute("SELECT COUNT(*) FROM daily_kline").fetchone()[0]
        kline_stock_count = db.execute("SELECT COUNT(DISTINCT sec_code) FROM daily_kline").fetchone()[0]
        max_date = db.execute("SELECT MAX(trade_date) FROM daily_kline").fetchone()[0]
        
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


@app.get("/api/tables")
async def get_tables(db = Depends(get_db)):
    """获取所有表"""
    try:
        tables = db.execute("SHOW TABLES").fetchall()
        table_list = []
        for t in tables:
            name = t[0]
            count = db.execute(f"SELECT COUNT(*) FROM {name}").fetchone()[0]
            table_list.append({"name": name, "count": count})
        
        return {
            "code": 200,
            "message": "success",
            "data": table_list
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
