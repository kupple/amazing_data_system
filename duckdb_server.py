"""
DuckDB Server - 简单的 HTTP 服务器提供数据库查询
"""
import uvicorn
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
import duckdb
from pathlib import Path
import os

app = FastAPI(title="DuckDB Server")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# 数据目录
DATA_DIR = Path("data")

# 挂载所有数据库
databases = {}
if DATA_DIR.exists():
    for db_file in DATA_DIR.glob("*.duckdb"):
        alias = db_file.stem
        databases[alias] = str(db_file)
        print(f"Loaded {alias}: {db_file}")


@app.get("/")
async def root():
    return {"databases": list(databases.keys())}


@app.get("/query/{db}")
async def query(db: str, sql: str):
    """执行 SQL 查询"""
    if db not in databases:
        raise HTTPException(status_code=404, detail=f"Database {db} not found")
    
    try:
        conn = duckdb.connect(databases[db], read_only=True)
        result = conn.execute(sql).fetchall()
        conn.close()
        return {"data": result}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=5433)
