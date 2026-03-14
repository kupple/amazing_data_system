"""
QMT MCP 服务模块
通过 MCP 协议调用 QMT 数据
"""
import json
from typing import Optional, Dict, Any, List
from datetime import datetime
from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import StreamingResponse
from pydantic import BaseModel
import uvicorn
import asyncio

from src.qmt.database import get_qmt_db
from src.qmt.client import get_qmt_client
from src.qmt.scheduler import get_qmt_scheduler
from src.logger import logger


# MCP 请求/响应模型
class MCPRequest(BaseModel):
    """MCP 请求"""
    jsonrpc: str = "2.0"
    id: Optional[int] = None
    method: str
    params: Optional[Dict[str, Any]] = {}


class MCPResponse(BaseModel):
    """MCP 响应"""
    jsonrpc: str = "2.0"
    id: Optional[int] = None
    result: Optional[Any] = None
    error: Optional[Dict[str, Any]] = None


# 创建 MCP 应用
qmt_mcp_app = FastAPI(title="QMT MCP")
qmt_mcp_service = None


def get_qmt_mcp_service():
    """获取 QMT MCP 服务实例"""
    global qmt_mcp_service
    if qmt_mcp_service is None:
        qmt_mcp_service = QMTMCPService()
    return qmt_mcp_service


class QMTMCPService:
    """QMT MCP 服务"""
    
    def __init__(self):
        self.db = None
        self.client = None
        self.scheduler = None
    
    def get_tools(self) -> List[Dict]:
        """获取工具列表"""
        return [
            {
                "name": "qmt_list_tables",
                "description": "列出 QMT 数据库所有表",
                "inputSchema": {
                    "type": "object",
                    "properties": {}
                }
            },
            {
                "name": "qmt_get_stats",
                "description": "获取 QMT 数据统计信息",
                "inputSchema": {
                    "type": "object",
                    "properties": {}
                }
            },
            {
                "name": "qmt_query_data",
                "description": "查询 QMT 数据",
                "inputSchema": {
                    "type": "object",
                    "properties": {
                        "table": {"type": "string", "description": "表名"},
                        "limit": {"type": "integer", "description": "返回条数", "default": 100},
                        "where": {"type": "string", "description": "查询条件"}
                    },
                    "required": ["table"]
                }
            },
            {
                "name": "qmt_sync_sector_list",
                "description": "同步板块列表",
                "inputSchema": {
                    "type": "object",
                    "properties": {}
                }
            },
            {
                "name": "qmt_sync_stock_list",
                "description": "同步股票列表",
                "inputSchema": {
                    "type": "object",
                    "properties": {}
                }
            },
            {
                "name": "qmt_sync_etf_list",
                "description": "同步ETF列表",
                "inputSchema": {
                    "type": "object",
                    "properties": {}
                }
            },
            {
                "name": "qmt_sync_index_weight",
                "description": "同步指数成分股",
                "inputSchema": {
                    "type": "object",
                    "properties": {
                        "indexes": {"type": "array", "items": {"type": "string"}, "description": "指数代码列表"}
                    }
                }
            },
            {
                "name": "qmt_sync_kline",
                "description": "同步K线数据",
                "inputSchema": {
                    "type": "object",
                    "properties": {
                        "sec_codes": {"type": "array", "items": {"type": "string"}, "description": "证券代码"},
                        "days": {"type": "integer", "description": "天数", "default": 30}
                    },
                    "required": ["sec_codes"]
                }
            },
        ]
    
    def execute_tool(self, tool_name: str, params: Dict) -> Any:
        """执行工具"""
        try:
            if tool_name == "qmt_list_tables":
                db = get_qmt_db()
                tables = db.get_tables()
                return {"tables": tables}
            
            elif tool_name == "qmt_get_stats":
                db = get_qmt_db()
                tables = db.get_tables()
                stats = {t: db.get_table_count(t) for t in tables}
                return {"stats": stats}
            
            elif tool_name == "qmt_query_data":
                db = get_qmt_db()
                table = params.get("table")
                limit = params.get("limit", 100)
                where = params.get("where")
                
                if not table:
                    return {"error": "table is required"}
                
                tables = db.get_tables()
                if table not in tables:
                    return {"error": f"table {table} not found"}
                
                cols = "*"
                sql = f"SELECT {cols} FROM {table}"
                if where:
                    sql += f" WHERE {where}"
                sql += f" LIMIT {limit}"
                
                df = db.query(sql)
                return {"data": df.to_dict(orient="records"), "count": len(df)}
            
            elif tool_name == "qmt_sync_sector_list":
                scheduler = get_qmt_scheduler()
                return scheduler.sync_sector_list()
            
            elif tool_name == "qmt_sync_stock_list":
                scheduler = get_qmt_scheduler()
                return scheduler.sync_stock_list()
            
            elif tool_name == "qmt_sync_etf_list":
                scheduler = get_qmt_scheduler()
                return scheduler.sync_etf_list()
            
            elif tool_name == "qmt_sync_index_weight":
                scheduler = get_qmt_scheduler()
                indexes = params.get("indexes")
                return scheduler.sync_index_weight(indexes)
            
            elif tool_name == "qmt_sync_kline":
                scheduler = get_qmt_scheduler()
                sec_codes = params.get("sec_codes", [])
                days = params.get("days", 30)
                return scheduler.sync_kline(sec_codes, days)
            
            else:
                return {"error": f"unknown tool: {tool_name}"}
        
        except Exception as e:
            logger.error(f"QMT MCP 执行失败: {e}")
            return {"error": str(e)}
    
    def handle_request(self, request: MCPRequest) -> MCPResponse:
        """处理 MCP 请求"""
        try:
            # 初始化
            if request.method == "initialize":
                return MCPResponse(
                    jsonrpc="2.0",
                    id=request.id,
                    result={
                        "protocolVersion": "2024-11-05",
                        "capabilities": {"tools": {"listChanged": True}},
                        "serverInfo": {"name": "QMT MCP Server", "version": "1.0.0"}
                    }
                )
            
            # 工具列表
            elif request.method in ["tools/list", "tool.list"]:
                return MCPResponse(
                    jsonrpc="2.0",
                    id=request.id,
                    result={"tools": self.get_tools()}
                )
            
            # 工具调用
            elif request.method in ["tools/call", "tool.call"]:
                tool_name = request.params.get("tool") or request.params.get("name")
                tool_params = request.params.get("parameters", {}) or request.params.get("arguments", {})
                result = self.execute_tool(tool_name, tool_params)
                return MCPResponse(jsonrpc="2.0", id=request.id, result=result)
            
            # Ping
            elif request.method == "ping":
                return MCPResponse(jsonrpc="2.0", id=request.id, result={"status": "ok"})
            
            else:
                return MCPResponse(
                    jsonrpc="2.0",
                    id=request.id,
                    error={"code": -32601, "message": f"方法未找到: {request.method}"}
                )
        
        except Exception as e:
            logger.error(f"MCP 请求处理失败: {e}")
            return MCPResponse(
                jsonrpc="2.0",
                id=request.id,
                error={"code": -32603, "message": str(e)}
            )


# ==================== MCP 端点 ====================

@qmt_mcp_app.get("/")
async def mcp_root():
    """MCP 根路径"""
    service = get_qmt_mcp_service()
    return {
        "name": "QMT MCP Server",
        "version": "1.0.0",
        "protocolVersion": "2024-11-05",
        "capabilities": {"tools": {"listChanged": True}},
        "serverInfo": {"name": "QMT MCP Server", "version": "1.0.0"}
    }


@qmt_mcp_app.post("/mcp")
async def handle_mcp(request: MCPRequest):
    """处理 MCP JSON-RPC 请求"""
    service = get_qmt_mcp_service()
    return service.handle_request(request)


@qmt_mcp_app.get("/tools")
async def list_tools():
    """列出所有工具"""
    service = get_qmt_mcp_service()
    return {"tools": service.get_tools()}


@qmt_mcp_app.post("/tools/{tool_name}")
async def call_tool(tool_name: str, params: Dict[str, Any] = {}):
    """调用工具 (RESTful)"""
    service = get_qmt_mcp_service()
    try:
        result = service.execute_tool(tool_name, params)
        return {"success": True, "result": result}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@qmt_mcp_app.get("/sse")
async def sse_endpoint(request: Request):
    """SSE 端点"""
    async def event_stream():
        yield f"event: connected\ndata: {json.dumps({'status': 'connected'})}\n\n"
        try:
            while True:
                await asyncio.sleep(30)
                yield f"event: ping\ndata: {json.dumps({'time': datetime.now().isoformat()})}\n\n"
        except asyncio.CancelledError:
            pass
    
    return StreamingResponse(
        event_stream(),
        media_type="text/event-stream",
        headers={"Cache-Control": "no-cache", "Connection": "keep-alive"}
    )


# ==================== 启动 MCP 服务 ====================

def start_qmt_mcp_server(port: int = 8003):
    """启动 QMT MCP 服务"""
    logger.info(f"启动 QMT MCP 服务: http://0.0.0.0:{port}")
    uvicorn.run(qmt_mcp_app, host="0.0.0.0", port=port, log_level="info")


if __name__ == "__main__":
    start_qmt_mcp_server()
