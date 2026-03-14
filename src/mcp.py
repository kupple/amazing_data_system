"""
MCP (Model Context Protocol) 服务模块
支持通过 MCP 协议调用触发
"""
import json
import asyncio
from typing import Optional, Dict, Any, List, Union, Callable
from datetime import datetime
from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import JSONResponse, StreamingResponse
from pydantic import BaseModel
import uvicorn

from src.config import config
from src.logger import logger
from src.database import get_db, DuckDBManager


# MCP 协议类型
class MCPRequest(BaseModel):
    """MCP 请求"""
    jsonrpc: str = "2.0"
    id: Optional[int] = None
    method: str
    params: Optional[Dict[str, Any]] = None


class MCPResponse(BaseModel):
    """MCP 响应"""
    jsonrpc: str = "2.0"
    id: Optional[int] = None
    result: Optional[Any] = None
    error: Optional[Dict[str, Any]] = None


class MCPTool:
    """MCP 工具定义"""
    def __init__(self, name: str, description: str, input_schema: Dict[str, Any]):
        self.name = name
        self.description = description
        self.input_schema = input_schema


class MCPService:
    """MCP 服务"""
    
    def __init__(self):
        self.db: Optional[DuckDBManager] = None
        self.tools: Dict[str, MCPTool] = {}
        self._register_tools()
    
    def _register_tools(self):
        """注册 MCP 工具"""
        
        # 数据库查询工具
        self.register_tool(MCPTool(
            name="query_data",
            description="查询数据库中的金融数据",
            input_schema={
                "type": "object",
                "properties": {
                    "table": {"type": "string", "description": "表名"},
                    "columns": {"type": "string", "description": "查询列"},
                    "where": {"type": "string", "description": "WHERE 条件"},
                    "limit": {"type": "integer", "description": "返回条数"}
                },
                "required": ["table"]
            }
        ))
        
        # 获取表列表
        self.register_tool(MCPTool(
            name="list_tables",
            description="列出所有可用的数据表",
            input_schema={
                "type": "object",
                "properties": {}
            }
        ))
        
        # 获取同步状态
        self.register_tool(MCPTool(
            name="get_sync_status",
            description="获取数据同步状态",
            input_schema={
                "type": "object",
                "properties": {
                    "data_type": {"type": "string", "description": "数据类型"}
                }
            }
        ))
        
        # 获取行情数据
        self.register_tool(MCPTool(
            name="get_quote",
            description="获取证券实时行情",
            input_schema={
                "type": "object",
                "properties": {
                    "sec_code": {"type": "string", "description": "证券代码"}
                },
                "required": ["sec_code"]
            }
        ))
        
        # 获取K线数据
        self.register_tool(MCPTool(
            name="get_kline",
            description="获取K线数据",
            input_schema={
                "type": "object",
                "properties": {
                    "sec_code": {"type": "string", "description": "证券代码"},
                    "kline_type": {"type": "string", "description": "K线类型"},
                    "limit": {"type": "integer", "description": "返回条数"}
                },
                "required": ["sec_code"]
            }
        ))
        
        # 获取财务数据
        self.register_tool(MCPTool(
            name="get_financial_data",
            description="获取财务数据",
            input_schema={
                "type": "object",
                "properties": {
                    "sec_code": {"type": "string", "description": "证券代码"},
                    "data_type": {"type": "string", "description": "数据类型"},
                    "limit": {"type": "integer", "description": "返回条数"}
                },
                "required": ["sec_code"]
            }
        ))
        
        # 获取系统统计
        self.register_tool(MCPTool(
            name="get_stats",
            description="获取系统统计信息",
            input_schema={
                "type": "object",
                "properties": {}
            }
        ))
    
    def register_tool(self, tool: MCPTool):
        """注册工具"""
        self.tools[tool.name] = tool
    
    def get_tools(self) -> List[Dict[str, Any]]:
        """获取所有工具"""
        return [
            {
                "name": tool.name,
                "description": tool.description,
                "inputSchema": tool.input_schema
            }
            for tool in self.tools.values()
        ]
    
    def execute_tool(self, tool_name: str, params: Dict[str, Any]) -> Any:
        """执行工具"""
        if tool_name not in self.tools:
            raise ValueError(f"未知工具: {tool_name}")
        
        if self.db is None:
            self.db = get_db()
        
        # 根据工具名称执行相应操作
        if tool_name == "query_data":
            return self._query_data(params)
        elif tool_name == "list_tables":
            return self._list_tables()
        elif tool_name == "get_sync_status":
            return self._get_sync_status(params.get("data_type"))
        elif tool_name == "get_quote":
            return self._get_quote(params.get("sec_code"))
        elif tool_name == "get_kline":
            return self._get_kline(
                params.get("sec_code"),
                params.get("kline_type", "1D"),
                params.get("limit", 100)
            )
        elif tool_name == "get_financial_data":
            return self._get_financial_data(
                params.get("sec_code"),
                params.get("data_type"),
                params.get("limit", 100)
            )
        elif tool_name == "get_stats":
            return self._get_stats()
        
        raise ValueError(f"工具 {tool_name} 未实现")
    
    def _query_data(self, params: Dict[str, Any]) -> List[Dict]:
        """查询数据"""
        table = params.get("table")
        columns = params.get("columns", "*")
        where = params.get("where")
        limit = params.get("limit", 100)
        
        sql = f"SELECT {columns} FROM {table}"
        if where:
            sql += f" WHERE {where}"
        sql += f" LIMIT {limit}"
        
        df = self.db.query(sql)
        return df.to_dict(orient="records")
    
    def _list_tables(self) -> List[Dict]:
        """列出表"""
        tables = self.db.get_tables()
        result = []
        for table in tables:
            count = self.db.get_table_count(table)
            result.append({"name": table, "record_count": count})
        return result
    
    def _get_sync_status(self, data_type: Optional[str]) -> Union[Dict, List[Dict]]:
        """获取同步状态"""
        return self.db.get_sync_status(data_type)
    
    def _get_quote(self, sec_code: str) -> List[Dict]:
        """获取行情"""
        tables = ["stock_snapshot", "index_snapshot", "etf_snapshot", "cb_snapshot"]
        
        for table in tables:
            if self.db.table_exists(table):
                df = self.db.query(f"SELECT * FROM {table} WHERE sec_code = '{sec_code}' LIMIT 1")
                if not df.empty:
                    return df.to_dict(orient="records")
        
        return []
    
    def _get_kline(self, sec_code: str, kline_type: str, limit: int) -> List[Dict]:
        """获取K线"""
        table_name = f"kline_{kline_type}"
        
        if not self.db.table_exists(table_name):
            # 尝试通用表
            table_name = "historical_kline"
        
        df = self.db.query(f"""
            SELECT * FROM {table_name}
            WHERE sec_code = '{sec_code}'
            ORDER BY trade_time DESC
            LIMIT {limit}
        """)
        
        return df.to_dict(orient="records")
    
    def _get_financial_data(self, sec_code: str, data_type: Optional[str], 
                           limit: int) -> List[Dict]:
        """获取财务数据"""
        table_map = {
            "balance_sheet": "balance_sheet",
            "cash_flow": "cash_flow",
            "income": "income",
            "express": "express_report",
            "forecast": "forecast_report"
        }
        
        table = table_map.get(data_type, data_type) if data_type else "balance_sheet"
        
        if not self.db.table_exists(table):
            return []
        
        df = self.db.query(f"""
            SELECT * FROM {table}
            WHERE sec_code = '{sec_code}'
            ORDER BY report_date DESC
            LIMIT {limit}
        """)
        
        return df.to_dict(orient="records")
    
    def _get_stats(self) -> Dict:
        """获取统计"""
        tables = self.db.get_tables()
        total_records = sum(self.db.get_table_count(t) for t in tables)
        
        return {
            "total_tables": len(tables),
            "total_records": total_records,
            "sync_status": self.db.get_sync_status()
        }
    
    def handle_request(self, request: MCPRequest) -> MCPResponse:
        """处理 MCP JSON-RPC 请求"""
        try:
            # MCP 协议初始化
            if request.method == "initialize":
                return MCPResponse(
                    jsonrpc="2.0",
                    id=request.id,
                    result={
                        "protocolVersion": "2024-11-05",
                        "capabilities": {
                            "tools": {"listChanged": True},
                            "resources": {"subscribe": True, "listChanged": True}
                        },
                        "serverInfo": {
                            "name": "AmazingData MCP Server",
                            "version": "1.0.0"
                        }
                    }
                )
            
            # 工具列表
            elif request.method in ["tools/list", "tool.list"]:
                result = self.get_tools()
                return MCPResponse(
                    jsonrpc="2.0",
                    id=request.id,
                    result={"tools": result}
                )
            
            # 工具调用
            elif request.method in ["tools/call", "tool.call"]:
                # 支持两种参数格式
                tool_name = request.params.get("tool") or request.params.get("name")
                tool_params = request.params.get("parameters", {}) or request.params.get("arguments", {})
                
                result = self.execute_tool(tool_name, tool_params)
                
                return MCPResponse(
                    jsonrpc="2.0",
                    id=request.id,
                    result=result
                )
            
            # 资源列表
            elif request.method in ["resources/list", "resource.list"]:
                if self.db is None:
                    self.db = get_db()
                tables = self.db.get_tables() if self.db else []
                return MCPResponse(
                    jsonrpc="2.0",
                    id=request.id,
                    result={
                        "resources": [
                            {
                                "uri": f"db://{table}",
                                "name": table,
                                "description": f"数据库表: {table}",
                                "mimeType": "application/json"
                            }
                            for table in tables
                        ]
                    }
                )
            
            # 资源读取
            elif request.method in ["resources/read", "resource.read"]:
                uri = request.params.get("uri", "")
                if uri.startswith("db://"):
                    table = uri[5:]
                    if self.db is None:
                        self.db = get_db()
                    df = self.db.query(f"SELECT * FROM {table} LIMIT 100")
                    return MCPResponse(
                        jsonrpc="2.0",
                        id=request.id,
                        result={
                            "contents": [{
                                "uri": uri,
                                "mimeType": "application/json",
                                "text": df.to_json(orient="records")
                            }]
                        }
                    )
            
            # Ping
            elif request.method == "ping":
                return MCPResponse(
                    jsonrpc="2.0",
                    id=request.id,
                    result={"status": "ok"}
                )
            
            else:
                return MCPResponse(
                    jsonrpc="2.0",
                    id=request.id,
                    error={
                        "code": -32601,
                        "message": f"方法未找到: {request.method}"
                    }
                )
        
        except Exception as e:
            logger.error(f"MCP 请求处理失败: {e}")
            return MCPResponse(
                jsonrpc="2.0",
                id=request.id,
                error={
                    "code": -32603,
                    "message": str(e)
                }
            )


# 创建 MCP 应用
mcp_app = FastAPI(title="AmazingData MCP")
mcp_service = MCPService()


# MCP 协议端点 - 支持 JSON-RPC 2.0
@mcp_app.get("/")
async def mcp_root():
    """MCP 根路径 - 返回服务器信息"""
    return {
        "name": "AmazingData MCP",
        "version": "1.0.0",
        "protocolVersion": "2024-11-05",
        "capabilities": {
            "tools": {
                "listChanged": True
            },
            "resources": {
                "subscribe": True,
                "listChanged": True
            }
        },
        "serverInfo": {
            "name": "AmazingData MCP Server",
            "version": "1.0.0"
        }
    }


@mcp_app.post("/mcp")
async def handle_mcp(request: MCPRequest):
    """处理 MCP JSON-RPC 请求"""
    return mcp_service.handle_request(request)


# SSE 端点 - 用于支持 MCP Client 长连接
@mcp_app.get("/sse")
async def sse_endpoint(request: Request):
    """SSE 端点，用于 MCP Client 连接"""
    async def event_stream():
        # 发送初始连接消息
        yield f"event: connected\ndata: {json.dumps({'status': 'connected'})}\n\n"
        
        # 保持连接，可以发送增量更新
        try:
            while True:
                await asyncio.sleep(30)
                yield f"event: ping\ndata: {json.dumps({'time': datetime.now().isoformat()})}\n\n"
        except asyncio.CancelledError:
            pass
    
    return StreamingResponse(
        event_stream(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "Connection": "keep-alive",
        }
    )


# 工具调用端点 (RESTful 风格)
@mcp_app.get("/tools")
async def list_tools():
    """列出所有工具"""
    return {"tools": mcp_service.get_tools()}


@mcp_app.post("/tools/{tool_name}")
async def call_tool(tool_name: str, params: Dict[str, Any] = {}):
    """调用工具"""
    try:
        result = mcp_service.execute_tool(tool_name, params)
        return {"success": True, "result": result}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# 资源端点
@mcp_app.get("/resources")
async def list_resources():
    """列出所有资源"""
    if mcp_service.db is None:
        mcp_service.db = get_db()
    tables = mcp_service.db.get_tables()
    return {
        "resources": [
            {
                "uri": f"db://{table}",
                "name": table,
                "description": f"数据库表: {table}",
                "mimeType": "application/json"
            }
            for table in tables
        ]
    }


@mcp_app.get("/resources/{resource_uri:path}")
async def read_resource(resource_uri: str):
    """读取资源内容"""
    if mcp_service.db is None:
        mcp_service.db = get_db()
    
    # 解析 URI db://table
    if resource_uri.startswith("db://"):
        table = resource_uri[5:]
        try:
            df = mcp_service.db.query(f"SELECT * FROM {table} LIMIT 100")
            return {"content": df.to_dict(orient="records")}
        except Exception as e:
            raise HTTPException(status_code=404, detail=str(e))
    
    raise HTTPException(status_code=404, detail="Resource not found")


def start_mcp_server():
    """启动 MCP 服务器"""
    if not config.mcp.enabled:
        logger.info("MCP 服务未启用")
        return
    
    uvicorn.run(
        mcp_app,
        host="0.0.0.0",
        port=config.mcp.port,
        log_level="info"
    )


if __name__ == "__main__":
    start_mcp_server()
