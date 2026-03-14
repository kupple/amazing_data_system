"""
AmazingData 数据管理系统入口
"""
import argparse
import sys
import signal
from typing import Optional

from src.config import config
from src.logger import logger
from src.client import get_client, close_client
from src.database import get_db, close_db
from src.scheduler import get_scheduler, start_scheduler, stop_scheduler
from src.api import app as api_app
from src.mcp import mcp_app, start_mcp_server
from src.retry import retry_manager


class AmazingDataSystem:
    """AmazingData 数据管理系统"""
    
    def __init__(self):
        self.running = False
        self.api_process: Optional[object] = None
        self.mcp_process: Optional[object] = None
        
        # 注册信号处理器
        signal.signal(signal.SIGINT, self._signal_handler)
        signal.signal(signal.SIGTERM, self._signal_handler)
    
    def _signal_handler(self, signum, frame):
        """信号处理器"""
        logger.info("收到退出信号，正在关闭系统...")
        self.stop()
        sys.exit(0)
    
    def start(self, enable_api: bool = True, enable_mcp: bool = True, enable_scheduler: bool = True):
        """
        启动系统
        
        Args:
            enable_api: 是否启动 API 服务
            enable_mcp: 是否启动 MCP 服务
            enable_scheduler: 是否启动定时任务
        """
        logger.info("=" * 50)
        logger.info("AmazingData 数据管理系统启动中...")
        logger.info("=" * 50)
        
        # 测试数据库连接
        try:
            db = get_db()
            logger.info(f"数据库连接成功: {config.database.db_path}")
        except Exception as e:
            logger.error(f"数据库连接失败: {e}")
            return False
        
        # 测试 AmazingData 连接
        try:
            client = get_client()
            if client.login():
                logger.info("AmazingData 连接成功")
            else:
                logger.warning("AmazingData 连接失败，将稍后重试")
        except Exception as e:
            logger.warning(f"AmazingData 初始化失败: {e}")
        
        # 启动定时任务
        if enable_scheduler and config.scheduler.enabled:
            try:
                start_scheduler()
            except Exception as e:
                logger.error(f"定时任务启动失败: {e}")
        
        self.running = True
        
        # 启动 API 服务
        if enable_api:
            self._start_api()
        
        # 启动 MCP 服务
        if enable_mcp and config.mcp.enabled:
            self._start_mcp()
        
        logger.info("=" * 50)
        logger.info("系统启动完成!")
        logger.info("=" * 50)
        
        # 保持运行
        try:
            import time
            while self.running:
                time.sleep(1)
        except KeyboardInterrupt:
            self.stop()
    
    def _start_api(self):
        """启动 API 服务"""
        try:
            import uvicorn
            import threading
            
            def run_api():
                uvicorn.run(
                    api_app,
                    host=config.api.host,
                    port=config.api.port,
                    log_level="info"
                )
            
            thread = threading.Thread(target=run_api, daemon=True)
            thread.start()
            
            logger.info(f"API 服务已启动: http://{config.api.host}:{config.api.port}")
        except Exception as e:
            logger.error(f"API 服务启动失败: {e}")
    
    def _start_mcp(self):
        """启动 MCP 服务"""
        try:
            import threading
            
            def run_mcp():
                uvicorn.run(
                    mcp_app,
                    host="0.0.0.0",
                    port=config.mcp.port,
                    log_level="info"
                )
            
            thread = threading.Thread(target=run_mcp, daemon=True)
            thread.start()
            
            logger.info(f"MCP 服务已启动: http://0.0.0.0:{config.mcp.port}")
        except Exception as e:
            logger.error(f"MCP 服务启动失败: {e}")
    
    def stop(self):
        """停止系统"""
        logger.info("正在停止系统...")
        
        # 停止定时任务
        try:
            stop_scheduler()
        except:
            pass
        
        # 关闭数据库
        try:
            close_db()
        except:
            pass
        
        # 关闭客户端
        try:
            close_client()
        except:
            pass
        
        logger.info("系统已停止")
        self.running = False
    
    def run_once(self, data_type: str, **kwargs):
        """单次运行数据同步"""
        from src.scheduler import get_scheduler
        from src.models import DataSource
        
        # 获取数据源
        ds = DataSource(data_type)
        
        # 执行同步
        scheduler = get_scheduler()
        result = scheduler.fetcher.fetch_and_save(ds, **kwargs)
        
        logger.info(f"同步结果: {result}")
        return result


def main():
    """主函数"""
    parser = argparse.ArgumentParser(description="AmazingData 数据管理系统")
    
    parser.add_argument(
        "--mode", 
        choices=["server", "api", "mcp", "qmt", "qmt-mcp", "sync", "once"],
        default="server",
        help="运行模式 (server=完整服务, api=仅API, mcp=仅MCP, qmt=仅QMT API, qmt-mcp=仅QMT MCP)"
    )
    parser.add_argument(
        "--no-api",
        action="store_true",
        help="不启动 API 服务"
    )
    parser.add_argument(
        "--no-mcp",
        action="store_true",
        help="不启动 MCP 服务"
    )
    parser.add_argument(
        "--no-scheduler",
        action="store_true",
        help="不启动定时任务"
    )
    parser.add_argument(
        "--data-type",
        type=str,
        help="单次同步的数据类型"
    )
    parser.add_argument(
        "--port",
        type=int,
        default=8000,
        help="API 服务端口"
    )
    
    args = parser.parse_args()
    
    if args.mode == "server":
        # 启动完整服务
        system = AmazingDataSystem()
        system.start(
            enable_api=not args.no_api,
            enable_mcp=not args.no_mcp,
            enable_scheduler=not args.no_scheduler
        )
    
    elif args.mode == "api":
        # 仅启动 API
        import uvicorn
        uvicorn.run(api_app, host="0.0.0.0", port=args.port)
    
    elif args.mode == "mcp":
        # 仅启动 MCP
        start_mcp_server()
    
    elif args.mode == "qmt":
        # 仅启动 QMT API
        import uvicorn
        from src.api import qmt_app
        logger.info(f"启动 QMT API 服务: http://0.0.0.0:8002")
        uvicorn.run(qmt_app, host="0.0.0.0", port=8002)
    
    elif args.mode == "qmt-mcp":
        # 启动 QMT MCP 服务
        from src.qmt.mcp import start_qmt_mcp_server
        start_qmt_mcp_server(port=8003)
    
    elif args.mode == "sync":
        # 手动触发同步
        scheduler = get_scheduler()
        scheduler.start()
        
        # 保持运行
        import time
        try:
            while True:
                time.sleep(1)
        except KeyboardInterrupt:
            scheduler.stop()
    
    elif args.mode == "once":
        # 单次同步
        if not args.data_type:
            print("错误: --data-type 必须指定")
            sys.exit(1)
        
        system = AmazingDataSystem()
        system.run_once(args.data_type)


if __name__ == "__main__":
    main()
