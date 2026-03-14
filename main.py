"""
多数据源数据管理系统
支持 AmazingData 和 QMT 等多个数据源
"""
import argparse
import sys
import signal
from typing import Optional

from src.common import config, logger
from src.collectors.manager import get_manager


class DataSystem:
    """数据管理系统"""
    
    def __init__(self):
        self.running = False
        self.manager = get_manager()
        
        signal.signal(signal.SIGINT, self._signal_handler)
        signal.signal(signal.SIGTERM, self._signal_handler)
    
    def _signal_handler(self, signum, frame):
        logger.info("收到退出信号，正在关闭系统...")
        self.stop()
        sys.exit(0)
    
    def initialize_collectors(self):
        """初始化所有数据采集器"""
        # 注册 Starlight
        try:
            from src.collectors.starlight import AmazingDataClient
            starlight_client = AmazingDataClient()
            self.manager.register("starlight", starlight_client)
        except Exception as e:
            logger.warning(f"Starlight 初始化失败: {e}")
        
        # 注册 MiniQMT
        try:
            from src.collectors.miniqmt import QMTClient
            miniqmt_client = QMTClient(
                qmt_path=config.qmt.qmt_path,
                account_id=config.qmt.account_id
            )
            self.manager.register("miniqmt", miniqmt_client)
        except Exception as e:
            logger.warning(f"MiniQMT 初始化失败: {e}")
    
    def start(self, source: Optional[str] = None, enable_api: bool = True, enable_scheduler: bool = True):
        """启动系统"""
        logger.info("=" * 50)
        logger.info("数据管理系统启动中...")
        logger.info("=" * 50)
        
        self.initialize_collectors()
        
        if source:
            self.manager.set_active(source)
        
        self.manager.connect_all()
        
        logger.info(f"可用数据源: {', '.join(self.manager.list_collectors())}")
        logger.info(f"当前数据源: {self.manager._active_collector}")
        
        if enable_api:
            self._start_api()
        
        if enable_scheduler:
            self._start_scheduler()
        
        self.running = True
        logger.info("系统启动完成!")
        
        import time
        try:
            while self.running:
                time.sleep(1)
        except KeyboardInterrupt:
            self.stop()
    
    def _start_api(self):
        """启动 API 服务"""
        try:
            import uvicorn
            import threading
            from src.services.api import app
            
            def run():
                uvicorn.run(app, host=config.api.host, port=config.api.port, log_level="info")
            
            threading.Thread(target=run, daemon=True).start()
            logger.info(f"API 服务: http://{config.api.host}:{config.api.port}")
        except Exception as e:
            logger.error(f"API 启动失败: {e}")
    
    def _start_scheduler(self):
        """启动定时任务"""
        if not config.scheduler.enabled:
            return
        
        try:
            from src.collectors.starlight import start_scheduler
            start_scheduler()
        except Exception as e:
            logger.error(f"定时任务启动失败: {e}")
    
    def stop(self):
        """停止系统"""
        logger.info("正在停止系统...")
        self.manager.disconnect_all()
        self.running = False
        logger.info("系统已停止")


def main():
    parser = argparse.ArgumentParser(description="多数据源数据管理系统")
    parser.add_argument("--source", choices=["starlight", "miniqmt"], help="指定数据源")
    parser.add_argument("--mode", choices=["server", "api"], default="server", help="运行模式")
    parser.add_argument("--no-api", action="store_true", help="不启动 API")
    parser.add_argument("--no-scheduler", action="store_true", help="不启动定时任务")
    
    args = parser.parse_args()
    
    system = DataSystem()
    
    if args.mode == "server":
        system.start(
            source=args.source,
            enable_api=not args.no_api,
            enable_scheduler=not args.no_scheduler
        )
    elif args.mode == "api":
        import uvicorn
        from src.services.api import app
        uvicorn.run(app, host="0.0.0.0", port=8000)


if __name__ == "__main__":
    main()
