"""
日志系统模块
"""
import os
import logging
import json
from datetime import datetime, date
from pathlib import Path
from typing import Optional, Dict, Any
from logging.handlers import RotatingFileHandler
from src.common.config import config


class DateEncoder(json.JSONEncoder):
    """JSON 日期编码器"""
    def default(self, obj):
        if isinstance(obj, (datetime, date)):
            return obj.isoformat()
        return super().default(obj)


class Logger:
    """日志系统"""
    
    def __init__(self, name: str = "amazing_data"):
        self.name = name
        self.log_dir = Path(config.log.log_dir)
        self.log_dir.mkdir(parents=True, exist_ok=True)
        
        # 主日志器
        self.logger = logging.getLogger(name)
        self.logger.setLevel(getattr(logging, config.log.log_level))
        
        # 避免重复添加 handler
        if not self.logger.handlers:
            # 控制台输出
            console_handler = logging.StreamHandler()
            console_handler.setLevel(logging.INFO)
            console_formatter = logging.Formatter(
                '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
            )
            console_handler.setFormatter(console_formatter)
            self.logger.addHandler(console_handler)
            
            # 文件输出 - 主日志
            main_log_file = self.log_dir / f"{name}.log"
            file_handler = RotatingFileHandler(
                main_log_file,
                maxBytes=10*1024*1024,  # 10MB
                backupCount=config.log.max_log_files,
                encoding='utf-8'
            )
            file_handler.setLevel(logging.DEBUG)
            file_formatter = logging.Formatter(
                '%(asctime)s - %(name)s - %(levelname)s - %(funcName)s:%(lineno)d - %(message)s'
            )
            file_handler.setFormatter(file_formatter)
            self.logger.addHandler(file_handler)
        
        # 每日成功/失败记录
        self._daily_records: Dict[str, Any] = {}
    
    def info(self, message: str, **kwargs):
        """记录信息"""
        self.logger.info(message, extra=kwargs)
    
    def debug(self, message: str, **kwargs):
        """记录调试信息"""
        self.logger.debug(message, extra=kwargs)
    
    def warning(self, message: str, **kwargs):
        """记录警告"""
        self.logger.warning(message, extra=kwargs)
    
    def error(self, message: str, **kwargs):
        """记录错误"""
        self.logger.error(message, extra=kwargs)
    
    def critical(self, message: str, **kwargs):
        """记录严重错误"""
        self.logger.critical(message, extra=kwargs)
    
    def log_fetch(self, data_type: str, success: bool, record_count: int = 0, 
                  error_message: Optional[str] = None, **kwargs):
        """
        记录数据获取结果
        
        Args:
            data_type: 数据类型
            success: 是否成功
            record_count: 记录数
            error_message: 错误信息
        """
        today = datetime.now().strftime("%Y-%m-%d")
        
        # 每日汇总记录
        if data_type not in self._daily_records:
            self._daily_records[data_type] = {
                "date": today,
                "success_count": 0,
                "failed_count": 0,
                "total_records": 0,
                "errors": []
            }
        
        record = self._daily_records[data_type]
        
        if success:
            record["success_count"] += 1
            record["total_records"] += record_count
            self.info(f"[{data_type}] 获取成功, 记录数: {record_count}")
        else:
            record["failed_count"] += 1
            record["errors"].append({
                "time": datetime.now().isoformat(),
                "error": error_message
            })
            self.error(f"[{data_type}] 获取失败: {error_message}")
        
        # 写入每日详细日志
        daily_log_file = self.log_dir / f"fetch_{today}.jsonl"
        log_entry = {
            "timestamp": datetime.now().isoformat(),
            "data_type": data_type,
            "success": success,
            "record_count": record_count,
            "error_message": error_message,
            **kwargs
        }
        
        with open(daily_log_file, 'a', encoding='utf-8') as f:
            f.write(json.dumps(log_entry, ensure_ascii=False, cls=DateEncoder) + "\n")
        
        # 写入每日汇总
        summary_file = self.log_dir / f"summary_{today}.json"
        with open(summary_file, 'w', encoding='utf-8') as f:
            json.dump(self._daily_records, f, ensure_ascii=False, indent=2, cls=DateEncoder)
    
    def log_retry(self, data_type: str, attempt: int, max_attempts: int, 
                  error: str):
        """记录重试信息"""
        self.warning(f"[{data_type}] 第 {attempt}/{max_attempts} 次重试, 错误: {error}")
    
    def get_daily_summary(self, date: Optional[str] = None) -> Dict[str, Any]:
        """获取每日汇总"""
        if date is None:
            date = datetime.now().strftime("%Y-%m-%d")
        
        summary_file = self.log_dir / f"summary_{date}.json"
        
        if summary_file.exists():
            with open(summary_file, 'r', encoding='utf-8') as f:
                return json.load(f)
        
        return {}
    
    def get_fetch_logs(self, date: Optional[str] = None, 
                       data_type: Optional[str] = None,
                       limit: int = 100) -> list:
        """获取获取日志"""
        if date is None:
            date = datetime.now().strftime("%Y-%m-%d")
        
        daily_log_file = self.log_dir / f"fetch_{date}.jsonl"
        
        if not daily_log_file.exists():
            return []
        
        logs = []
        with open(daily_log_file, 'r', encoding='utf-8') as f:
            for line in f:
                try:
                    log = json.loads(line)
                    if data_type is None or log.get("data_type") == data_type:
                        logs.append(log)
                except:
                    continue
        
        return logs[-limit:]


# 全局日志实例
logger = Logger()
