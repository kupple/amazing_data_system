"""
重试机制模块
"""
import time
import functools
from typing import Callable, Any, Optional, Type, Tuple
from dataclasses import dataclass
from datetime import datetime
from src.common.logger import logger
from src.common.config import config


@dataclass
class RetryConfig:
    """重试配置"""
    max_attempts: int = 3
    initial_delay: float = 1.0  # 初始延迟（秒）
    max_delay: float = 60.0     # 最大延迟（秒）
    exponential_base: float = 2.0  # 指数退避基数
    jitter: bool = True  # 是否添加随机抖动


class RetryableError(Exception):
    """可重试的错误"""
    pass


class NonRetryableError(Exception):
    """不可重试的错误"""
    pass


def calculate_delay(attempt: int, config: RetryConfig) -> float:
    """
    计算延迟时间
    
    使用指数退避算法: delay = initial_delay * (exponential_base ^ attempt)
    """
    delay = config.initial_delay * (config.exponential_base ** attempt)
    delay = min(delay, config.max_delay)
    
    if config.jitter:
        import random
        delay = delay * (0.5 + random.random())
    
    return delay


def retry(
    max_attempts: Optional[int] = None,
    initial_delay: Optional[float] = None,
    exceptions: Optional[Tuple[Type[Exception], ...]] = None,
    on_retry: Optional[Callable] = None,
    data_type: Optional[str] = None
):
    """
    重试装饰器
    
    Args:
        max_attempts: 最大尝试次数
        initial_delay: 初始延迟（秒）
        exceptions: 需要重试的异常类型元组
        on_retry: 每次重试前的回调函数
        data_type: 数据类型（用于日志）
    """
    if max_attempts is None:
        max_attempts = config.scheduler.retry_times
    
    if initial_delay is None:
        initial_delay = 1.0
    
    if exceptions is None:
        exceptions = (Exception,)
    
    retry_config = RetryConfig(
        max_attempts=max_attempts,
        initial_delay=initial_delay
    )
    
    def decorator(func: Callable) -> Callable:
        @functools.wraps(func)
        def wrapper(*args, **kwargs) -> Any:
            last_exception = None
            
            for attempt in range(max_attempts):
                try:
                    return func(*args, **kwargs)
                except exceptions as e:
                    last_exception = e
                    
                    # 判断是否为可重试错误
                    if isinstance(e, NonRetryableError):
                        logger.error(f"[{data_type or func.__name__}] 不可重试错误: {e}")
                        raise
                    
                    # 判断是否还有重试机会
                    if attempt < max_attempts - 1:
                        delay = calculate_delay(attempt, retry_config)
                        
                        log_msg = f"[{data_type or func.__name__}] 第 {attempt + 1}/{max_attempts} 次失败: {e}, {delay:.1f}秒后重试"
                        logger.warning(log_msg)
                        
                        # 记录重试日志
                        logger.log_retry(
                            data_type or func.__name__,
                            attempt + 1,
                            max_attempts,
                            str(e)
                        )
                        
                        # 调用重试回调
                        if on_retry:
                            on_retry(attempt, e, *args, **kwargs)
                        
                        time.sleep(delay)
                    else:
                        log_msg = f"[{data_type or func.__name__}] 达到最大重试次数 ({max_attempts}), 最终错误: {e}"
                        logger.error(log_msg)
            
            # 所有重试都失败
            raise last_exception
        
        return wrapper
    return decorator


class RetryManager:
    """重试管理器"""
    
    def __init__(self):
        self.failed_tasks: dict = {}
    
    def add_failed_task(self, task_id: str, task_info: dict):
        """添加失败任务"""
        self.failed_tasks[task_id] = {
            **task_info,
            "failed_at": datetime.now().isoformat(),
            "attempts": task_info.get("attempts", 0)
        }
    
    def get_failed_tasks(self) -> dict:
        """获取所有失败任务"""
        return self.failed_tasks
    
    def remove_task(self, task_id: str):
        """移除失败任务"""
        if task_id in self.failed_tasks:
            del self.failed_tasks[task_id]
    
    def retry_task(self, task_id: str, func: Callable, *args, **kwargs) -> Any:
        """
        重试单个任务
        
        Args:
            task_id: 任务ID
            func: 重试的函数
            *args, **kwargs: 函数参数
            
        Returns:
            函数执行结果
        """
        task_info = self.failed_tasks.get(task_id)
        if not task_info:
            raise ValueError(f"Task {task_id} not found")
        
        max_attempts = task_info.get("max_attempts", config.scheduler.retry_times)
        
        @retry(max_attempts=max_attempts, data_type=task_info.get("data_type"))
        def _retry_func():
            return func(*args, **kwargs)
        
        result = _retry_func()
        self.remove_task(task_id)
        return result


# 全局重试管理器
retry_manager = RetryManager()
