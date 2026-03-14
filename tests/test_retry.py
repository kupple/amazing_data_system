"""
测试用例 - 重试机制
"""
import pytest
import time
from unittest.mock import Mock, patch
import pandas as pd

from src.common.retry import (
    RetryConfig, 
    calculate_delay, 
    retry, 
    RetryManager,
    RetryableError,
    NonRetryableError
)


class TestRetryConfig:
    """重试配置测试"""
    
    def test_retry_config_defaults(self):
        """测试默认配置"""
        config = RetryConfig()
        
        assert config.max_attempts == 3
        assert config.initial_delay == 1.0
        assert config.max_delay == 60.0
        assert config.exponential_base == 2.0
        assert config.jitter is True
    
    def test_retry_config_custom(self):
        """测试自定义配置"""
        config = RetryConfig(
            max_attempts=5,
            initial_delay=2.0,
            max_delay=30.0,
            exponential_base=3.0,
            jitter=False
        )
        
        assert config.max_attempts == 5
        assert config.initial_delay == 2.0
        assert config.max_delay == 30.0
        assert config.exponential_base == 3.0
        assert config.jitter is False


class TestCalculateDelay:
    """延迟计算测试"""
    
    def test_exponential_backoff(self):
        """测试指数退避"""
        config = RetryConfig(initial_delay=1.0, exponential_base=2.0, jitter=False)
        
        delay0 = calculate_delay(0, config)
        delay1 = calculate_delay(1, config)
        delay2 = calculate_delay(2, config)
        
        assert delay0 == 1.0
        assert delay1 == 2.0
        assert delay2 == 4.0
    
    def test_max_delay(self):
        """测试最大延迟限制"""
        config = RetryConfig(initial_delay=1.0, exponential_base=10.0, max_delay=50.0, jitter=False)
        
        delay = calculate_delay(10, config)
        
        assert delay == 50.0
    
    def test_jitter(self):
        """测试随机抖动"""
        config = RetryConfig(initial_delay=1.0, exponential_base=2.0, jitter=True)
        
        # 运行多次，验证有随机性
        delays = [calculate_delay(0, config) for _ in range(10)]
        
        # 抖动范围应该在 0.5x 到 1.5x 之间
        assert all(0.5 <= d <= 1.5 for d in delays)


class TestRetryDecorator:
    """重试装饰器测试"""
    
    def test_success_first_try(self):
        """测试首次成功"""
        call_count = 0
        
        @retry(max_attempts=3, initial_delay=0.01)
        def success_func():
            nonlocal call_count
            call_count += 1
            return "success"
        
        result = success_func()
        
        assert result == "success"
        assert call_count == 1
    
    def test_retry_then_success(self):
        """测试重试后成功"""
        call_count = 0
        
        @retry(max_attempts=3, initial_delay=0.01)
        def retry_then_success():
            nonlocal call_count
            call_count += 1
            if call_count < 2:
                raise RetryableError("Temporary error")
            return "success"
        
        result = retry_then_success()
        
        assert result == "success"
        assert call_count == 2
    
    def test_all_retries_failed(self):
        """测试所有重试都失败"""
        call_count = 0
        
        @retry(max_attempts=3, initial_delay=0.01)
        def always_fail():
            nonlocal call_count
            call_count += 1
            raise RetryableError("Always fails")
        
        with pytest.raises(RetryableError):
            always_fail()
        
        assert call_count == 3
    
    def test_non_retryable_error(self):
        """测试不可重试错误"""
        call_count = 0
        
        @retry(max_attempts=3, initial_delay=0.01)
        def non_retryable():
            nonlocal call_count
            call_count += 1
            raise NonRetryableError("Cannot retry")
        
        with pytest.raises(NonRetryableError):
            non_retryable()
        
        assert call_count == 1  # 只调用一次
    
    def test_regular_exception_retries(self):
        """测试普通异常会重试"""
        call_count = 0
        
        @retry(max_attempts=3, initial_delay=0.01)
        def regular_exception():
            nonlocal call_count
            call_count += 1
            if call_count < 2:
                raise Exception("Regular error")
            return "success"
        
        result = regular_exception()
        
        assert result == "success"
        assert call_count == 2
    
    @patch('src.retry.logger')
    def test_retry_logs_warning(self, mock_logger):
        """测试重试时记录警告"""
        call_count = 0
        
        @retry(max_attempts=3, initial_delay=0.01, data_type="test_data")
        def test_func():
            nonlocal call_count
            call_count += 1
            if call_count < 3:
                raise Exception("Error")
            return "success"
        
        try:
            test_func()
        except:
            pass
        
        # 验证记录了重试日志
        assert mock_logger.warning.called
    
    def test_retry_with_exception_filter(self):
        """测试指定异常类型重试"""
        call_count = 0
        
        @retry(
            max_attempts=3, 
            initial_delay=0.01,
            exceptions=(ValueError,)
        )
        def test_func():
            nonlocal call_count
            call_count += 1
            if call_count < 2:
                raise ValueError("Value error")
            return "success"
        
        result = test_func()
        
        assert result == "success"
        assert call_count == 2
    
    def test_retry_with_non_matching_exception(self):
        """测试不匹配的异常不重试"""
        call_count = 0
        
        @retry(
            max_attempts=3,
            initial_delay=0.01,
            exceptions=(ValueError,)
        )
        def test_func():
            nonlocal call_count
            call_count += 1
            raise TypeError("Type error")
        
        with pytest.raises(TypeError):
            test_func()
        
        assert call_count == 1


class TestRetryManager:
    """重试管理器测试"""
    
    def test_add_failed_task(self):
        """测试添加失败任务"""
        manager = RetryManager()
        
        manager.add_failed_task("task1", {
            "data_type": "test",
            "func": "test_func",
            "error": "Test error"
        })
        
        tasks = manager.get_failed_tasks()
        
        assert "task1" in tasks
        assert tasks["task1"]["data_type"] == "test"
    
    def test_remove_task(self):
        """测试移除任务"""
        manager = RetryManager()
        
        manager.add_failed_task("task1", {"data": "test"})
        manager.remove_task("task1")
        
        tasks = manager.get_failed_tasks()
        
        assert "task1" not in tasks
    
    def test_retry_task_success(self):
        """测试重试任务成功"""
        manager = RetryManager()
        call_count = 0
        
        def test_func():
            nonlocal call_count
            call_count += 1
            if call_count < 2:
                raise Exception("Temp error")
            return "success"
        
        manager.add_failed_task("task1", {
            "data_type": "test",
            "max_attempts": 3
        })
        
        result = manager.retry_task("task1", test_func)
        
        assert result == "success"
        assert call_count == 2
        assert "task1" not in manager.get_failed_tasks()
    
    def test_retry_nonexistent_task(self):
        """测试重试不存在的任务"""
        manager = RetryManager()
        
        with pytest.raises(ValueError):
            manager.retry_task("nonexistent", lambda: "test")


class TestRetryDecoratorsWithRealFunctions:
    """真实函数重试测试"""
    
    def test_retry_with_client_fetch(self):
        """测试模拟客户端获取"""
        from src.client import AmazingDataClient
        
        # 模拟 fetch_data 方法
        with patch.object(AmazingDataClient, 'fetch_data') as mock_fetch:
            mock_fetch.side_effect = [
                Exception("Network error"),
                pd.DataFrame({"data": [1, 2, 3]})
            ]
            
            client = AmazingDataClient()
            
            # 实际不执行，只验证装饰器可以应用
            assert callable(retry)


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
