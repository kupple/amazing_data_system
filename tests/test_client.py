"""
测试用例 - 客户端模块
"""
import pytest
from unittest.mock import Mock, patch, MagicMock
import pandas as pd
from datetime import datetime

from src.client import AmazingDataClient, get_client
from src.models import DataSource


class TestAmazingDataClient:
    """AmazingData 客户端测试"""
    
    @pytest.fixture
    def client(self):
        """创建客户端实例"""
        with patch('src.client.SDK_AVAILABLE', True):
            with patch('src.client.AmazingData') as mock_ad:
                with patch('src.client.Tgw') as mock_tgw:
                    # 设置 mock
                    mock_tgw_instance = MagicMock()
                    mock_tgw.return_value = mock_tgw_instance
                    
                    client = AmazingDataClient(
                        account="test_account",
                        password="test_password",
                        ip="127.0.0.1",
                        port=8600
                    )
                    
                    yield client
    
    def test_client_init(self):
        """测试客户端初始化"""
        client = AmazingDataClient(
            account="test_account",
            password="test_password",
            ip="127.0.0.1",
            port=8600
        )
        
        assert client.account == "test_account"
        assert client.password == "test_password"
        assert client.ip == "127.0.0.1"
        assert client.port == 8600
        assert not client.is_connected
    
    def test_client_with_config(self):
        """测试使用默认配置"""
        client = AmazingDataClient()
        
        assert client.account == "225300062129"
        assert client.password == "22530006212920260312"
        assert client.ip == "120.86.124.106"
        assert client.port == 8600
    
    @patch('src.client.SDK_AVAILABLE', False)
    def test_connect_without_sdk(self):
        """测试 SDK 未安装时连接"""
        client = AmazingDataClient()
        result = client.connect()
        
        assert result is False
        assert not client.is_connected
    
    def test_disconnect(self):
        """测试断开连接"""
        client = AmazingDataClient()
        client._connected = True
        client._client = MagicMock()
        
        client.disconnect()
        
        assert not client.is_connected
    
    def test_fetch_data_invalid_type(self):
        """测试无效数据类型"""
        client = AmazingDataClient()
        
        with pytest.raises(ValueError) as exc_info:
            client.fetch_data("invalid_type")
        
        assert "不支持" in str(exc_info.value)


class TestDataSource:
    """数据源枚举测试"""
    
    def test_data_source_values(self):
        """测试数据源值"""
        assert DataSource.SECURITY_INFO.value == "security_info"
        assert DataSource.STOCK_SNAPSHOT.value == "stock_snapshot"
        assert DataSource.BALANCE_SHEET.value == "balance_sheet"
        assert DataSource.TOP10_HOLDERS.value == "top10_holders"
        assert DataSource.MARGIN_SUMMARY.value == "margin_summary"
    
    def test_data_source_coverage(self):
        """测试数据源覆盖"""
        # 确保主要数据源都已定义
        expected_sources = [
            "SECURITY_INFO",
            "STOCK_SNAPSHOT", 
            "HISTORICAL_KLINE",
            "BALANCE_SHEET",
            "CASH_FLOW",
            "INCOME",
            "TOP10_HOLDERS",
            "MARGIN_SUMMARY",
            "DRAGON_TIGER"
        ]
        
        for source_name in expected_sources:
            assert hasattr(DataSource, source_name), f"Missing: {source_name}"


class TestRetryDecorator:
    """重试装饰器测试"""
    
    def test_retry_success(self):
        """测试重试成功"""
        from src.common.retry import retry
        
        call_count = 0
        
        @retry(max_attempts=3, initial_delay=0.1)
        def succeed_on_second():
            nonlocal call_count
            call_count += 1
            if call_count < 2:
                raise Exception("Temporary error")
            return "success"
        
        result = succeed_on_second()
        
        assert result == "success"
        assert call_count == 2
    
    def test_retry_exhausted(self):
        """测试重试耗尽"""
        from src.common.retry import retry
        
        @retry(max_attempts=3, initial_delay=0.1)
        def always_fail():
            raise Exception("Permanent error")
        
        with pytest.raises(Exception) as exc_info:
            always_fail()
        
        assert "Permanent error" in str(exc_info.value)
    
    def test_retry_non_retryable(self):
        """测试不可重试错误"""
        from src.common.retry import retry, NonRetryableError
        
        @retry(max_attempts=3, initial_delay=0.1)
        def non_retryable():
            raise NonRetryableError("Non-retryable error")
        
        with pytest.raises(NonRetryableError):
            non_retryable()


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
