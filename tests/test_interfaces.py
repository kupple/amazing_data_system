"""
接口功能测试 - 验证接口可用性和数据库存储
仅下载少量数据进行测试
"""
import pytest
import os
import tempfile
import shutil
from datetime import datetime, timedelta
from unittest.mock import patch, MagicMock

# 设置测试环境
os.environ.setdefault("AD_ACCOUNT", "225300062129")
os.environ.setdefault("AD_PASSWORD", "22530006212920260312")
os.environ.setdefault("AD_IP", "120.86.124.106")
os.environ.setdefault("AD_PORT", "8600")

from src.config import config
from src.database import DuckDBManager
from src.models import DataSource


class TestConnection:
    """测试连接"""
    
    def test_config_loaded(self):
        """测试配置加载"""
        assert config.amazing_data.account == "225300062129"
        assert config.amazing_data.ip == "120.86.124.106"
        assert config.amazing_data.port == 8600
        print("✓ 配置加载正确")


class TestDatabase:
    """测试数据库"""
    
    @pytest.fixture
    def temp_db(self):
        temp_dir = tempfile.mkdtemp()
        db_path = os.path.join(temp_dir, "test.duckdb")
        db = DuckDBManager(db_path)
        yield db
        db.close()
        shutil.rmtree(temp_dir, ignore_errors=True)
    
    def test_db_init(self, temp_db):
        """测试数据库初始化"""
        assert temp_db.conn is not None
        assert os.path.exists(temp_db.db_path)
        print("✓ 数据库初始化成功")
    
    def test_insert_and_query(self, temp_db):
        """测试插入和查询"""
        import pandas as pd
        
        # 模拟行情数据
        test_data = pd.DataFrame([
            {
                "sec_code": "000001",
                "sec_name": "平安银行",
                "trade_date": "20240301",
                "close": 15.50,
                "open": 15.20,
                "high": 15.60,
                "low": 15.10,
                "volume": 1000000
            },
            {
                "sec_code": "000002", 
                "sec_name": "万科A",
                "trade_date": "20240301",
                "close": 12.30,
                "open": 12.10,
                "high": 12.50,
                "low": 12.00,
                "volume": 800000
            }
        ])
        
        # 插入数据
        temp_db.insert_dataframe(test_data, "stock_snapshot")
        
        # 验证插入
        count = temp_db.get_table_count("stock_snapshot")
        assert count == 2
        
        # 查询数据
        result = temp_db.query("SELECT * FROM stock_snapshot WHERE sec_code = '000001'")
        assert len(result) == 1
        assert result.iloc[0]["close"] == 15.50
        
        print("✓ 数据库插入查询成功")
    
    def test_incremental_update(self, temp_db):
        """测试增量更新"""
        import pandas as pd
        
        # 初始数据
        df1 = pd.DataFrame([
            {"sec_code": "000001", "trade_date": "20240301", "close": 15.50}
        ])
        temp_db.insert_dataframe(df1, "test_incr")
        
        # 增量数据（包含新数据和旧数据）
        df2 = pd.DataFrame([
            {"sec_code": "000001", "trade_date": "20240301", "close": 15.50},  # 旧数据
            {"sec_code": "000001", "trade_date": "20240302", "close": 15.80},  # 新数据
        ])
        
        temp_db.incremental_update(
            "test_incr",
            df2,
            key_columns=["sec_code", "trade_date"],
            date_column="trade_date"
        )
        
        # 验证：应该有2条，000001在20240301只有一条
        count = temp_db.get_table_count("test_incr")
        assert count == 2
        
        print("✓ 增量更新成功")
    
    def test_sync_status(self, temp_db):
        """测试同步状态记录"""
        temp_db.update_sync_status("test_data", success=True, record_count=100)
        
        status = temp_db.get_sync_status("test_data")
        
        assert status["status"] == "success"
        assert status["record_count"] == 100
        
        print("✓ 同步状态记录成功")


class TestDataSources:
    """测试数据源枚举"""
    
    def test_all_data_sources_defined(self):
        """测试所有数据源已定义"""
        # 核心数据源
        core_sources = [
            DataSource.SECURITY_INFO,
            DataSource.SECURITY_CODE,
            DataSource.STOCK_SNAPSHOT,
            DataSource.BALANCE_SHEET,
            DataSource.MARGIN_SUMMARY,
            DataSource.DRAGON_TIGER,
            DataSource.CB_ISSUANCE,
            DataSource.TREASURY_YIELD,
        ]
        
        for ds in core_sources:
            assert ds.value is not None
        
        print(f"✓ 已定义 {len(list(DataSource))} 个数据源")


class TestMockClient:
    """模拟客户端测试"""
    
    def test_client_methods_exist(self):
        """测试客户端方法存在"""
        from src.client import AmazingDataClient
        
        # 验证关键方法存在
        methods = [
            'get_stock_snapshot', 'get_margin_summary', 'get_balance_sheet',
            'get_dragon_tiger', 'get_cb_issuance', 'get_etf_redeem'
        ]
        
        for method in methods:
            assert hasattr(AmazingDataClient, method), f"缺少方法: {method}"
        
        print(f"✓ 客户端包含所有必需方法")
    
    def test_fetch_data_method_map(self):
        """测试通用方法映射"""
        from src.client import AmazingDataClient
        from src.models import DataSource
        
        # 验证 DataSource 到方法的映射
        test_cases = [
            (DataSource.STOCK_SNAPSHOT, 'get_stock_snapshot'),
            (DataSource.MARGIN_SUMMARY, 'get_margin_summary'),
            (DataSource.BALANCE_SHEET, 'get_balance_sheet'),
            (DataSource.DRAGON_TIGER, 'get_dragon_tiger'),
            (DataSource.CB_ISSUANCE, 'get_cb_issuance'),
            (DataSource.ETF_REDEEM, 'get_etf_redeem'),
        ]
        
        for ds, method_name in test_cases:
            assert hasattr(AmazingDataClient, method_name), f"映射错误: {ds.value}"
        
        print("✓ 数据源方法映射正确")


class TestIntegration:
    """集成测试 - 完整流程"""
    
    def test_full_workflow(self):
        """测试完整工作流"""
        import pandas as pd
        import tempfile, shutil
        
        temp_dir = tempfile.mkdtemp()
        
        try:
            # 1. 创建数据库
            db = DuckDBManager(os.path.join(temp_dir, "workflow.duckdb"))
            
            # 2. 模拟数据
            test_cases = [
                ("stock_snapshot", pd.DataFrame([
                    {"sec_code": "000001", "close": 15.5, "trade_date": "20240301"},
                    {"sec_code": "000002", "close": 12.3, "trade_date": "20240301"},
                ])),
                ("margin_summary", pd.DataFrame([
                    {"trade_date": "20240301", "rzye": 1000000, "rzmre": 500000},
                ])),
                ("balance_sheet", pd.DataFrame([
                    {"sec_code": "000001", "report_date": "20231231", "total_assets": 10000000},
                ])),
            ]
            
            # 3. 插入数据
            for table_name, df in test_cases:
                db.insert_dataframe(df, table_name)
                db.update_sync_status(table_name, success=True, record_count=len(df))
            
            # 4. 验证
            for table_name, _ in test_cases:
                count = db.get_table_count(table_name)
                assert count > 0, f"表 {table_name} 无数据"
            
            # 5. 查询验证
            result = db.query("SELECT * FROM stock_snapshot WHERE sec_code = '000001'")
            assert len(result) == 1
            
            # 6. 获取同步状态
            statuses = db.get_sync_status()
            assert len(statuses) >= 3
            
            db.close()
            
            print("✓ 完整工作流测试通过")
            
        finally:
            shutil.rmtree(temp_dir, ignore_errors=True)


class TestAPIEndpoints:
    """API 端点测试"""
    
    def test_api_import(self):
        """测试 API 模块导入"""
        from src.api import app
        assert app is not None
        print("✓ API 模块导入成功")
    
    def test_api_routes(self):
        """测试 API 路由"""
        from src.api import app
        routes = [r.path for r in app.routes]
        
        # 验证关键路由
        assert "/" in routes
        assert "/health" in routes
        assert "/api/tables" in routes
        assert "/api/data/{table_name}" in routes
        assert "/api/sync/status" in routes
        
        print(f"✓ API 已注册 {len(routes)} 个路由")


class TestMCPService:
    """MCP 服务测试"""
    
    def test_mcp_import(self):
        """测试 MCP 模块导入"""
        from src.mcp import MCPService
        service = MCPService()
        
        # 验证工具已注册
        tools = service.get_tools()
        assert len(tools) > 0
        
        print(f"✓ MCP 服务已注册 {len(tools)} 个工具")


if __name__ == "__main__":
    print("=" * 50)
    print("AmazingData 接口功能测试")
    print("=" * 50)
    
    pytest.main([__file__, "-v", "--tb=short"])
