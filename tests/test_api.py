"""
测试用例 - API 模块
"""
import pytest
import os
import tempfile
from fastapi.testclient import TestClient
from unittest.mock import patch, MagicMock
import pandas as pd

from src.api import app
from src.database import DuckDBManager


@pytest.fixture
def client():
    """创建测试客户端"""
    return TestClient(app)


@pytest.fixture
def mock_db():
    """创建模拟数据库"""
    with tempfile.NamedTemporaryFile(suffix=".duckdb", delete=False) as f:
        db_path = f.name
    
    db = DuckDBManager(db_path)
    
    # 插入测试数据
    test_data = pd.DataFrame({
        "sec_code": ["000001", "000002"],
        "sec_name": ["平安银行", "万科A"],
        "close": [15.5, 12.3]
    })
    db.insert_dataframe(test_data, "stock_snapshot")
    
    yield db
    
    db.close()
    os.unlink(db_path)


class TestRoot:
    """根路径测试"""
    
    def test_root(self, client):
        """测试根路径"""
        response = client.get("/")
        
        assert response.status_code == 200
        data = response.json()
        assert data["name"] == "AmazingData API"
        assert data["version"] == "1.0.0"
    
    def test_health(self, client):
        """测试健康检查"""
        response = client.get("/health")
        
        assert response.status_code == 200
        assert response.json()["status"] == "healthy"


class TestTables:
    """表接口测试"""
    
    @patch('src.api.get_db')
    def test_list_tables(self, mock_get_db, client):
        """测试获取表列表"""
        mock_db = MagicMock()
        mock_db.get_tables.return_value = ["table1", "table2"]
        mock_db.get_table_count.side_effect = [100, 200]
        mock_get_db.return_value = mock_db
        
        response = client.get("/api/tables")
        
        assert response.status_code == 200
        data = response.json()
        assert data["code"] == 200
        assert len(data["data"]) == 2
    
    @patch('src.api.get_db')
    def test_get_table_info(self, mock_get_db, client):
        """测试获取表信息"""
        mock_db = MagicMock()
        mock_db.get_table_count.return_value = 100
        mock_db.query.return_value = pd.DataFrame([
            {"name": "sec_code", "type": "VARCHAR"}
        ])
        mock_get_db.return_value = mock_db
        
        response = client.get("/api/tables/test_table/info")
        
        assert response.status_code == 200
        data = response.json()
        assert data["data"]["record_count"] == 100


class TestDataQuery:
    """数据查询测试"""
    
    @patch('src.api.get_db')
    def test_query_data_success(self, mock_get_db, client):
        """测试成功查询"""
        mock_db = MagicMock()
        mock_db.table_exists.return_value = True
        mock_db.query.side_effect = [
            pd.DataFrame({"total": [10]}),  # count 查询
            pd.DataFrame({"sec_code": ["000001"], "close": [15.5]})  # data 查询
        ]
        mock_get_db.return_value = mock_db
        
        response = client.get("/api/data/stock_snapshot?limit=10")
        
        assert response.status_code == 200
        data = response.json()
        assert data["code"] == 200
        assert len(data["data"]) == 1
    
    @patch('src.api.get_db')
    def test_query_data_table_not_found(self, mock_get_db, client):
        """测试表不存在"""
        mock_db = MagicMock()
        mock_db.table_exists.return_value = False
        mock_get_db.return_value = mock_db
        
        response = client.get("/api/data/nonexistent_table")
        
        assert response.status_code == 404
    
    @patch('src.api.get_db')
    def test_query_data_with_where(self, mock_get_db, client):
        """测试带条件的查询"""
        mock_db = MagicMock()
        mock_db.table_exists.return_value = True
        mock_db.query.side_effect = [
            pd.DataFrame({"total": [1]}),
            pd.DataFrame({"sec_code": ["000001"]})
        ]
        mock_get_db.return_value = mock_db
        
        response = client.get("/api/data/stock_snapshot?where=sec_code='000001'")
        
        assert response.status_code == 200


class TestSQL:
    """SQL 接口测试"""
    
    @patch('src.api.get_db')
    def test_execute_select(self, mock_get_db, client):
        """测试执行 SELECT"""
        mock_db = MagicMock()
        mock_db.query.return_value = pd.DataFrame({"a": [1, 2]})
        mock_get_db.return_value = mock_db
        
        response = client.post("/api/sql?sql=SELECT * FROM test")
        
        assert response.status_code == 200
        data = response.json()
        assert data["code"] == 200
    
    @patch('src.api.get_db')
    def test_execute_non_select(self, mock_get_db, client):
        """测试非 SELECT 被拒绝"""
        response = client.post("/api/sql?sql=DELETE FROM test")
        
        assert response.status_code == 400
    
    @patch('src.api.get_db')
    def test_sql_injection_attempt(self, mock_get_db, client):
        """测试 SQL 注入尝试"""
        response = client.post("/api/sql?sql=SELECT * FROM test; DROP TABLE test;--")
        
        assert response.status_code == 400


class TestSyncStatus:
    """同步状态测试"""
    
    @patch('src.api.get_db')
    @patch('src.logger.logger')
    def test_get_sync_status(self, mock_logger, mock_get_db, client):
        """测试获取同步状态"""
        mock_db = MagicMock()
        mock_db.get_sync_status.return_value = {
            "data_type": "test",
            "status": "success"
        }
        mock_get_db.return_value = mock_db
        
        response = client.get("/api/sync/status?data_type=test")
        
        assert response.status_code == 200


class TestStats:
    """统计接口测试"""
    
    @patch('src.api.get_db')
    def test_get_stats_overview(self, mock_get_db, client):
        """测试获取统计概览"""
        mock_db = MagicMock()
        mock_db.get_tables.return_value = ["table1", "table2"]
        mock_db.get_table_count.side_effect = [100, 200]
        mock_db.get_sync_status.return_value = []
        mock_get_db.return_value = mock_db
        
        response = client.get("/api/stats/overview")
        
        assert response.status_code == 200
        data = response.json()
        assert data["data"]["total_tables"] == 2
        assert data["data"]["total_records"] == 300


class TestQuote:
    """便捷查询测试"""
    
    @patch('src.api.get_db')
    def test_get_quote_success(self, mock_get_db, client):
        """测试获取行情成功"""
        mock_db = MagicMock()
        mock_db.table_exists.return_value = True
        mock_db.query.return_value = pd.DataFrame({
            "sec_code": ["000001"],
            "close": [15.5]
        })
        mock_get_db.return_value = mock_db
        
        response = client.get("/api/quote/000001")
        
        assert response.status_code == 200
    
    @patch('src.api.get_db')
    def test_get_quote_not_found(self, mock_get_db, client):
        """测试行情不存在"""
        mock_db = MagicMock()
        mock_db.table_exists.return_value = False
        mock_get_db.return_value = mock_db
        
        response = client.get("/api/quote/999999")
        
        assert response.status_code == 404


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
