"""
测试用例 - 数据库模块
"""
import pytest
import os
import tempfile
import shutil
import pandas as pd
from datetime import datetime, date

from src.common.database import DuckDBManager


class TestDuckDBManager:
    """DuckDB 管理器测试"""
    
    @pytest.fixture
    def temp_db(self):
        """创建临时数据库"""
        temp_dir = tempfile.mkdtemp()
        db_path = os.path.join(temp_dir, "test.duckdb")
        
        db = DuckDBManager(db_path)
        
        yield db
        
        db.close()
        shutil.rmtree(temp_dir, ignore_errors=True)
    
    def test_init(self, temp_db):
        """测试初始化"""
        assert temp_db.conn is not None
        assert os.path.exists(temp_db.db_path)
    
    def test_table_creation(self, temp_db):
        """测试表创建"""
        assert temp_db.table_exists("fetch_records")
        assert temp_db.table_exists("sync_status")
        assert temp_db.table_exists("daily_summary")
    
    def test_insert_dataframe(self, temp_db):
        """测试插入 DataFrame"""
        df = pd.DataFrame({
            "sec_code": ["000001", "000002"],
            "sec_name": ["平安银行", "万科A"],
            "trade_date": ["20240301", "20240301"]
        })
        
        temp_db.insert_dataframe(df, "test_table")
        
        assert temp_db.table_exists("test_table")
        assert temp_db.get_table_count("test_table") == 2
    
    def test_insert_empty_dataframe(self, temp_db):
        """测试插入空 DataFrame"""
        df = pd.DataFrame()
        
        temp_db.insert_dataframe(df, "empty_table")
        
        assert not temp_db.table_exists("empty_table")
    
    def test_query(self, temp_db):
        """测试查询"""
        df = pd.DataFrame({
            "sec_code": ["000001", "000002"],
            "value": [100, 200]
        })
        
        temp_db.insert_dataframe(df, "query_test")
        
        result = temp_db.query("SELECT * FROM query_test")
        
        assert len(result) == 2
        assert "sec_code" in result.columns
    
    def test_get_latest_date(self, temp_db):
        """测试获取最新日期"""
        df = pd.DataFrame({
            "sec_code": ["000001"],
            "trade_date": ["20240301"]
        })
        
        temp_db.insert_dataframe(df, "date_test")
        
        latest = temp_db.get_latest_date("date_test", "trade_date")
        
        assert latest == "20240301"
    
    def test_get_latest_date_empty(self, temp_db):
        """测试空表获取最新日期"""
        df = pd.DataFrame({
            "sec_code": [],
            "trade_date": []
        })
        
        temp_db.insert_dataframe(df, "empty_date_test")
        
        latest = temp_db.get_latest_date("empty_date_test", "trade_date")
        
        assert latest is None
    
    def test_table_count(self, temp_db):
        """测试获取表记录数"""
        df = pd.DataFrame({
            "id": range(100)
        })
        
        temp_db.insert_dataframe(df, "count_test")
        
        count = temp_db.get_table_count("count_test")
        
        assert count == 100
    
    def test_get_tables(self, temp_db):
        """测试获取所有表"""
        tables = temp_db.get_tables()
        
        assert "fetch_records" in tables
        assert "sync_status" in tables
    
    def test_save_fetch_record(self, temp_db):
        """测试保存获取记录"""
        temp_db.save_fetch_record(
            "test_data",
            success=True,
            record_count=100,
            start_date="20240301",
            end_date="20240301"
        )
        
        result = temp_db.query("SELECT * FROM fetch_records WHERE data_type = 'test_data'")
        
        assert len(result) == 1
        assert result.iloc[0]["success"] == True
        assert result.iloc[0]["record_count"] == 100
    
    def test_update_sync_status(self, temp_db):
        """测试更新同步状态"""
        temp_db.update_sync_status(
            "test_sync",
            success=True,
            record_count=50
        )
        
        status = temp_db.get_sync_status("test_sync")
        
        assert status["data_type"] == "test_sync"
        assert status["status"] == "success"
        assert status["record_count"] == 50
    
    def test_get_sync_status_all(self, temp_db):
        """测试获取所有同步状态"""
        temp_db.update_sync_status("sync1", success=True, record_count=10)
        temp_db.update_sync_status("sync2", success=True, record_count=20)
        
        statuses = temp_db.get_sync_status()
        
        assert len(statuses) >= 2
    
    def test_incremental_update(self, temp_db):
        """测试增量更新"""
        df1 = pd.DataFrame({
            "sec_code": ["000001"],
            "trade_date": ["20240301"],
            "value": [100]
        })
        
        temp_db.insert_dataframe(df1, "incremental_test")
        
        df2 = pd.DataFrame({
            "sec_code": ["000001", "000002"],
            "trade_date": ["20240301", "20240302"],
            "value": [100, 200]
        })
        
        temp_db.incremental_update(
            "incremental_test",
            df2,
            key_columns=["sec_code", "trade_date"],
            date_column="trade_date"
        )
        
        count = temp_db.get_table_count("incremental_test")
        
        assert count == 2
    
    def test_incremental_update_novel_data(self, temp_db):
        """测试增量更新无新数据"""
        df1 = pd.DataFrame({
            "sec_code": ["000001"],
            "trade_date": ["20240301"],
            "value": [100]
        })
        
        temp_db.insert_dataframe(df1, "novel_test")
        
        df2 = pd.DataFrame({
            "sec_code": ["000001"],
            "trade_date": ["20240301"],
            "value": [100]
        })
        
        temp_db.incremental_update(
            "novel_test",
            df2,
            key_columns=["sec_code", "trade_date"],
            date_column="trade_date"
        )
        
        count = temp_db.get_table_count("novel_test")
        
        assert count == 1
    
    def test_context_manager(self):
        """测试上下文管理器"""
        temp_dir = tempfile.mkdtemp()
        db_path = os.path.join(temp_dir, "test.duckdb")
        
        try:
            with DuckDBManager(db_path) as db:
                assert db.conn is not None
            
            assert os.path.exists(db_path)
        finally:
            shutil.rmtree(temp_dir, ignore_errors=True)


class TestDuckDBIntegration:
    """DuckDB 集成测试"""
    
    def test_full_workflow(self):
        """测试完整工作流"""
        temp_dir = tempfile.mkdtemp()
        db_path = os.path.join(temp_dir, "test.duckdb")
        
        try:
            db = DuckDBManager(db_path)
            
            securities = pd.DataFrame({
                "sec_code": ["000001", "000002", "000003"],
                "sec_name": ["平安银行", "万科A", "招商银行"],
                "trade_date": ["20240301", "20240301", "20240301"]
            })
            db.insert_dataframe(securities, "securities")
            
            quotes = pd.DataFrame({
                "sec_code": ["000001", "000002"],
                "trade_date": ["20240301", "20240301"],
                "close": [15.5, 12.3],
                "volume": [1000000, 800000]
            })
            db.insert_dataframe(quotes, "quotes")
            
            result = db.query("""
                SELECT s.sec_name, q.close, q.volume 
                FROM securities s 
                JOIN quotes q ON s.sec_code = q.sec_code
            """)
            
            assert len(result) == 2
            
            db.update_sync_status("securities", success=True, record_count=3)
            db.update_sync_status("quotes", success=True, record_count=2)
            
            status = db.get_sync_status()
            assert len(status) >= 2
            
            db.close()
            
        finally:
            shutil.rmtree(temp_dir, ignore_errors=True)


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
