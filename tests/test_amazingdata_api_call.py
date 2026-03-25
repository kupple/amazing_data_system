from unittest.mock import MagicMock, patch

import pandas as pd
from fastapi.testclient import TestClient

from src.services.api import app


client = TestClient(app)


class TestAmazingDataCall:
    """AmazingData 通用调用接口测试"""

    @patch('src.collectors.starlight.client.get_client')
    def test_call_amazingdata_success_with_dataframe(self, mock_get_client):
        """测试 DataFrame 返回值会被正确序列化"""
        mock_client = MagicMock()
        mock_client.is_connected = True
        mock_client.get_code_info.return_value = pd.DataFrame([
            {"sec_code": "000001.SZ", "sec_name": "平安银行"}
        ])
        mock_get_client.return_value = mock_client

        response = client.post("/api/amazingdata/call", json={
            "method": "get_code_info",
            "parameters": {"security_type": "EXTRA_STOCK_A"}
        })

        assert response.status_code == 200
        data = response.json()
        assert data["code"] == 200
        assert data["total"] == 1
        assert data["data"][0]["sec_code"] == "000001.SZ"

    @patch('src.collectors.starlight.client.get_client')
    def test_call_amazingdata_rejects_undocumented_method(self, mock_get_client):
        """测试未开放的方法不能通过通用调用接口访问"""
        mock_client = MagicMock()
        mock_client.is_connected = True
        mock_client.connect.return_value = True
        mock_get_client.return_value = mock_client

        response = client.post("/api/amazingdata/call", json={
            "method": "connect",
            "parameters": {}
        })

        assert response.status_code == 400
        assert "允许调用" in response.json()["data"]["error"]

    @patch('src.collectors.starlight.client.get_client')
    def test_call_amazingdata_supports_basedata_prefixed_method(self, mock_get_client):
        """测试兼容文档中的 BaseData.method 写法"""
        mock_client = MagicMock()
        mock_client.is_connected = True
        mock_client.get_code_list.return_value = ["000001.SZ"]
        mock_get_client.return_value = mock_client

        response = client.post("/api/amazingdata/call", json={
            "method": "BaseData.get_code_list",
            "parameters": {"security_type": "EXTRA_STOCK_A"}
        })

        assert response.status_code == 200
        data = response.json()
        assert data["data"] == ["000001.SZ"]
        assert data["total"] == 1

    @patch('src.collectors.starlight.client.get_client')
    def test_call_amazingdata_supports_ad_infodata_prefixed_method(self, mock_get_client):
        """测试兼容文档中的 ad.InfoData.method 写法"""
        mock_client = MagicMock()
        mock_client.is_connected = True
        mock_client.get_stock_basic.return_value = pd.DataFrame([
            {"sec_code": "000001.SZ", "sec_name": "平安银行"}
        ])
        mock_get_client.return_value = mock_client

        response = client.post("/api/amazingdata/call", json={
            "method": "ad.InfoData.get_stock_basic",
            "parameters": {"code_list": ["000001.SZ"]}
        })

        assert response.status_code == 200
        data = response.json()
        assert data["data"][0]["sec_name"] == "平安银行"

    @patch('src.collectors.starlight.client.get_client')
    def test_call_amazingdata_invalid_parameters_returns_400(self, mock_get_client):
        """测试参数错误返回 400 而不是 500"""
        class FakeClient:
            is_connected = True

            def get_stock_basic(self, code_list):
                raise AssertionError("should not be called")

        mock_client = FakeClient()
        mock_get_client.return_value = mock_client

        response = client.post("/api/amazingdata/call", json={
            "method": "get_stock_basic",
            "parameters": {"security_type": "EXTRA_STOCK_A"}
        })

        assert response.status_code == 400
        assert "参数错误" in response.json()["data"]["error"]

    @patch('src.collectors.starlight.client.get_client')
    def test_call_amazingdata_serializes_nested_dataframe(self, mock_get_client):
        """测试字典中的 DataFrame 也会被正确序列化"""
        mock_client = MagicMock()
        mock_client.is_connected = True
        mock_client.query_kline.return_value = {
            "000001.SZ": pd.DataFrame([{"close": 12.34}]),
            "meta": {"period": 1440}
        }
        mock_get_client.return_value = mock_client

        response = client.post("/api/amazingdata/call", json={
            "method": "query_kline",
            "parameters": {
                "code_list": ["000001.SZ"],
                "begin_date": 20240101,
                "end_date": 20240131,
                "period": 1440
            }
        })

        assert response.status_code == 200
        data = response.json()
        assert data["data"]["000001.SZ"][0]["close"] == 12.34
        assert data["total"] == 2
