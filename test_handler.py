import json
import os
import pytest
from unittest.mock import MagicMock, patch

os.environ.setdefault("TABLE_NAME", "test-table")
os.environ.setdefault("POWERTOOLS_SERVICE_NAME", "test-service")
os.environ.setdefault("POWERTOOLS_LOG_LEVEL", "DEBUG")
os.environ.setdefault("AWS_DEFAULT_REGION", "us-east-1")


def _apigw_event(method: str, path: str, body: dict = None, path_params: dict = None) -> dict:
    return {
        "httpMethod": method,
        "path": path,
        "pathParameters": path_params or {},
        "queryStringParameters": None,
        "headers": {"Content-Type": "application/json"},
        "body": json.dumps(body) if body else None,
        "requestContext": {"requestId": "test-req-123"},
    }


def _ctx():
    ctx = MagicMock()
    ctx.function_name = "test-fn"
    ctx.memory_limit_in_mb = 512
    ctx.invoked_function_arn = "arn:aws:lambda:us-east-1:123456789012:function:test-fn"
    ctx.aws_request_id = "test-req-123"
    return ctx


class TestGetItem:
    @patch("handler.get_item")
    def test_returns_item(self, mock_get):
        mock_get.return_value = {"pk": "ITEM#abc", "sk": "META", "id": "abc", "name": "Widget"}

        from handler import lambda_handler
        event = _apigw_event("GET", "/items/abc", path_params={"item_id": "abc"})
        resp = lambda_handler(event, _ctx())

        assert resp["statusCode"] == 200
        body = json.loads(resp["body"])
        assert body["data"]["name"] == "Widget"

    @patch("handler.get_item")
    def test_returns_404_when_missing(self, mock_get):
        mock_get.return_value = None

        from handler import lambda_handler
        event = _apigw_event("GET", "/items/missing", path_params={"item_id": "missing"})
        resp = lambda_handler(event, _ctx())

        assert resp["statusCode"] == 404


class TestCreateItem:
    @patch("handler.put_item")
    def test_creates_item(self, mock_put):
        mock_put.return_value = None

        from handler import lambda_handler
        event = _apigw_event("POST", "/items", body={"name": "Gadget", "price": 9.99})
        resp = lambda_handler(event, _ctx())

        assert resp["statusCode"] == 201
        body = json.loads(resp["body"])
        assert body["data"]["name"] == "Gadget"
        assert "id" in body["data"]

    def test_rejects_missing_name(self):
        from handler import lambda_handler
        event = _apigw_event("POST", "/items", body={"price": 9.99})
        resp = lambda_handler(event, _ctx())

        assert resp["statusCode"] == 400
