import json
from http import HTTPStatus


def _build(status: int, body: dict) -> dict:
    return {
        "statusCode": status,
        "headers": {
            "Content-Type": "application/json",
            "Access-Control-Allow-Origin": "*",
        },
        "body": json.dumps(body, default=str),
    }


def ok(data: dict) -> dict:
    return _build(HTTPStatus.OK, {"data": data})


def created(data: dict) -> dict:
    return _build(HTTPStatus.CREATED, {"data": data})


def error(status: int, message: str) -> dict:
    return _build(status, {"error": message})
