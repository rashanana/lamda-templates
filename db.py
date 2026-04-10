from typing import Any, Optional
from aws_lambda_powertools import Logger
from botocore.exceptions import ClientError

logger = Logger(child=True)


def get_item(table, pk: str, sk: str) -> Optional[dict]:
    try:
        resp = table.get_item(Key={"pk": pk, "sk": sk})
        return resp.get("Item")
    except ClientError as e:
        logger.exception("DynamoDB get_item failed", extra={"pk": pk, "sk": sk})
        raise


def put_item(table, item: dict) -> None:
    try:
        table.put_item(Item=item)
    except ClientError:
        logger.exception("DynamoDB put_item failed", extra={"pk": item.get("pk")})
        raise
