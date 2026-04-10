import json
import os
from http import HTTPStatus
from typing import Any

import boto3
from aws_lambda_powertools import Logger, Tracer, Metrics
from aws_lambda_powertools.metrics import MetricUnit
from aws_lambda_powertools.utilities.typing import LambdaContext
from aws_lambda_powertools.event_handler import APIGatewayRestResolver
from aws_lambda_powertools.event_handler.exceptions import (
    BadRequestError,
    NotFoundError,
    ServiceError,
)

from utils.response import ok, created, error
from utils.db import get_item, put_item

logger = Logger()
tracer = Tracer()
metrics = Metrics()
app = APIGatewayRestResolver()

TABLE_NAME = os.environ["TABLE_NAME"]
dynamodb = boto3.resource("dynamodb")
table = dynamodb.Table(TABLE_NAME)


# ── routes ────────────────────────────────────────────────────────────────────

@app.get("/items/<item_id>")
@tracer.capture_method
def get_item_route(item_id: str) -> dict:
    logger.info("Fetching item", extra={"item_id": item_id})

    item = get_item(table, pk=f"ITEM#{item_id}", sk="META")
    if not item:
        raise NotFoundError(f"Item {item_id} not found")

    metrics.add_metric(name="ItemFetched", unit=MetricUnit.Count, value=1)
    return ok(item)


@app.post("/items")
@tracer.capture_method
def create_item_route() -> dict:
    body: dict = app.current_event.json_body
    if not body.get("name"):
        raise BadRequestError("'name' is required")

    import uuid
    item_id = str(uuid.uuid4())
    item = {"pk": f"ITEM#{item_id}", "sk": "META", "id": item_id, **body}

    put_item(table, item)
    logger.info("Item created", extra={"item_id": item_id})
    metrics.add_metric(name="ItemCreated", unit=MetricUnit.Count, value=1)

    return created(item)


# ── entry point ───────────────────────────────────────────────────────────────

@logger.inject_lambda_context(log_event=True)
@tracer.capture_lambda_handler
@metrics.log_metrics(capture_cold_start_metric=True)
def lambda_handler(event: dict, context: LambdaContext) -> dict:
    return app.resolve(event, context)
