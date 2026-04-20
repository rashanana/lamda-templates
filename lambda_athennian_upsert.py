"""
Lambda: SQS Consumer + Athennian GraphQL Upsert (Steps 5 & 6)
--------------------------------------------------------------
- Triggered by SQS event source mapping (not Step Functions)
- Consumes delta records published by Lambda 2
- Builds GraphQL mutation and upserts HR Data Hub data into Athennian
- Per-record processing with explicit success/failure reporting
- Failed records return to SQS for retry up to DLQ threshold
"""

import json
import logging
import os
from typing import Optional

import boto3
import urllib3

logger = logging.getLogger()
logger.setLevel(logging.INFO)

# ── Config ────────────────────────────────────────────────────────────────────
ATHENNIAN_API_URL  = os.environ["ATHENNIAN_API_URL"]
SECRET_NAME        = os.environ["ATHENNIAN_SECRET_NAME"]
DLQ_ALARM_METRIC   = os.environ.get("DLQ_ALARM_METRIC_NAMESPACE", "JnJ/Integration")
MAX_BATCH_FAILURES = int(os.environ.get("MAX_BATCH_FAILURES", "3"))  # fail batch if >= N records error

# ── AWS clients ───────────────────────────────────────────────────────────────
secrets_client  = boto3.client("secretsmanager")
cloudwatch      = boto3.client("cloudwatch")
http            = urllib3.PoolManager()

# Module-level token cache — reused across warm invocations within same container
_cached_token: Optional[str] = None


# ── Helpers: Auth ─────────────────────────────────────────────────────────────

def get_api_token() -> str:
    """
    Retrieve Athennian API token from Secrets Manager.
    Cached at module level for warm Lambda reuse.
    Token is refreshed if Secrets Manager returns a different value
    (handles rotation without requiring a cold start).
    """
    global _cached_token
    response = secrets_client.get_secret_value(SecretId=SECRET_NAME)
    fresh    = json.loads(response["SecretString"])["api_token"]
    if fresh != _cached_token:
        logger.info("API token refreshed (rotation detected or first load).")
        _cached_token = fresh
    return _cached_token


# ── Helpers: GraphQL Mutation ─────────────────────────────────────────────────

def build_upsert_mutation(hr_record: dict) -> dict:
    """
    Build Athennian GraphQL upsert mutation from an HR Data Hub record.

    Maps HR Data Hub fields → Athennian custom fields.
    Adjust field mappings to match your confirmed schema.
    """
    return {
        "query": """
            mutation UpsertUserFromHR($input: UpsertUserInput!) {
                upsertUser(input: $input) {
                    id
                    email
                    customFields {
                        wwid
                        department
                        jobTitle
                        managerWwid
                        employeeStatus
                        costCenter
                        location
                    }
                    updatedAt
                }
            }
        """,
        "variables": {
            "input": {
                # Identity
                "wwid":           hr_record.get("wwid"),
                "email":          hr_record.get("workEmail"),

                # HR fields mapped to Athennian custom fields
                "customFields": {
                    "wwid":           hr_record.get("wwid"),
                    "department":     hr_record.get("department"),
                    "jobTitle":       hr_record.get("jobTitle"),
                    "managerWwid":    hr_record.get("managerWwid"),
                    "employeeStatus": hr_record.get("employmentStatus"),
                    "costCenter":     hr_record.get("costCenter"),
                    "location":       hr_record.get("workLocation"),
                },
            }
        },
    }


def execute_upsert(token: str, hr_record: dict) -> dict:
    """
    Execute one GraphQL upsert mutation against Athennian.

    Returns the upserted user record on success.
    Raises RuntimeError on HTTP error, GraphQL error, or missing data.
    Does NOT catch — caller handles retry/DLQ routing.
    """
    mutation = build_upsert_mutation(hr_record)
    payload  = json.dumps(mutation).encode("utf-8")

    response = http.request(
        "POST",
        ATHENNIAN_API_URL,
        body=payload,
        headers={
            "Content-Type": "application/json",
            "Authorization": f"Bearer {token}",
        },
        timeout=urllib3.Timeout(connect=5.0, read=30.0),
        retries=urllib3.Retry(
            total=3,
            backoff_factor=2.0,
            status_forcelist=[429, 500, 502, 503, 504],
        ),
    )

    if response.status != 200:
        raise RuntimeError(
            f"Athennian HTTP {response.status}: {response.data.decode()[:500]}"
        )

    body = json.loads(response.data.decode("utf-8"))

    if "errors" in body:
        # GraphQL errors are returned with HTTP 200 — handle explicitly
        errors = body["errors"]
        raise RuntimeError(f"GraphQL upsert errors for wwid={hr_record.get('wwid')}: {errors}")

    upserted = body.get("data", {}).get("upsertUser")
    if not upserted:
        raise RuntimeError(
            f"Upsert returned no data for wwid={hr_record.get('wwid')}. "
            f"Full response: {json.dumps(body)[:500]}"
        )

    return upserted


# ── Helpers: Observability ────────────────────────────────────────────────────

def emit_metric(metric_name: str, value: float, unit: str = "Count", dimensions: list = None) -> None:
    """
    Emit a custom CloudWatch metric for pipeline observability.
    Non-blocking — logs warning on failure rather than raising.
    """
    try:
        cloudwatch.put_metric_data(
            Namespace=DLQ_ALARM_METRIC,
            MetricData=[{
                "MetricName": metric_name,
                "Value":      value,
                "Unit":       unit,
                "Dimensions": dimensions or [{"Name": "Integration", "Value": "AthennianHRSync"}],
            }],
        )
    except Exception as e:
        logger.warning("Failed to emit CloudWatch metric %s: %s", metric_name, str(e))


# ── Handler ───────────────────────────────────────────────────────────────────

def lambda_handler(event: dict, context) -> dict:
    """
    Entry point triggered by SQS event source mapping.

    SQS message body format (published by Lambda 2):
        {
            "correlationId": "...",
            "sourceS3Key":   "athennian/raw/Athennian_..._batch0001.json",
            "hrRecord":      { ...HR Data Hub record... }
        }

    SQS partial batch failure response:
        { "batchItemFailures": [{"itemIdentifier": "<messageId>"}, ...] }

    Returning failed message IDs causes only those messages to return
    to the queue for retry. Successful messages are deleted automatically.
    """
    records              = event.get("Records", [])
    batch_item_failures  = []
    processed            = 0
    failed               = 0

    logger.info("SQS batch received: %d messages", len(records))

    token = get_api_token()

    for sqs_record in records:
        message_id = sqs_record["messageId"]
        body       = None

        try:
            body           = json.loads(sqs_record["body"])
            correlation_id = body.get("correlationId", "unknown")
            hr_record      = body.get("hrRecord", {})
            wwid           = hr_record.get("wwid", "unknown")

            if not hr_record:
                raise ValueError(f"Empty hrRecord in message {message_id}")

            if not wwid or wwid == "unknown":
                raise ValueError(f"Missing WWID in hrRecord for message {message_id}")

            logger.info(
                "Processing wwid=%s correlationId=%s messageId=%s",
                wwid, correlation_id, message_id,
            )

            upserted = execute_upsert(token, hr_record)

            logger.info(
                "Upsert SUCCESS wwid=%s athennianId=%s updatedAt=%s",
                wwid,
                upserted.get("id"),
                upserted.get("updatedAt"),
            )

            emit_metric("UpsertSuccess", 1)
            processed += 1

        except Exception as e:
            wwid_info = ""
            if body:
                wwid_info = body.get("hrRecord", {}).get("wwid", "unknown")

            logger.error(
                "Upsert FAILED wwid=%s messageId=%s error=%s",
                wwid_info, message_id, str(e),
                exc_info=True,
            )

            emit_metric("UpsertFailure", 1)
            failed += 1

            # Return failed message ID — SQS will retry up to maxReceiveCount
            # then route to DLQ without blocking successful messages
            batch_item_failures.append({"itemIdentifier": message_id})

            # Safety valve: if too many failures in one batch, raise immediately
            # This triggers full batch retry and alerts on-call via CloudWatch alarm
            if failed >= MAX_BATCH_FAILURES:
                logger.critical(
                    "MAX_BATCH_FAILURES=%d reached in this batch. "
                    "Raising to trigger full batch retry and alarm. "
                    "Processed=%d Failed=%d",
                    MAX_BATCH_FAILURES, processed, failed,
                )
                emit_metric("BatchAbort", 1)
                raise RuntimeError(
                    f"Batch aborted: {failed} failures exceeded MAX_BATCH_FAILURES={MAX_BATCH_FAILURES}"
                )

    # Emit batch summary metrics
    emit_metric("BatchProcessed", processed)
    if failed:
        emit_metric("BatchFailed", failed)

    logger.info(
        "Batch COMPLETE — processed=%d failed=%d batchItemFailures=%d",
        processed, failed, len(batch_item_failures),
    )

    # Partial batch failure response
    # Successful messages are auto-deleted; only failed ones return to queue
    return {"batchItemFailures": batch_item_failures}
