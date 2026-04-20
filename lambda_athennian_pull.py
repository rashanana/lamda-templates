"""
Lambda: Athennian User Pull (Step 2)
-------------------------------------
- Invoked by Step Functions
- Pulls all Athennian users via GraphQL with pagination
- Filters to records with non-empty WWID
- Batches records (default 800) and writes each batch to S3
- Implements circuit breaker to prevent runaway pagination
"""

import json
import logging
import os
import uuid
from datetime import datetime, timezone

import boto3
import urllib3

logger = logging.getLogger()
logger.setLevel(logging.INFO)

# ── Config (override via Lambda env vars) ─────────────────────────────────────
ATHENNIAN_API_URL  = os.environ["ATHENNIAN_API_URL"]
SECRET_NAME        = os.environ["ATHENNIAN_SECRET_NAME"]   # Secrets Manager key
S3_BUCKET          = os.environ["S3_BUCKET"]
S3_PREFIX          = os.environ.get("S3_PREFIX", "athennian/raw/")
BATCH_SIZE         = int(os.environ.get("BATCH_SIZE", "800"))
PAGE_SIZE          = int(os.environ.get("PAGE_SIZE", "100"))   # Athennian max
MAX_ITERATIONS     = int(os.environ.get("MAX_ITERATIONS", "500"))  # circuit breaker

# ── AWS clients ───────────────────────────────────────────────────────────────
secrets_client = boto3.client("secretsmanager")
s3_client      = boto3.client("s3")
http           = urllib3.PoolManager()


# ── Helpers ───────────────────────────────────────────────────────────────────

def get_api_token() -> str:
    """Retrieve Athennian API token from Secrets Manager."""
    response = secrets_client.get_secret_value(SecretId=SECRET_NAME)
    secret   = json.loads(response["SecretString"])
    return secret["api_token"]


def build_query(offset: int, limit: int) -> dict:
    """Build paginated GraphQL query for Athennian users."""
    return {
        "query": """
            query GetUsers($limit: Int!, $offset: Int!) {
                users(limit: $limit, offset: $offset) {
                    id
                    email
                    firstName
                    lastName
                    customFields {
                        wwid
                    }
                    status
                    createdAt
                    updatedAt
                }
            }
        """,
        "variables": {"limit": limit, "offset": offset},
    }


def fetch_page(token: str, offset: int) -> list:
    """
    Execute one paginated GraphQL call to Athennian.
    Returns list of user records for this page (may be empty).
    Raises on HTTP error or GraphQL errors in response.
    """
    payload = json.dumps(build_query(offset, PAGE_SIZE)).encode("utf-8")
    response = http.request(
        "POST",
        ATHENNIAN_API_URL,
        body=payload,
        headers={
            "Content-Type": "application/json",
            "Authorization": f"Bearer {token}",
        },
        timeout=urllib3.Timeout(connect=5.0, read=30.0),
        retries=urllib3.Retry(total=3, backoff_factor=1.0),
    )

    if response.status != 200:
        raise RuntimeError(
            f"Athennian API HTTP {response.status}: {response.data.decode()}"
        )

    body = json.loads(response.data.decode("utf-8"))

    if "errors" in body:
        raise RuntimeError(f"GraphQL errors: {body['errors']}")

    return body.get("data", {}).get("users", [])


def has_wwid(user: dict) -> bool:
    """Return True if user has a non-empty WWID custom field."""
    wwid = (user.get("customFields") or {}).get("wwid", "")
    return bool(wwid and str(wwid).strip())


def write_batch_to_s3(
    batch: list,
    correlation_id: str,
    batch_index: int,
    s3_bucket: str,
    s3_prefix: str,
) -> str:
    """
    Write a batch of filtered user records to S3.
    Key format: <prefix>Athennian_<CorrelationId>_<Date>_batch<N>.json
    Returns the S3 key written.
    """
    date_str = datetime.now(timezone.utc).strftime("%Y%m%d")
    key      = f"{s3_prefix}Athennian_{correlation_id}_{date_str}_batch{batch_index:04d}.json"

    payload = {
        "correlationId":  correlation_id,
        "batchIndex":     batch_index,
        "recordCount":    len(batch),
        "generatedAt":    datetime.now(timezone.utc).isoformat(),
        "records":        batch,
    }

    s3_client.put_object(
        Bucket=s3_bucket,
        Key=key,
        Body=json.dumps(payload, default=str),
        ContentType="application/json",
    )
    logger.info("Wrote batch %d (%d records) → s3://%s/%s", batch_index, len(batch), s3_bucket, key)
    return key


# ── Handler ───────────────────────────────────────────────────────────────────

def lambda_handler(event: dict, context) -> dict:
    """
    Entry point invoked by Step Functions.

    Input event (from Step Functions):
        { "correlationId": "<optional — generated if absent>" }

    Output:
        {
            "correlationId": "...",
            "s3Keys": ["...", ...],
            "totalFetched": N,
            "totalFiltered": N,
            "batchesWritten": N,
            "skippedNoWwid": N,
            "iterations": N
        }
    """
    correlation_id = event.get("correlationId") or str(uuid.uuid4())
    logger.info("START correlationId=%s", correlation_id)

    token = get_api_token()

    offset        = 0
    iteration     = 0
    total_fetched = 0
    skipped       = 0
    s3_keys       = []
    batch_index   = 1
    pending_batch = []   # accumulate filtered records before flushing to S3

    while True:
        # ── Circuit breaker ───────────────────────────────────────────────────
        if iteration >= MAX_ITERATIONS:
            logger.error(
                "Circuit breaker: reached MAX_ITERATIONS=%d at offset=%d. "
                "Possible infinite pagination. Halting.",
                MAX_ITERATIONS, offset,
            )
            raise RuntimeError(
                f"Circuit breaker triggered at iteration {iteration}. "
                "Increase MAX_ITERATIONS env var if record volume warrants it."
            )

        logger.info("Iteration %d | offset=%d", iteration, offset)
        page = fetch_page(token, offset)

        if not page:
            logger.info("Empty page returned at offset=%d — pagination complete.", offset)
            break

        total_fetched += len(page)

        for user in page:
            if has_wwid(user):
                pending_batch.append(user)
            else:
                skipped += 1
                logger.debug("Skipped user id=%s — no WWID", user.get("id"))

            # Flush batch to S3 when batch size reached
            if len(pending_batch) >= BATCH_SIZE:
                key = write_batch_to_s3(
                    pending_batch, correlation_id, batch_index,
                    S3_BUCKET, S3_PREFIX
                )
                s3_keys.append(key)
                batch_index   += 1
                pending_batch  = []

        offset    += PAGE_SIZE
        iteration += 1

    # Flush any remaining records below BATCH_SIZE threshold
    if pending_batch:
        key = write_batch_to_s3(
            pending_batch, correlation_id, batch_index,
            S3_BUCKET, S3_PREFIX
        )
        s3_keys.append(key)

    total_filtered = sum(0 for _ in []) + (total_fetched - skipped)

    result = {
        "correlationId":  correlation_id,
        "s3Keys":         s3_keys,
        "totalFetched":   total_fetched,
        "totalFiltered":  total_filtered,
        "batchesWritten": len(s3_keys),
        "skippedNoWwid":  skipped,
        "iterations":     iteration,
    }

    logger.info("COMPLETE %s", json.dumps(result))
    return result
