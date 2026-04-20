"""
Lambda: S3 File Processor + HR Data Hub Delta Fetch (Step 3)
--------------------------------------------------------------
- Invoked by Step Functions after Lambda 1 completes
- Reads last processed timestamp from SSM Parameter Store
- Reads each S3 file written by Lambda 1
- Batches WWIDs and calls HR Data Hub API with timestamp for delta records
- Publishes each delta record individually to SQS
- Only updates SSM timestamp on FULL successful completion
"""

import json
import logging
import os
from datetime import datetime, timezone
from typing import Generator

import boto3
import urllib3

logger = logging.getLogger()
logger.setLevel(logging.INFO)

# ── Config ────────────────────────────────────────────────────────────────────
HR_API_URL          = os.environ["HR_DATA_HUB_API_URL"]
HR_SECRET_NAME      = os.environ["HR_DATA_HUB_SECRET_NAME"]
S3_BUCKET           = os.environ["S3_BUCKET"]
S3_ARCHIVE_PREFIX   = os.environ.get("S3_ARCHIVE_PREFIX", "athennian/archive/")
SQS_QUEUE_URL       = os.environ["SQS_QUEUE_URL"]
SSM_TIMESTAMP_KEY   = os.environ["SSM_TIMESTAMP_KEY"]          # e.g. /jnj/integration/last-processed-ts
WWID_BATCH_SIZE     = int(os.environ.get("WWID_BATCH_SIZE", "200"))   # WWIDs per HR API call
HR_PAGE_SIZE        = int(os.environ.get("HR_PAGE_SIZE", "1000"))     # HR Data Hub max per page
MAX_SQS_BATCH       = 10                                               # SQS send_message_batch limit

# ── AWS clients ───────────────────────────────────────────────────────────────
s3_client      = boto3.client("s3")
ssm_client     = boto3.client("ssm")
sqs_client     = boto3.client("sqs")
secrets_client = boto3.client("secretsmanager")
http           = urllib3.PoolManager()


# ── Helpers: Secrets & Config ─────────────────────────────────────────────────

def get_hr_api_token() -> str:
    """Retrieve HR Data Hub API token from Secrets Manager."""
    response = secrets_client.get_secret_value(SecretId=HR_SECRET_NAME)
    return json.loads(response["SecretString"])["api_token"]


def get_last_processed_timestamp() -> str:
    """
    Read last successfully processed timestamp from SSM.
    Falls back to epoch if parameter does not yet exist (first run).
    """
    try:
        response = ssm_client.get_parameter(Name=SSM_TIMESTAMP_KEY)
        ts = response["Parameter"]["Value"]
        logger.info("Last processed timestamp from SSM: %s", ts)
        return ts
    except ssm_client.exceptions.ParameterNotFound:
        fallback = "1970-01-01T00:00:00Z"
        logger.warning("SSM key %s not found — using epoch fallback: %s", SSM_TIMESTAMP_KEY, fallback)
        return fallback


def update_last_processed_timestamp(ts: str) -> None:
    """
    Write new timestamp to SSM.
    ONLY called after the entire run completes successfully.
    """
    ssm_client.put_parameter(
        Name=SSM_TIMESTAMP_KEY,
        Value=ts,
        Type="String",
        Overwrite=True,
    )
    logger.info("SSM timestamp updated → %s", ts)


# ── Helpers: S3 ───────────────────────────────────────────────────────────────

def read_s3_file(bucket: str, key: str) -> dict:
    """Read and parse a JSON file from S3."""
    response = s3_client.get_object(Bucket=bucket, Key=key)
    return json.loads(response["Body"].read().decode("utf-8"))


def archive_s3_file(bucket: str, source_key: str, archive_prefix: str) -> None:
    """
    Move processed S3 file to archive prefix.
    Copy then delete (S3 has no native move).
    """
    filename   = source_key.split("/")[-1]
    archive_key = f"{archive_prefix}{filename}"

    s3_client.copy_object(
        Bucket=bucket,
        CopySource={"Bucket": bucket, "Key": source_key},
        Key=archive_key,
    )
    s3_client.delete_object(Bucket=bucket, Key=source_key)
    logger.info("Archived s3://%s/%s → %s", bucket, source_key, archive_key)


def extract_wwids(records: list) -> list:
    """Extract non-empty WWIDs from Athennian user records."""
    wwids = []
    for r in records:
        wwid = (r.get("customFields") or {}).get("wwid", "")
        if wwid and str(wwid).strip():
            wwids.append(str(wwid).strip())
    return wwids


def chunk(lst: list, size: int) -> Generator:
    """Yield successive chunks of a list."""
    for i in range(0, len(lst), size):
        yield lst[i : i + size]


# ── Helpers: HR Data Hub API ──────────────────────────────────────────────────

def fetch_hr_delta_page(
    token: str,
    wwids: list,
    since_timestamp: str,
    page: int,
) -> dict:
    """
    Call HR Data Hub API for a batch of WWIDs with delta timestamp.
    Returns full response body.
    Raises on HTTP error or non-200 status.
    """
    payload = json.dumps({
        "wwids":         wwids,
        "updatedSince":  since_timestamp,
        "page":          page,
        "pageSize":      HR_PAGE_SIZE,
    }).encode("utf-8")

    response = http.request(
        "POST",
        f"{HR_API_URL}/users/delta",
        body=payload,
        headers={
            "Content-Type": "application/json",
            "Authorization": f"Bearer {token}",
        },
        timeout=urllib3.Timeout(connect=5.0, read=60.0),
        retries=urllib3.Retry(total=3, backoff_factor=1.5),
    )

    if response.status != 200:
        raise RuntimeError(
            f"HR Data Hub API HTTP {response.status}: {response.data.decode()}"
        )

    return json.loads(response.data.decode("utf-8"))


def fetch_all_hr_delta_records(
    token: str,
    wwids: list,
    since_timestamp: str,
) -> list:
    """
    Paginate through ALL HR Data Hub pages for a WWID batch.
    All pages must be retrieved before records are considered complete.
    Returns flat list of all delta records.
    """
    all_records = []
    page        = 1

    while True:
        logger.info(
            "HR Data Hub: fetching page %d for %d WWIDs since %s",
            page, len(wwids), since_timestamp,
        )
        body = fetch_hr_delta_page(token, wwids, since_timestamp, page)

        records   = body.get("records", [])
        all_records.extend(records)

        total_pages = body.get("totalPages", 1)
        logger.info("HR page %d/%d — %d records", page, total_pages, len(records))

        if page >= total_pages or not records:
            break

        page += 1

    return all_records


# ── Helpers: SQS ──────────────────────────────────────────────────────────────

def publish_records_to_sqs(
    records: list,
    correlation_id: str,
    s3_key: str,
) -> dict:
    """
    Publish delta records to SQS individually (per-record for fine-grained retry).
    Uses send_message_batch (max 10) to reduce API call overhead.
    Returns counts of sent and failed messages.
    """
    sent   = 0
    failed = 0

    for batch in chunk(records, MAX_SQS_BATCH):
        entries = []
        for i, record in enumerate(batch):
            entries.append({
                "Id":           str(i),
                "MessageBody":  json.dumps({
                    "correlationId": correlation_id,
                    "sourceS3Key":   s3_key,
                    "hrRecord":      record,
                }),
                # Deduplication via content hash prevents duplicate upserts
                # on re-drive. Only effective if queue has ContentBasedDeduplication
                # or you switch to FIFO + MessageDeduplicationId.
                "MessageAttributes": {
                    "correlationId": {
                        "StringValue": correlation_id,
                        "DataType":    "String",
                    }
                },
            })

        response = sqs_client.send_message_batch(
            QueueUrl=SQS_QUEUE_URL,
            Entries=entries,
        )

        sent   += len(response.get("Successful", []))
        failed += len(response.get("Failed", []))

        if response.get("Failed"):
            for failure in response["Failed"]:
                logger.error(
                    "SQS send failure — Id=%s Code=%s Message=%s",
                    failure["Id"], failure["Code"], failure["Message"],
                )

    return {"sent": sent, "failed": failed}


# ── Handler ───────────────────────────────────────────────────────────────────

def lambda_handler(event: dict, context) -> dict:
    """
    Entry point invoked by Step Functions.

    Input event (passed from Lambda 1 output):
        {
            "correlationId": "...",
            "s3Keys": ["athennian/raw/Athennian_..._batch0001.json", ...],
            "totalFetched": N,
            "totalFiltered": N,
            "batchesWritten": N,
            "skippedNoWwid": N,
            "iterations": N
        }

    Output:
        {
            "correlationId": "...",
            "filesProcessed": N,
            "totalWwids": N,
            "totalDeltaRecords": N,
            "totalSqsSent": N,
            "totalSqsFailed": N,
            "newTimestamp": "...",
            "status": "SUCCESS" | "PARTIAL_FAILURE"
        }
    """
    correlation_id = event.get("correlationId")
    s3_keys        = event.get("s3Keys", [])

    if not s3_keys:
        logger.warning("No S3 keys received from upstream Lambda. Nothing to process.")
        return {"correlationId": correlation_id, "filesProcessed": 0, "status": "NO_OP"}

    logger.info("START correlationId=%s | files=%d", correlation_id, len(s3_keys))

    # Capture run timestamp BEFORE fetching — ensures no changes are missed
    # between Lambda 1 completion and now
    run_timestamp       = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    since_timestamp     = get_last_processed_timestamp()
    hr_token            = get_hr_api_token()

    total_wwids          = 0
    total_delta_records  = 0
    total_sqs_sent       = 0
    total_sqs_failed     = 0
    files_processed      = 0

    for s3_key in s3_keys:
        logger.info("Processing file: %s", s3_key)

        try:
            file_data = read_s3_file(S3_BUCKET, s3_key)
        except Exception as e:
            # Log and re-raise — Step Functions will catch and route to error state
            # Do NOT update SSM timestamp; partial processing is a failure
            logger.error("Failed to read S3 file %s: %s", s3_key, str(e))
            raise

        athennian_records = file_data.get("records", [])
        wwids             = extract_wwids(athennian_records)
        total_wwids       += len(wwids)

        logger.info("File %s — %d Athennian records, %d WWIDs", s3_key, len(athennian_records), len(wwids))

        # Process WWIDs in batches against HR Data Hub
        for wwid_batch in chunk(wwids, WWID_BATCH_SIZE):
            delta_records = fetch_all_hr_delta_records(
                hr_token, wwid_batch, since_timestamp
            )

            if not delta_records:
                logger.info("No delta records for this WWID batch — skipping SQS publish.")
                continue

            total_delta_records += len(delta_records)
            logger.info("Publishing %d delta records to SQS", len(delta_records))

            sqs_result      = publish_records_to_sqs(delta_records, correlation_id, s3_key)
            total_sqs_sent  += sqs_result["sent"]
            total_sqs_failed += sqs_result["failed"]

        # Archive file after successful processing
        archive_s3_file(S3_BUCKET, s3_key, S3_ARCHIVE_PREFIX)
        files_processed += 1

    # ── Only update SSM timestamp if ALL files processed with zero SQS failures ──
    if total_sqs_failed > 0:
        logger.error(
            "SQS failures detected (%d). SSM timestamp NOT updated. "
            "Records will be reprocessed on next run.",
            total_sqs_failed,
        )
        status = "PARTIAL_FAILURE"
    else:
        update_last_processed_timestamp(run_timestamp)
        status = "SUCCESS"

    result = {
        "correlationId":     correlation_id,
        "filesProcessed":    files_processed,
        "totalWwids":        total_wwids,
        "totalDeltaRecords": total_delta_records,
        "totalSqsSent":      total_sqs_sent,
        "totalSqsFailed":    total_sqs_failed,
        "newTimestamp":      run_timestamp if status == "SUCCESS" else since_timestamp,
        "status":            status,
    }

    logger.info("COMPLETE %s", json.dumps(result))
    return result
