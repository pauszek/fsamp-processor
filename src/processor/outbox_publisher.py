"""Token-fenced DynamoDB Streams publisher for the transactional outbox."""

from __future__ import annotations

import json
import os
from datetime import UTC, datetime
from math import ceil
from typing import TYPE_CHECKING, Any, cast
from uuid import uuid4

from aws_lambda_powertools import Logger, Metrics, Tracer
from aws_lambda_powertools.metrics import MetricUnit
from aws_lambda_powertools.utilities.batch import (
    BatchProcessor,
    EventType,
    process_partial_response,
)
from aws_lambda_powertools.utilities.data_classes.dynamo_db_stream_event import (
    DynamoDBRecord,
)
from aws_lambda_powertools.utilities.typing import LambdaContext
from botocore.exceptions import ClientError

from processor.config import Settings, get_settings
from processor.domain.models import (
    OUTBOX_SHARD_COUNT,
    OutboxEvent,
    OutboxEventType,
    OutboxStatus,
)
from processor.infrastructure import AWSClientFactory, enforce_fips

if TYPE_CHECKING:
    from mypy_boto3_dynamodb import DynamoDBClient
    from mypy_boto3_sns import SNSClient

logger = Logger(service="outbox-publisher")
tracer = Tracer(service="outbox-publisher")
metrics = Metrics(namespace="FSAMP/OutboxPublisher")
processor = BatchProcessor(event_type=EventType.DynamoDBStreams)

_sns_client: SNSClient | None = None
_dynamodb_client: DynamoDBClient | None = None
_aws_factory: AWSClientFactory | None = None
_settings: Settings | None = None

_STATUS_NAME = "#status"
_PUBLISHING_VALUE = ":publishing"
_OWNER_VALUE = ":token"


class ClaimUnavailableError(RuntimeError):
    """The row is not terminal, but another worker currently owns the lease."""


def get_aws_factory() -> AWSClientFactory:
    global _aws_factory, _settings
    if _aws_factory is None:
        _settings = get_settings()
        _settings.validate_outbox_runtime()
        enforce_fips(_settings.should_require_fips)
        _aws_factory = AWSClientFactory(
            region=_settings.aws_region,
            endpoint_url=_settings.aws_endpoint_url,
            use_fips=_settings.should_use_fips,
        )
    return _aws_factory


def _setting(name: str, environment_name: str, default: Any) -> Any:
    if _settings is not None:
        return getattr(_settings, name, default)
    value = os.environ.get(environment_name)
    if value is None:
        return default
    return int(value) if isinstance(default, int) else value


def get_sns_topic_arn() -> str:
    return str(_setting("sns_topic_arn", "SNS_TOPIC_ARN", ""))


def get_file_events_topic_arn() -> str:
    return str(_setting("file_events_topic_arn", "FILE_EVENTS_TOPIC_ARN", ""))


def get_processing_events_topic_arn() -> str:
    return str(_setting("processing_events_topic_arn", "PROCESSING_EVENTS_TOPIC_ARN", ""))


def get_outbox_table_name() -> str:
    return str(_setting("outbox_table_name", "OUTBOX_TABLE_NAME", ""))


def get_max_retry_count() -> int:
    value = int(_setting("outbox_max_retry_count", "MAX_RETRY_COUNT", 5))
    if not 1 <= value <= 100:
        raise ValueError("MAX_RETRY_COUNT must be between 1 and 100")
    return value


def get_claim_ttl_seconds() -> int:
    value = int(_setting("publish_claim_ttl_seconds", "PUBLISH_CLAIM_TTL_SECONDS", 300))
    if not 30 <= value <= 3600:
        raise ValueError("PUBLISH_CLAIM_TTL_SECONDS must be between 30 and 3600")
    return value


def get_retention_seconds() -> int:
    value = int(_setting("outbox_retention_seconds", "OUTBOX_RETENTION_SECONDS", 2592000))
    if not 86400 <= value <= 31536000:
        raise ValueError("OUTBOX_RETENTION_SECONDS must be between 1 and 365 days")
    return value


def get_sns_client() -> SNSClient:
    global _sns_client
    if _sns_client is None:
        _sns_client = get_aws_factory().get_sns_client()
    return _sns_client


def get_dynamodb_client() -> DynamoDBClient:
    global _dynamodb_client
    if _dynamodb_client is None:
        _dynamodb_client = get_aws_factory().get_dynamodb_client()
    return _dynamodb_client


def get_topic_arn_for_event(outbox_event: OutboxEvent) -> str:
    if (
        outbox_event.event_type == OutboxEventType.FILE_UPLOADED
        or outbox_event.aggregate_type == "FileUpload"
    ):
        topic = get_file_events_topic_arn() or get_sns_topic_arn()
        if topic:
            return topic
        raise ValueError("FILE_EVENTS_TOPIC_ARN is required")
    topic = get_processing_events_topic_arn() or get_sns_topic_arn()
    if topic:
        return topic
    raise ValueError("PROCESSING_EVENTS_TOPIC_ARN is required")


def dynamodb_event_name(record: DynamoDBRecord) -> str:
    value = record.event_name
    name = getattr(value, "name", None)
    if isinstance(name, str) and name in {"INSERT", "MODIFY", "REMOVE"}:
        return name
    wire_value = getattr(value, "value", None)
    if isinstance(wire_value, str):
        return wire_value
    return str(value).split(".")[-1]


def _is_conditional_failure(error: ClientError) -> bool:
    return error.response.get("Error", {}).get("Code") == "ConditionalCheckFailedException"


def _key(event: OutboxEvent) -> dict[str, dict[str, str]]:
    return {
        "PK": {"S": str(event.outbox_partition)},
        "SK": {"S": f"EVENT#{event.event_id}"},
    }


def _read_live_event(event: OutboxEvent) -> OutboxEvent | None:
    response = get_dynamodb_client().get_item(
        TableName=get_outbox_table_name(),
        Key=_key(event),
        ConsistentRead=True,
    )
    item = response.get("Item")
    return OutboxEvent.from_dynamodb_item(item) if item else None


@tracer.capture_method
def claim_event_for_publish(outbox_event: OutboxEvent) -> str | None:
    """Acquire a renewable lease; return ``None`` only for terminal PUBLISHED."""
    dynamodb = get_dynamodb_client()
    now = datetime.now(UTC)
    now_epoch = int(now.timestamp())
    token = str(uuid4())
    expires = now_epoch + get_claim_ttl_seconds()
    try:
        dynamodb.update_item(
            TableName=get_outbox_table_name(),
            Key=_key(outbox_event),
            UpdateExpression=(
                "SET #status = :publishing, publishingStartedAt = :started, "
                "publisherClaimExpiresAt = :expires, publisherClaimToken = :token, "
                "GSI1PK = :gsi"
            ),
            ConditionExpression=(
                "((#status = :pending OR #status = :failed) "
                "OR (#status = :publishing AND publisherClaimExpiresAt < :now)) "
                "AND (attribute_not_exists(retryCount) OR retryCount < :maxRetries)"
            ),
            ExpressionAttributeNames={_STATUS_NAME: "status"},
            ExpressionAttributeValues={
                ":pending": {"S": OutboxStatus.PENDING.value},
                ":failed": {"S": OutboxStatus.FAILED.value},
                _PUBLISHING_VALUE: {"S": OutboxStatus.PUBLISHING.value},
                ":started": {"S": now.isoformat()},
                ":expires": {"N": str(expires)},
                ":now": {"N": str(now_epoch)},
                _OWNER_VALUE: {"S": token},
                ":maxRetries": {"N": str(get_max_retry_count())},
                ":gsi": {"S": f"STATUS#PUBLISHING#{outbox_event.outbox_shard}"},
            },
        )
        return token
    except ClientError as error:
        if not _is_conditional_failure(error):
            raise
        live = _read_live_event(outbox_event)
        if live is not None and live.status == OutboxStatus.PUBLISHED:
            return None
        live_status = live.status.value if live is not None else "MISSING"
        raise ClaimUnavailableError(
            f"Outbox event is not terminal and cannot be claimed (status={live_status})"
        ) from error


@tracer.capture_method
def publish_to_sns(outbox_event: OutboxEvent) -> str:
    """Publish only a schema-validated canonical event body."""
    payload = outbox_event.to_sns_message()
    attributes = outbox_event.to_sns_attributes()
    attributes.update(
        {
            "fileId": {
                "DataType": "String",
                "StringValue": str(payload["fileId"]),
            },
            "correlationId": {
                "DataType": "String",
                "StringValue": str(payload["correlationId"]),
            },
            "idempotencyKey": {
                "DataType": "String",
                "StringValue": outbox_event.event_id,
            },
        }
    )
    response = get_sns_client().publish(
        TopicArn=get_topic_arn_for_event(outbox_event),
        Message=json.dumps(payload, separators=(",", ":")),
        MessageAttributes=attributes,
    )
    return str(response["MessageId"])


@tracer.capture_method
def mark_event_published(outbox_event: OutboxEvent, claim_token: str) -> None:
    """Commit publication only while this worker still owns the lease."""
    now = datetime.now(UTC)
    try:
        get_dynamodb_client().update_item(
            TableName=get_outbox_table_name(),
            Key=_key(outbox_event),
            UpdateExpression=(
                "SET #status = :published, publishedAt = :publishedAt, "
                "GSI1PK = :gsi, #ttl = :ttl "
                "REMOVE publishingStartedAt, publisherClaimExpiresAt, publisherClaimToken"
            ),
            ConditionExpression="#status = :publishing AND publisherClaimToken = :token",
            ExpressionAttributeNames={_STATUS_NAME: "status", "#ttl": "ttl"},
            ExpressionAttributeValues={
                ":published": {"S": OutboxStatus.PUBLISHED.value},
                _PUBLISHING_VALUE: {"S": OutboxStatus.PUBLISHING.value},
                ":publishedAt": {"S": now.isoformat()},
                _OWNER_VALUE: {"S": claim_token},
                ":gsi": {"S": f"STATUS#PUBLISHED#{outbox_event.outbox_shard}"},
                ":ttl": {"N": str(int(now.timestamp()) + get_retention_seconds())},
            },
        )
    except ClientError as error:
        if not _is_conditional_failure(error):
            raise
        live = _read_live_event(outbox_event)
        if live is not None and live.status == OutboxStatus.PUBLISHED:
            return
        raise ClaimUnavailableError("Publisher lease was lost before commit") from error


@tracer.capture_method
def mark_event_failed(
    outbox_event: OutboxEvent,
    error: str,
    claim_token: str,
) -> None:
    """Record failure only while this worker owns the lease; never downgrade."""
    try:
        get_dynamodb_client().update_item(
            TableName=get_outbox_table_name(),
            Key=_key(outbox_event),
            UpdateExpression=(
                "SET #status = :failed, lastError = :error, "
                "retryCount = if_not_exists(retryCount, :zero) + :inc, GSI1PK = :gsi "
                "REMOVE publishingStartedAt, publisherClaimExpiresAt, publisherClaimToken"
            ),
            ConditionExpression="#status = :publishing AND publisherClaimToken = :token",
            ExpressionAttributeNames={_STATUS_NAME: "status"},
            ExpressionAttributeValues={
                ":failed": {"S": OutboxStatus.FAILED.value},
                _PUBLISHING_VALUE: {"S": OutboxStatus.PUBLISHING.value},
                _OWNER_VALUE: {"S": claim_token},
                ":error": {"S": error[:2000]},
                ":zero": {"N": "0"},
                ":inc": {"N": "1"},
                ":gsi": {"S": f"STATUS#FAILED#{outbox_event.outbox_shard}"},
            },
        )
    except ClientError as client_error:
        if not _is_conditional_failure(client_error):
            raise
        live = _read_live_event(outbox_event)
        if live is not None and live.status == OutboxStatus.PUBLISHED:
            return
        raise ClaimUnavailableError(
            "Publisher lease was lost before failure commit"
        ) from client_error


@tracer.capture_method
def record_handler(record: DynamoDBRecord) -> dict[str, Any]:
    """Publish an INSERT snapshot, reconciling every decision with live state."""
    event_name = dynamodb_event_name(record)
    if event_name != "INSERT":
        return {"status": "skipped", "reason": "not_insert"}
    new_image = getattr(record.dynamodb, "new_image", None)
    if not new_image:
        raise ValueError("DynamoDB INSERT record is missing NewImage")
    if OutboxEvent._dynamodb_value(new_image.get("status")) != OutboxStatus.PENDING.value:
        return {"status": "skipped", "reason": "not_pending"}

    outbox_event = OutboxEvent.from_dynamodb_item(new_image)
    claim_token: str | None = None
    try:
        claim_token = claim_event_for_publish(outbox_event)
        if claim_token is None:
            return {"status": "skipped", "reason": "already_published"}
        publish_to_sns(outbox_event)
        mark_event_published(outbox_event, claim_token)
        metrics.add_metric(name="EventsPublished", unit=MetricUnit.Count, value=1)
        metrics.add_metadata(key="event_type", value=outbox_event.event_type.value)
        return {
            "status": "published",
            "event_id": outbox_event.event_id,
            "event_type": outbox_event.event_type.value,
        }
    except Exception as error:
        if claim_token is not None:
            try:
                mark_event_failed(outbox_event, str(error), claim_token)
            except Exception:
                logger.exception(
                    "Could not persist token-fenced outbox failure",
                    event_id=outbox_event.event_id,
                )
        metrics.add_metric(name="EventsFailedToPublish", unit=MetricUnit.Count, value=1)
        raise


def _query_retryable(status: OutboxStatus, limit: int = 100) -> list[dict[str, Any]]:
    """Query every status shard and follow all result pages."""
    items: list[dict[str, Any]] = []
    now = int(datetime.now(UTC).timestamp())
    per_shard_limit = max(1, ceil(limit / OUTBOX_SHARD_COUNT))
    for shard_number in range(OUTBOX_SHARD_COUNT):
        shard = f"{shard_number:02x}"
        start_key: dict[str, Any] | None = None
        shard_count = 0
        while shard_count < per_shard_limit:
            values: dict[str, Any] = {
                ":status": {"S": f"STATUS#{status.value}#{shard}"},
            }
            request: dict[str, Any] = {
                "TableName": get_outbox_table_name(),
                "IndexName": "GSI1",
                "KeyConditionExpression": "GSI1PK = :status",
                "ExpressionAttributeValues": values,
                "Limit": per_shard_limit - shard_count,
            }
            if status == OutboxStatus.FAILED:
                request["FilterExpression"] = "retryCount < :maxRetries"
                values[":maxRetries"] = {"N": str(get_max_retry_count())}
            elif status == OutboxStatus.PUBLISHING:
                request["FilterExpression"] = "publisherClaimExpiresAt < :now"
                values[":now"] = {"N": str(now)}
            if start_key:
                request["ExclusiveStartKey"] = start_key
            response = get_dynamodb_client().query(**request)
            page = response.get("Items", [])
            items.extend(page)
            shard_count += len(page)
            start_key = response.get("LastEvaluatedKey")
            if not start_key:
                break
    return sorted(
        items,
        key=lambda item: str(OutboxEvent._dynamodb_value(item.get("createdAt")) or ""),
    )[:limit]


@logger.inject_lambda_context(log_event=False)
@tracer.capture_lambda_handler
@metrics.log_metrics(capture_cold_start_metric=True)
def lambda_handler(event: dict[str, Any], context: LambdaContext) -> dict[str, Any]:
    """Return partial failures so the stream mapping can retry and redrive."""
    if not (
        get_file_events_topic_arn() or get_processing_events_topic_arn() or get_sns_topic_arn()
    ):
        raise ValueError("At least one SNS topic ARN is required")
    if not get_outbox_table_name():
        raise ValueError("OUTBOX_TABLE_NAME is required")
    return cast(
        dict[str, Any],
        process_partial_response(
            event=event,
            record_handler=record_handler,
            processor=processor,
            context=context,
        ),
    )


@logger.inject_lambda_context(log_event=False)
@tracer.capture_lambda_handler
@metrics.log_metrics
def retry_handler(event: dict[str, Any], context: LambdaContext) -> dict[str, Any]:
    """Recover PENDING, FAILED, and expired PUBLISHING events across all shards."""
    del event, context
    items = (
        _query_retryable(OutboxStatus.PENDING)
        + _query_retryable(OutboxStatus.FAILED)
        + _query_retryable(OutboxStatus.PUBLISHING)
    )
    unique = {(item["PK"]["S"], item["SK"]["S"]): item for item in items}
    succeeded = 0
    failed = 0
    for item in unique.values():
        outbox_event = OutboxEvent.from_dynamodb_item(item)
        token: str | None = None
        try:
            token = claim_event_for_publish(outbox_event)
            if token is None:
                continue
            publish_to_sns(outbox_event)
            mark_event_published(outbox_event, token)
            succeeded += 1
        except Exception as error:
            failed += 1
            if token is not None:
                try:
                    mark_event_failed(outbox_event, str(error), token)
                except Exception:
                    logger.exception(
                        "Could not persist retry failure",
                        event_id=outbox_event.event_id,
                    )
    metrics.add_metric(name="EventsRetried", unit=MetricUnit.Count, value=succeeded)
    metrics.add_metric(name="EventsRetryFailed", unit=MetricUnit.Count, value=failed)
    return {
        "statusCode": 200,
        "body": {
            "success_count": succeeded,
            "failure_count": failed,
            "total_processed": len(unique),
        },
    }
