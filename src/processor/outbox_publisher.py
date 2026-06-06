"""
AWS Lambda handler for the Outbox Publisher.

This Lambda is triggered by DynamoDB Streams when new events are written
to the outbox table. It reads the event, publishes to SNS, and marks
the event as published.

Architecture:
    DynamoDB Outbox Table
           |
    DynamoDB Streams (NEW_IMAGE)
           |
    Outbox Publisher Lambda
           |
        SNS Topic
           |
    Downstream Consumers (SQS, Lambda, etc.)

Benefits of Outbox Pattern:
- At-least-once delivery guarantee
- Decoupled from message broker availability
- Event replay capability
- Audit trail of all published events
"""

import json
import os
from datetime import UTC, datetime, timedelta
from typing import TYPE_CHECKING, Any, cast

from aws_lambda_powertools import Logger, Metrics, Tracer

if TYPE_CHECKING:
    from mypy_boto3_dynamodb import DynamoDBClient
    from mypy_boto3_sns import SNSClient
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

from processor.config import Settings, get_settings
from processor.domain.models import OutboxEvent, OutboxStatus
from processor.infrastructure import AWSClientFactory, enforce_fips

logger = Logger(service="outbox-publisher")
tracer = Tracer(service="outbox-publisher")
metrics = Metrics(namespace="FSAMP/OutboxPublisher")
processor = BatchProcessor(event_type=EventType.DynamoDBStreams)
MAX_RETRY_COUNT = int(os.environ.get("MAX_RETRY_COUNT", "3"))
PUBLISH_CLAIM_TTL_SECONDS = int(os.environ.get("PUBLISH_CLAIM_TTL_SECONDS", "300"))
_sns_client: SNSClient | None = None
_dynamodb_client: DynamoDBClient | None = None
_aws_factory: AWSClientFactory | None = None
_settings: Settings | None = None


def get_aws_factory() -> AWSClientFactory:
    """Get AWS client factory (singleton for Lambda warm starts)."""
    global _aws_factory, _settings

    if _aws_factory is None:
        _settings = get_settings()
        enforce_fips(_settings.should_require_fips)
        _aws_factory = AWSClientFactory(
            region=_settings.aws_region,
            endpoint_url=_settings.aws_endpoint_url,
            use_fips=_settings.should_use_fips,
        )

    return _aws_factory


def get_sns_topic_arn() -> str:
    """Resolve SNS topic ARN from settings or environment."""
    if _settings and _settings.sns_topic_arn:
        return _settings.sns_topic_arn
    return os.environ.get("SNS_TOPIC_ARN", "")


def get_outbox_table_name() -> str:
    """Resolve outbox table name from settings or environment."""
    if _settings and _settings.outbox_table_name:
        return _settings.outbox_table_name
    return os.environ.get("OUTBOX_TABLE_NAME", "")


def get_sns_client() -> SNSClient:
    """Get SNS client (singleton for Lambda warm starts)."""
    global _sns_client
    if _sns_client is None:
        _sns_client = get_aws_factory().get_sns_client()
    return _sns_client


def get_dynamodb_client() -> DynamoDBClient:
    """Get DynamoDB client (singleton for Lambda warm starts)."""
    global _dynamodb_client
    if _dynamodb_client is None:
        _dynamodb_client = get_aws_factory().get_dynamodb_client()
    return _dynamodb_client


@tracer.capture_method
def record_handler(record: DynamoDBRecord) -> dict[str, Any]:
    """
    Process a single DynamoDB Stream record.

    This is triggered for each INSERT to the outbox table.
    We only process INSERT events (new outbox items).

    Args:
        record: DynamoDB Streams record.

    Returns:
        Result dictionary with event details.
    """
    event_name = record.event_name
    event_id = record.event_id

    if str(record.event_name) != "INSERT":
        logger.debug("Skipping non-INSERT event", event_name=event_name, event_id=event_id)
        return {"status": "skipped", "reason": "not_insert"}

    new_image = getattr(record.dynamodb, "new_image", None)
    if not new_image:
        logger.warning("No new image in stream record", event_name=event_name, event_id=event_id)
        return {"status": "skipped", "reason": "no_new_image"}

    status = OutboxEvent._dynamodb_value(new_image.get("status"))
    if status != OutboxStatus.PENDING.value:
        logger.debug(
            "Skipping non-PENDING event", status=status, event_name=event_name, event_id=event_id
        )
        return {"status": "skipped", "reason": "not_pending", "event_status": status}

    try:
        outbox_event = OutboxEvent.from_dynamodb_item(new_image)

        logger.info(
            "Claiming outbox event for SNS publish",
            outbox_event_id=outbox_event.event_id,
            event_type=outbox_event.event_type.value,
            aggregate_id=outbox_event.aggregate_id,
            event_name=event_name,
            event_id=event_id,
        )

        if not claim_event_for_publish(outbox_event):
            logger.info("Outbox event already claimed", event_id=outbox_event.event_id)
            return {
                "status": "skipped",
                "reason": "already_claimed",
                "event_id": outbox_event.event_id,
            }

        publish_to_sns(outbox_event)

        mark_event_published(outbox_event)

        metrics.add_metric(
            name="EventsPublished",
            unit=MetricUnit.Count,
            value=1,
        )
        metrics.add_metadata(
            key="event_type",
            value=outbox_event.event_type.value,
        )

        logger.info("Outbox event published successfully", event_id=outbox_event.event_id)

        return {
            "status": "published",
            "event_id": outbox_event.event_id,
            "event_type": outbox_event.event_type.value,
        }

    except Exception as e:
        logger.exception("Failed to publish outbox event", event_name=event_name, event_id=event_id)

        try:
            dynamo_event_id = OutboxEvent._dynamodb_value(new_image.get("eventId"))
            if dynamo_event_id:
                aggregate_type = (
                    OutboxEvent._dynamodb_value(new_image.get("aggregateType")) or "FileProcessing"
                )
                mark_event_failed(str(dynamo_event_id), str(e), aggregate_type=str(aggregate_type))
        except Exception:
            logger.exception(
                "Failed to mark event as failed", event_name=event_name, event_id=event_id
            )

        metrics.add_metric(
            name="EventsFailedToPublish",
            unit=MetricUnit.Count,
            value=1,
        )

        raise


@tracer.capture_method
def publish_to_sns(outbox_event: OutboxEvent) -> str:
    """
    Publish outbox event to SNS topic.

    Args:
        outbox_event: The event to publish.

    Returns:
        SNS message ID.
    """
    sns = get_sns_client()

    message_payload = (
        outbox_event.payload
        if "schemaVersion" in outbox_event.payload and "eventType" in outbox_event.payload
        else outbox_event.to_sns_message()
    )
    message = json.dumps(message_payload)
    attributes = outbox_event.to_sns_attributes()
    if "fileId" in outbox_event.payload:
        attributes["fileId"] = {
            "DataType": "String",
            "StringValue": str(outbox_event.payload["fileId"]),
        }
    if "correlationId" in outbox_event.payload:
        attributes["correlationId"] = {
            "DataType": "String",
            "StringValue": str(outbox_event.payload["correlationId"]),
        }
    # Stable dedupe key for consumers of SNS standard topics.
    attributes["idempotencyKey"] = {
        "DataType": "String",
        "StringValue": outbox_event.event_id,
    }

    topic_arn = get_sns_topic_arn()
    response = sns.publish(
        TopicArn=topic_arn,
        Message=message,
        MessageAttributes=attributes,
    )

    message_id = str(response["MessageId"])
    logger.debug("Published to SNS", message_id=message_id)

    return message_id


@tracer.capture_method
def claim_event_for_publish(
    outbox_event: OutboxEvent,
    expected_status: OutboxStatus = OutboxStatus.PENDING,
) -> bool:
    """Claim an outbox event before publishing it to SNS."""
    dynamodb = get_dynamodb_client()
    now = datetime.now(UTC)
    claim_expires_at = int((now + timedelta(seconds=PUBLISH_CLAIM_TTL_SECONDS)).timestamp())

    try:
        dynamodb.update_item(
            TableName=get_outbox_table_name(),
            Key={
                "PK": {"S": f"OUTBOX#{outbox_event.aggregate_type}"},
                "SK": {"S": f"EVENT#{outbox_event.event_id}"},
            },
            UpdateExpression=(
                "SET #status = :publishing, publishingStartedAt = :started, "
                "publisherClaimExpiresAt = :expires, GSI1PK = :gsi1pk"
            ),
            ConditionExpression="#status = :expected_status",
            ExpressionAttributeNames={"#status": "status"},
            ExpressionAttributeValues={
                ":publishing": {"S": OutboxStatus.PUBLISHING.value},
                ":started": {"S": now.isoformat()},
                ":expires": {"N": str(claim_expires_at)},
                ":gsi1pk": {"S": f"STATUS#{OutboxStatus.PUBLISHING.value}"},
                ":expected_status": {"S": expected_status.value},
            },
        )
        logger.debug("Claimed outbox event", event_id=outbox_event.event_id)
        return True
    except dynamodb.exceptions.ConditionalCheckFailedException:
        logger.info(
            "Skipping outbox event because it is no longer claimable",
            event_id=outbox_event.event_id,
            expected_status=expected_status.value,
        )
        return False


@tracer.capture_method
def mark_event_published(outbox_event: OutboxEvent) -> None:
    """Mark a claimed outbox event as published in DynamoDB."""
    dynamodb = get_dynamodb_client()
    now = datetime.now(UTC).isoformat()
    ttl = int((datetime.now(UTC) + timedelta(hours=24)).timestamp())

    try:
        dynamodb.update_item(
            TableName=get_outbox_table_name(),
            Key={
                "PK": {"S": f"OUTBOX#{outbox_event.aggregate_type}"},
                "SK": {"S": f"EVENT#{outbox_event.event_id}"},
            },
            UpdateExpression=(
                "SET #status = :status, publishedAt = :published, "
                "GSI1PK = :gsi1pk, #ttl = :ttl "
                "REMOVE publishingStartedAt, publisherClaimExpiresAt"
            ),
            ConditionExpression="#status = :publishing",
            ExpressionAttributeNames={
                "#status": "status",
                "#ttl": "ttl",
            },
            ExpressionAttributeValues={
                ":status": {"S": OutboxStatus.PUBLISHED.value},
                ":publishing": {"S": OutboxStatus.PUBLISHING.value},
                ":published": {"S": now},
                ":gsi1pk": {"S": f"STATUS#{OutboxStatus.PUBLISHED.value}"},
                ":ttl": {"N": str(ttl)},
            },
        )
        logger.debug("Marked event as published", event_id=outbox_event.event_id)
    except dynamodb.exceptions.ConditionalCheckFailedException:
        logger.info(
            "Outbox event already marked as published; skipping idempotent update",
            event_id=outbox_event.event_id,
        )


@tracer.capture_method
def mark_event_failed(event_id: str, error: str, aggregate_type: str = "FileProcessing") -> None:
    """Mark outbox event as failed in DynamoDB."""
    dynamodb = get_dynamodb_client()

    dynamodb.update_item(
        TableName=get_outbox_table_name(),
        Key={
            "PK": {"S": f"OUTBOX#{aggregate_type}"},
            "SK": {"S": f"EVENT#{event_id}"},
        },
        UpdateExpression=(
            "SET #status = :status, lastError = :error, "
            "retryCount = if_not_exists(retryCount, :zero) + :inc, "
            "GSI1PK = :gsi1pk "
            "REMOVE publishingStartedAt, publisherClaimExpiresAt"
        ),
        ExpressionAttributeNames={
            "#status": "status",
        },
        ExpressionAttributeValues={
            ":status": {"S": OutboxStatus.FAILED.value},
            ":error": {"S": error[:1000]},  # Truncate long errors
            ":inc": {"N": "1"},
            ":zero": {"N": "0"},
            ":gsi1pk": {"S": f"STATUS#{OutboxStatus.FAILED.value}"},
        },
    )

    logger.warning("Marked event as failed", event_id=event_id, error=error)


@logger.inject_lambda_context(log_event=False)
@tracer.capture_lambda_handler
@metrics.log_metrics(capture_cold_start_metric=True)
def lambda_handler(event: dict[str, Any], context: LambdaContext) -> dict[str, Any]:
    """
    AWS Lambda handler for Outbox Publisher.

    Triggered by DynamoDB Streams when new events are written to the
    outbox table. Uses batch processing with partial failure support.

    Args:
        event: DynamoDB Streams event with Records.
        context: Lambda context.

    Returns:
        Batch response with itemIdentifier for failed records.
    """
    logger.info(
        "Processing DynamoDB Streams batch",
        record_count=len(event.get("Records", [])),
    )

    if not get_sns_topic_arn():
        raise ValueError("SNS_TOPIC_ARN environment variable not set")
    if not get_outbox_table_name():
        raise ValueError("OUTBOX_TABLE_NAME environment variable not set")

    return cast(
        dict[str, Any],
        process_partial_response(
            event=event,
            record_handler=record_handler,
            processor=processor,
            context=context,
        ),
    )


@logger.inject_lambda_context
@tracer.capture_lambda_handler
@metrics.log_metrics
def retry_handler(event: dict[str, Any], context: LambdaContext) -> dict[str, Any]:
    """
    Lambda handler for retrying failed outbox events.

    This can be triggered by CloudWatch Events (scheduled) to retry
    events that failed to publish.

    Args:
        event: CloudWatch Events scheduled event.
        context: Lambda context.

    Returns:
        Summary of retry results.
    """
    logger.info("Starting retry of failed outbox events")

    dynamodb = get_dynamodb_client()

    failed_response = dynamodb.query(
        TableName=get_outbox_table_name(),
        IndexName="GSI1",
        KeyConditionExpression="GSI1PK = :status",
        FilterExpression="retryCount < :max_retries",
        ExpressionAttributeValues={
            ":status": {"S": f"STATUS#{OutboxStatus.FAILED.value}"},
            ":max_retries": {"N": str(MAX_RETRY_COUNT)},
        },
        Limit=100,
    )

    publishing_response = dynamodb.query(
        TableName=get_outbox_table_name(),
        IndexName="GSI1",
        KeyConditionExpression="GSI1PK = :status",
        FilterExpression="publisherClaimExpiresAt < :now",
        ExpressionAttributeValues={
            ":status": {"S": f"STATUS#{OutboxStatus.PUBLISHING.value}"},
            ":now": {"N": str(int(datetime.now(UTC).timestamp()))},
        },
        Limit=100,
    )

    items = failed_response.get("Items", []) + publishing_response.get("Items", [])
    logger.info(f"Found {len(items)} outbox events to retry")

    success_count = 0
    failure_count = 0

    for item in items:
        outbox_event: OutboxEvent | None = None
        try:
            outbox_event = OutboxEvent.from_dynamodb_item(item)

            if not claim_event_for_publish(outbox_event, expected_status=outbox_event.status):
                continue

            publish_to_sns(outbox_event)
            mark_event_published(outbox_event)

            success_count += 1
            logger.info(f"Successfully retried event {outbox_event.event_id}")

        except Exception as exc:
            failure_count += 1
            logger.exception("Failed to retry event", item_pk=item.get("PK", {}).get("S"))
            if outbox_event is not None:
                mark_event_failed(
                    outbox_event.event_id,
                    str(exc),
                    aggregate_type=outbox_event.aggregate_type,
                )

    metrics.add_metric(name="EventsRetried", unit=MetricUnit.Count, value=success_count)
    metrics.add_metric(name="EventsRetryFailed", unit=MetricUnit.Count, value=failure_count)

    return {
        "statusCode": 200,
        "body": {
            "success_count": success_count,
            "failure_count": failure_count,
            "total_processed": len(items),
        },
    }
