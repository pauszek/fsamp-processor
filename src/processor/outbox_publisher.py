# =============================================================================
# Outbox Publisher Lambda Handler
# =============================================================================
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
from typing import Any

import boto3
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

from processor.domain.models import OutboxEvent, OutboxStatus

# =============================================================================
# AWS Lambda Powertools Configuration
# =============================================================================

logger = Logger(service="outbox-publisher")
tracer = Tracer(service="outbox-publisher")
metrics = Metrics(namespace="FSAMP/OutboxPublisher")
processor = BatchProcessor(event_type=EventType.DynamoDBStreams)

# =============================================================================
# Environment Configuration
# =============================================================================

SNS_TOPIC_ARN = os.environ.get("SNS_TOPIC_ARN", "")
OUTBOX_TABLE_NAME = os.environ.get("OUTBOX_TABLE_NAME", "")
MAX_RETRY_COUNT = int(os.environ.get("MAX_RETRY_COUNT", "3"))

# =============================================================================
# AWS Clients (Reused across warm invocations)
# =============================================================================

_sns_client = None
_dynamodb_client = None


def get_sns_client():
    """Get SNS client (singleton for Lambda warm starts)."""
    global _sns_client
    if _sns_client is None:
        _sns_client = boto3.client("sns")
    return _sns_client


def get_dynamodb_client():
    """Get DynamoDB client (singleton for Lambda warm starts)."""
    global _dynamodb_client
    if _dynamodb_client is None:
        _dynamodb_client = boto3.client("dynamodb")
    return _dynamodb_client


# =============================================================================
# Record Processing
# =============================================================================


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
    log = logger.bind(
        event_name=record.event_name,
        event_id=record.event_id,
    )

    # Only process INSERT events (new outbox items)
    if record.event_name != "INSERT":
        log.debug("Skipping non-INSERT event")
        return {"status": "skipped", "reason": "not_insert"}

    # Get the new image (the inserted item)
    new_image = record.dynamodb.new_image
    if not new_image:
        log.warning("No new image in stream record")
        return {"status": "skipped", "reason": "no_new_image"}

    # Only process PENDING events
    status = new_image.get("status")
    if status != OutboxStatus.PENDING.value:
        log.debug("Skipping non-PENDING event", status=status)
        return {"status": "skipped", "reason": "not_pending", "event_status": status}

    try:
        # Parse outbox event from DynamoDB item
        outbox_event = OutboxEvent.from_dynamodb_item(new_image)
        
        log = logger.bind(
            outbox_event_id=outbox_event.event_id,
            event_type=outbox_event.event_type.value,
            aggregate_id=outbox_event.aggregate_id,
        )
        
        log.info("Publishing outbox event to SNS")

        # Publish to SNS
        publish_to_sns(outbox_event)
        
        # Mark as published in DynamoDB
        mark_event_published(outbox_event)

        # Record metrics
        metrics.add_metric(
            name="EventsPublished",
            unit=MetricUnit.Count,
            value=1,
        )
        metrics.add_metadata(
            key="event_type",
            value=outbox_event.event_type.value,
        )

        log.info("Outbox event published successfully")

        return {
            "status": "published",
            "event_id": outbox_event.event_id,
            "event_type": outbox_event.event_type.value,
        }

    except Exception as e:
        log.exception("Failed to publish outbox event")
        
        # Try to mark as failed
        try:
            event_id = new_image.get("eventId")
            if event_id:
                mark_event_failed(event_id, str(e))
        except Exception:
            log.exception("Failed to mark event as failed")
        
        # Record failure metric
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
    
    message = json.dumps(outbox_event.to_sns_message())
    attributes = outbox_event.to_sns_attributes()
    
    response = sns.publish(
        TopicArn=SNS_TOPIC_ARN,
        Message=message,
        MessageAttributes=attributes,
        # Use message group ID for FIFO topics (if applicable)
        # MessageGroupId=outbox_event.message_group_id,
    )
    
    message_id = response["MessageId"]
    logger.debug("Published to SNS", message_id=message_id)
    
    return message_id


@tracer.capture_method
def mark_event_published(outbox_event: OutboxEvent) -> None:
    """Mark outbox event as published in DynamoDB."""
    from datetime import datetime, timedelta
    
    dynamodb = get_dynamodb_client()
    now = datetime.utcnow().isoformat()
    ttl = int((datetime.utcnow() + timedelta(hours=24)).timestamp())
    
    dynamodb.update_item(
        TableName=OUTBOX_TABLE_NAME,
        Key={
            "PK": {"S": f"OUTBOX#{outbox_event.aggregate_type}"},
            "SK": {"S": f"EVENT#{outbox_event.event_id}"},
        },
        UpdateExpression=(
            "SET #status = :status, publishedAt = :published, "
            "GSI1PK = :gsi1pk, #ttl = :ttl"
        ),
        ExpressionAttributeNames={
            "#status": "status",
            "#ttl": "ttl",
        },
        ExpressionAttributeValues={
            ":status": {"S": OutboxStatus.PUBLISHED.value},
            ":published": {"S": now},
            ":gsi1pk": {"S": f"STATUS#{OutboxStatus.PUBLISHED.value}"},
            ":ttl": {"N": str(ttl)},
        },
    )
    
    logger.debug("Marked event as published", event_id=outbox_event.event_id)


@tracer.capture_method
def mark_event_failed(event_id: str, error: str, aggregate_type: str = "FileProcessing") -> None:
    """Mark outbox event as failed in DynamoDB."""
    dynamodb = get_dynamodb_client()
    
    dynamodb.update_item(
        TableName=OUTBOX_TABLE_NAME,
        Key={
            "PK": {"S": f"OUTBOX#{aggregate_type}"},
            "SK": {"S": f"EVENT#{event_id}"},
        },
        UpdateExpression=(
            "SET #status = :status, lastError = :error, "
            "retryCount = if_not_exists(retryCount, :zero) + :inc, "
            "GSI1PK = :gsi1pk"
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


# =============================================================================
# Lambda Handler
# =============================================================================


@logger.inject_lambda_context(log_event=True)
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

    # Validate configuration
    if not SNS_TOPIC_ARN:
        raise ValueError("SNS_TOPIC_ARN environment variable not set")
    if not OUTBOX_TABLE_NAME:
        raise ValueError("OUTBOX_TABLE_NAME environment variable not set")

    # Process batch with partial failure support
    return process_partial_response(
        event=event,
        record_handler=record_handler,
        processor=processor,
        context=context,
    )


# =============================================================================
# Retry Handler (Optional - for manual retry of failed events)
# =============================================================================


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
    
    # Query failed events
    response = dynamodb.query(
        TableName=OUTBOX_TABLE_NAME,
        IndexName="GSI1",
        KeyConditionExpression="GSI1PK = :status",
        FilterExpression="retryCount < :max_retries",
        ExpressionAttributeValues={
            ":status": {"S": f"STATUS#{OutboxStatus.FAILED.value}"},
            ":max_retries": {"N": str(MAX_RETRY_COUNT)},
        },
        Limit=100,
    )
    
    items = response.get("Items", [])
    logger.info(f"Found {len(items)} failed events to retry")
    
    success_count = 0
    failure_count = 0
    
    for item in items:
        try:
            outbox_event = OutboxEvent.from_dynamodb_item(item)
            
            # Reset status to PENDING for retry
            dynamodb.update_item(
                TableName=OUTBOX_TABLE_NAME,
                Key={
                    "PK": {"S": f"OUTBOX#{outbox_event.aggregate_type}"},
                    "SK": {"S": f"EVENT#{outbox_event.event_id}"},
                },
                UpdateExpression="SET #status = :pending, GSI1PK = :gsi1pk",
                ExpressionAttributeNames={"#status": "status"},
                ExpressionAttributeValues={
                    ":pending": {"S": OutboxStatus.PENDING.value},
                    ":gsi1pk": {"S": f"STATUS#{OutboxStatus.PENDING.value}"},
                },
            )
            
            # Publish to SNS
            publish_to_sns(outbox_event)
            mark_event_published(outbox_event)
            
            success_count += 1
            logger.info(f"Successfully retried event {outbox_event.event_id}")
            
        except Exception as e:
            failure_count += 1
            logger.exception(f"Failed to retry event", item_pk=item.get("PK", {}).get("S"))
    
    # Record metrics
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
