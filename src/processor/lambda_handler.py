# =============================================================================
# Lambda Handler - AWS Lambda Entry Point
# =============================================================================
"""
AWS Lambda handler for FSAMP Processor.

This module provides the Lambda entry point that:
1. Receives SQS events (batch of messages)
2. Processes each message using the FileProcessorService
3. Reports batch item failures for partial batch response

Uses AWS Lambda Powertools for:
- Structured logging with correlation IDs
- Distributed tracing (X-Ray)
- Metrics collection (CloudWatch EMF)
- Batch processing utilities

Implements Outbox Pattern for reliable event publishing.
"""

from __future__ import annotations

import json
import time
from typing import Any, cast

from aws_lambda_powertools import Logger, Metrics, Tracer
from aws_lambda_powertools.metrics import MetricUnit
from aws_lambda_powertools.utilities.batch import (
    BatchProcessor,
    EventType,
    batch_processor,
)
from aws_lambda_powertools.utilities.data_classes.sqs_event import SQSRecord
from aws_lambda_powertools.utilities.typing import LambdaContext

from processor.adapters.outbound import (
    DynamoDBMetadataRepository,
    DynamoDBOutboxRepository,
    KMSCryptoProvider,
    S3FileStorage,
    SNSEventPublisher,
)
from processor.application import FileProcessorService
from processor.config import Settings, get_settings
from processor.domain.events import FileEvent
from processor.domain.exceptions import NonRetryableError, ProcessingError
from processor.infrastructure import AWSClientFactory

# =============================================================================
# AWS Lambda Powertools Configuration
# =============================================================================

logger = Logger(service="fsamp-processor")
tracer = Tracer(service="fsamp-processor")
metrics = Metrics(service="fsamp-processor", namespace="FSAMP/Processor")

# Batch processor for SQS with partial batch response
processor = BatchProcessor(event_type=EventType.SQS)

# =============================================================================
# Global Application Components (reused across invocations - warm start)
# =============================================================================

_file_processor: FileProcessorService | None = None
_settings: Settings | None = None


def get_file_processor() -> FileProcessorService:
    """
    Get or create the FileProcessorService singleton.

    Uses Lambda execution context reuse for warm starts.
    Components are created once and reused across invocations.
    """
    global _file_processor, _settings

    if _file_processor is not None:
        return _file_processor

    logger.info("Initializing FileProcessorService (cold start)")

    # Load settings
    _settings = get_settings()

    # Create AWS client factory with FIPS support
    aws_factory = AWSClientFactory(
        region=_settings.aws_region,
        endpoint_url=_settings.aws_endpoint_url,
        use_fips=_settings.should_use_fips,
    )

    # Create adapters
    file_storage = S3FileStorage(
        s3_client=aws_factory.get_s3_client(),
        default_kms_key_id=_settings.kms_key_id,
    )

    metadata_repo = DynamoDBMetadataRepository(
        dynamodb_client=aws_factory.get_dynamodb_client(),
        table_name=_settings.dynamodb_table_name,
    )

    event_publisher = SNSEventPublisher(
        sns_client=aws_factory.get_sns_client(),
        topic_arn=_settings.sns_topic_arn,
    )

    crypto_provider = KMSCryptoProvider(
        kms_client=aws_factory.get_kms_client(),
        key_id=_settings.kms_key_id,
    )

    # Create Outbox Repository for transactional writes
    outbox_repo = None
    if _settings.outbox_table_name:
        outbox_repo = DynamoDBOutboxRepository(
            dynamodb_client=aws_factory.get_dynamodb_client(),
            metadata_table_name=_settings.dynamodb_table_name,
            outbox_table_name=_settings.outbox_table_name,
        )
        logger.info("Outbox Pattern enabled", outbox_table=_settings.outbox_table_name)

    # Create application service
    _file_processor = FileProcessorService(
        file_storage=file_storage,
        metadata_repo=metadata_repo,
        event_publisher=event_publisher,
        crypto_provider=crypto_provider,
        outbox_repo=outbox_repo,
        max_file_size_bytes=_settings.max_file_size_bytes,
        use_outbox_pattern=outbox_repo is not None,
    )

    logger.info(
        "FileProcessorService initialized successfully", outbox_enabled=outbox_repo is not None
    )
    return _file_processor


# =============================================================================
# Record Handler (processes single SQS message)
# =============================================================================


@tracer.capture_method
def record_handler(record: SQSRecord) -> dict[str, Any]:
    """
    Process a single SQS record.

    This function is called by the batch processor for each message.
    It parses the message, processes the file event, and returns the result.

    Args:
        record: SQS record from the batch.

    Returns:
        Processing result dictionary.

    Raises:
        Exception: If processing fails (will be reported as batch item failure).
    """
    message_id = record.message_id
    start_time = time.time()

    # Add correlation context to logger
    logger.append_keys(message_id=message_id)

    try:
        # Parse the SQS message body (could be direct or SNS-wrapped)
        body = json.loads(record.body)

        # Handle SNS notification wrapper if present
        if "Message" in body and "TopicArn" in body:
            logger.debug("Unwrapping SNS notification")
            event_data = json.loads(body["Message"])
        else:
            event_data = body

        # Parse FileEvent from message
        file_event = FileEvent.model_validate(event_data)

        # Add event context to logger and tracer
        logger.append_keys(
            event_id=str(file_event.event_id),
            correlation_id=file_event.correlation_id,
            event_type=file_event.event_type.value,
        )
        tracer.put_annotation("event_id", str(file_event.event_id))
        tracer.put_annotation("event_type", file_event.event_type.value)
        tracer.put_annotation("correlation_id", str(file_event.correlation_id))

        # Record file size metric
        file_size_bytes = file_event.file_metadata.file_size_bytes
        metrics.add_metric(name="FileSizeBytes", unit=MetricUnit.Bytes, value=file_size_bytes)

        logger.info(
            "Processing file event",
            original_filename=file_event.file_metadata.original_filename,
            file_size_bytes=file_size_bytes,
        )

        # Get processor and handle event
        file_processor = get_file_processor()
        result = file_processor.handle(file_event)

        # Calculate total processing time
        total_duration_ms = int((time.time() - start_time) * 1000)

        # Record detailed success metrics
        metrics.add_metric(name="FilesProcessed", unit=MetricUnit.Count, value=1)
        metrics.add_metric(name="FilesProcessedSuccess", unit=MetricUnit.Count, value=1)
        metrics.add_metric(
            name="ProcessingDuration",
            unit=MetricUnit.Milliseconds,
            value=result.duration_ms or total_duration_ms,
        )

        # Record metrics by file type
        mime_type = file_event.file_metadata.mime_type or "unknown"
        metrics.add_dimension(
            name="MimeType", value=mime_type.split("/")[0] if "/" in mime_type else mime_type
        )
        metrics.add_metric(name="FilesByType", unit=MetricUnit.Count, value=1)

        # Record if file was marked safe or not
        if result.metadata.get("is_safe") is True:
            metrics.add_metric(name="SafeFiles", unit=MetricUnit.Count, value=1)
        elif result.metadata.get("is_safe") is False:
            metrics.add_metric(name="UnsafeFiles", unit=MetricUnit.Count, value=1)

        logger.info(
            "File processed successfully",
            status=result.status.value,
            duration_ms=result.duration_ms,
            is_safe=result.metadata.get("is_safe"),
            outbox_event_id=result.metadata.get("outbox_event_id"),
        )

        return {
            "messageId": message_id,
            "status": "success",
            "eventId": str(file_event.event_id),
            "processingStatus": result.status.value,
            "durationMs": total_duration_ms,
        }

    except NonRetryableError as e:
        # Non-retryable errors - don't retry, log and continue
        logger.warning("Non-retryable error", error=str(e))
        metrics.add_metric(name="FilesProcessed", unit=MetricUnit.Count, value=1)
        metrics.add_metric(name="FilesProcessedFailed", unit=MetricUnit.Count, value=1)
        metrics.add_metric(name="NonRetryableErrors", unit=MetricUnit.Count, value=1)

        # Record processing duration even for failures
        total_duration_ms = int((time.time() - start_time) * 1000)
        metrics.add_metric(
            name="FailedProcessingDuration", unit=MetricUnit.Milliseconds, value=total_duration_ms
        )

        # Return success to prevent retry (error is logged, event is dead-lettered)
        return {
            "messageId": message_id,
            "status": "skipped",
            "error": str(e),
            "retryable": False,
        }

    except ProcessingError as e:
        # Retryable errors - raise to trigger retry via batch item failure
        logger.error("Processing error (retryable)", error=str(e), retryable=e.retryable)
        metrics.add_metric(name="FilesProcessed", unit=MetricUnit.Count, value=1)
        metrics.add_metric(name="FilesProcessedFailed", unit=MetricUnit.Count, value=1)
        metrics.add_metric(name="RetryableErrors", unit=MetricUnit.Count, value=1)
        raise

    except Exception:
        # Unexpected errors - raise for retry
        logger.exception("Unexpected error processing message")
        metrics.add_metric(name="FilesProcessed", unit=MetricUnit.Count, value=1)
        metrics.add_metric(name="FilesProcessedFailed", unit=MetricUnit.Count, value=1)
        metrics.add_metric(name="UnexpectedErrors", unit=MetricUnit.Count, value=1)
        raise


# =============================================================================
# Lambda Handler (main entry point)
# =============================================================================


@logger.inject_lambda_context(log_event=True)
@tracer.capture_lambda_handler
@metrics.log_metrics(capture_cold_start_metric=True)
@batch_processor(record_handler=record_handler, processor=processor)  # type: ignore[untyped-decorator]
def lambda_handler(event: dict[str, Any], context: LambdaContext) -> dict[str, Any]:
    """
    AWS Lambda handler for SQS-triggered file processing.

    This handler:
    1. Receives a batch of SQS messages
    2. Processes each message using record_handler
    3. Returns batch item failures for partial batch response

    The @batch_processor decorator handles:
    - Iterating through SQS records
    - Error handling per record
    - Building partial batch response

    Args:
        event: Lambda event (SQS batch)
        context: Lambda context

    Returns:
        Response with batchItemFailures for failed messages
    """
    # Log batch info
    records = event.get("Records", [])
    logger.info(
        "Processing SQS batch",
        batch_size=len(records),
        function_name=context.function_name,
        remaining_time_ms=context.get_remaining_time_in_millis(),
    )

    # Record batch metrics
    metrics.add_metric(name="BatchSize", unit=MetricUnit.Count, value=len(records))

    # The @batch_processor decorator handles processing and returns
    # {"batchItemFailures": [...]} for partial batch response
    return cast(dict[str, Any], processor.response())


# =============================================================================
# Local Testing Support
# =============================================================================

if __name__ == "__main__":
    # For local testing with sample event
    import sys

    sample_event = {
        "Records": [
            {
                "messageId": "test-message-id",
                "receiptHandle": "test-receipt",
                "body": json.dumps(
                    {
                        "event_id": "test-event-123",
                        "event_type": "FILE_UPLOADED",
                        "correlation_id": "test-correlation",
                        "timestamp": "2024-01-01T00:00:00Z",
                        "source": "test",
                        "schema_version": "1.0.0",
                        "file_metadata": {
                            "file_id": "test-file-123",
                            "original_filename": "test.pdf",
                            "content_type": "application/pdf",
                            "file_size_bytes": 1024,
                            "checksum_sha256": "abc123",
                        },
                        "storage_location": {
                            "bucket_name": "test-bucket",
                            "object_key": "uploads/test.pdf",
                            "region": "us-west-2",
                        },
                        "security_context": {
                            "encryption_algorithm": "AES-256-GCM",
                            "kms_key_id": "alias/test-key",
                        },
                    }
                ),
                "attributes": {},
                "messageAttributes": {},
                "md5OfBody": "test",
                "eventSource": "aws:sqs",
                "eventSourceARN": "arn:aws:sqs:us-west-2:123456789:test-queue",
                "awsRegion": "us-west-2",
            }
        ]
    }

    # Mock context
    class MockContext:
        function_name = "test-function"
        memory_limit_in_mb = 512
        invoked_function_arn = "arn:aws:lambda:us-west-2:123456789:function:test"
        aws_request_id = "test-request-id"

        def get_remaining_time_in_millis(self) -> int:
            return 300000

    print("Testing Lambda handler locally...")
    result = lambda_handler(sample_event, MockContext())
    print(f"Result: {json.dumps(result, indent=2)}")
    sys.exit(0)
