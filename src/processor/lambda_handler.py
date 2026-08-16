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
from aws_lambda_powertools.metrics import MetricUnit, single_metric
from aws_lambda_powertools.utilities.batch import (
    BatchProcessor,
    EventType,
    batch_processor,
)
from aws_lambda_powertools.utilities.data_classes.sqs_event import SQSRecord
from aws_lambda_powertools.utilities.typing import LambdaContext
from pydantic import ValidationError

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
from processor.domain.exceptions import (
    EventValidationError,
    NonRetryableError,
    ProcessingError,
)
from processor.infrastructure import AWSClientFactory, enforce_fips

logger = Logger(service="fsamp-processor")
tracer = Tracer(service="fsamp-processor")
metrics = Metrics(service="fsamp-processor", namespace="FSAMP/Processor")

processor = BatchProcessor(event_type=EventType.SQS)
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

    _settings = get_settings()

    _settings.validate_processor_runtime()

    enforce_fips(_settings.should_require_fips)

    aws_factory = AWSClientFactory(
        region=_settings.aws_region,
        endpoint_url=_settings.aws_endpoint_url,
        use_fips=_settings.should_use_fips,
    )

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

    outbox_repo = None
    if _settings.outbox_table_name:
        outbox_repo = DynamoDBOutboxRepository(
            dynamodb_client=aws_factory.get_dynamodb_client(),
            metadata_table_name=_settings.dynamodb_table_name,
            outbox_table_name=_settings.outbox_table_name,
            retention_seconds=_settings.outbox_retention_seconds,
        )
        logger.info("Outbox Pattern enabled", outbox_table=_settings.outbox_table_name)

    _file_processor = FileProcessorService(
        file_storage=file_storage,
        metadata_repo=metadata_repo,
        event_publisher=event_publisher,
        crypto_provider=crypto_provider,
        outbox_repo=outbox_repo,
        max_file_size_bytes=_settings.max_file_size_bytes,
        use_outbox_pattern=outbox_repo is not None,
        allowed_bucket_name=_settings.s3_bucket_name,
        allowed_region=_settings.aws_region,
        quarantine_prefix=_settings.quarantine_prefix,
        processing_claim_ttl_seconds=_settings.processing_claim_ttl_seconds,
    )

    logger.info(
        "FileProcessorService initialized successfully", outbox_enabled=outbox_repo is not None
    )
    return _file_processor


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

    logger.append_keys(message_id=message_id)

    try:
        body = json.loads(record.body)

        if "Message" in body and "TopicArn" in body:
            logger.debug("Unwrapping SNS notification")
            event_data = json.loads(body["Message"])
        else:
            event_data = body

        file_event = FileEvent.model_validate(event_data)

        logger.append_keys(
            event_id=str(file_event.event_id),
            file_id=file_event.file_id_str,
            correlation_id=file_event.correlation_id_str,
            event_type=file_event.event_type.value,
        )
        tracer.put_annotation("event_id", str(file_event.event_id))
        tracer.put_annotation("file_id", file_event.file_id_str)
        tracer.put_annotation("event_type", file_event.event_type.value)
        tracer.put_annotation("correlation_id", str(file_event.correlation_id))

        file_size_bytes = file_event.file_metadata.file_size_bytes
        metrics.add_metric(name="FileSizeBytes", unit=MetricUnit.Bytes, value=file_size_bytes)

        logger.info(
            "Processing file event",
            redacted_filename=file_event.file_metadata.redacted_filename,
            file_size_bytes=file_size_bytes,
        )

        file_processor = get_file_processor()
        result = file_processor.handle(file_event)

        total_duration_ms = int(1000 * (time.time() - start_time))

        metrics.add_metric(name="FilesProcessed", unit=MetricUnit.Count, value=1)
        metrics.add_metric(name="FilesProcessedSuccess", unit=MetricUnit.Count, value=1)
        metrics.add_metric(
            name="ProcessingDuration",
            unit=MetricUnit.Milliseconds,
            value=result.duration_ms or total_duration_ms,
        )

        mime_type = file_event.file_metadata.mime_type or "unknown"
        mime_family = mime_type.split("/")[0] if "/" in mime_type else mime_type
        with single_metric(
            name="FilesByType",
            unit=MetricUnit.Count,
            value=1,
            namespace="FSAMP/Processor",
        ) as metric:
            metric.add_dimension(name="MimeType", value=mime_family)

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
            "fileId": file_event.file_id_str,
            "eventId": str(file_event.event_id),
            "processingStatus": result.status.value,
            "durationMs": total_duration_ms,
        }

    except NonRetryableError as e:
        logger.warning("Non-retryable error", error=str(e))
        metrics.add_metric(name="FilesProcessed", unit=MetricUnit.Count, value=1)
        metrics.add_metric(name="FilesProcessedFailed", unit=MetricUnit.Count, value=1)
        metrics.add_metric(name="NonRetryableErrors", unit=MetricUnit.Count, value=1)

        total_duration_ms = int(1000 * (time.time() - start_time))
        metrics.add_metric(
            name="FailedProcessingDuration", unit=MetricUnit.Milliseconds, value=total_duration_ms
        )

        raise

    except (json.JSONDecodeError, ValidationError) as e:
        metrics.add_metric(name="InvalidEvents", unit=MetricUnit.Count, value=1)
        raise EventValidationError(
            message="SQS payload is not a valid canonical FSAMP event",
            event_id=message_id,
            cause=e,
        ) from e

    except ProcessingError as e:
        logger.error("Processing error (retryable)", error=str(e), retryable=e.retryable)
        metrics.add_metric(name="FilesProcessed", unit=MetricUnit.Count, value=1)
        metrics.add_metric(name="FilesProcessedFailed", unit=MetricUnit.Count, value=1)
        metrics.add_metric(name="RetryableErrors", unit=MetricUnit.Count, value=1)
        raise

    except Exception:
        logger.exception("Unexpected error processing message")
        metrics.add_metric(name="FilesProcessed", unit=MetricUnit.Count, value=1)
        metrics.add_metric(name="FilesProcessedFailed", unit=MetricUnit.Count, value=1)
        metrics.add_metric(name="UnexpectedErrors", unit=MetricUnit.Count, value=1)
        raise

    finally:
        logger.remove_keys(["message_id", "event_id", "file_id", "correlation_id", "event_type"])


@logger.inject_lambda_context(log_event=False)
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
    records = event.get("Records", [])
    logger.info(
        "Processing SQS batch",
        batch_size=len(records),
        function_name=context.function_name,
        remaining_time_ms=context.get_remaining_time_in_millis(),
    )

    metrics.add_metric(name="BatchSize", unit=MetricUnit.Count, value=len(records))

    return cast(dict[str, Any], processor.response())
