#!/usr/bin/env python3
# =============================================================================
# FSAMP Processor - Main Entry Point
# =============================================================================
"""
Application entry point with dependency injection and graceful shutdown.
"""

import sys

import structlog

from processor.adapters.inbound import SQSConsumer
from processor.adapters.outbound import (
    DynamoDBMetadataRepository,
    KMSCryptoProvider,
    S3FileStorage,
    SNSEventPublisher,
)
from processor.application import FileProcessorService
from processor.config import Settings, get_settings
from processor.infrastructure import AWSClientFactory, configure_logging, enforce_fips

logger = structlog.get_logger(__name__)


def create_application(settings: Settings) -> SQSConsumer:
    """
    Create and wire up all application components (Composition Root).

    This is where Dependency Injection happens - we create all adapters
    and wire them to the application service.

    Args:
        settings: Application settings.

    Returns:
        Configured SQS consumer ready to start.
    """
    logger.info("Creating application components")

    # Create AWS client factory
    aws_factory = AWSClientFactory(
        region=settings.aws_region,
        endpoint_url=settings.aws_endpoint_url,
        use_fips=settings.should_use_fips,
    )

    # Verify AWS connectivity
    connectivity = aws_factory.verify_connectivity()
    if not all(connectivity.values()):
        failed = [k for k, v in connectivity.items() if not v]
        logger.error("AWS connectivity check failed", failed_services=failed)
        raise RuntimeError(f"Failed to connect to AWS services: {failed}")

    # Create outbound adapters (driven)
    file_storage = S3FileStorage(
        s3_client=aws_factory.get_s3_client(),
        default_kms_key_id=settings.kms_key_id,
    )

    metadata_repo = DynamoDBMetadataRepository(
        dynamodb_client=aws_factory.get_dynamodb_client(),
        table_name=settings.dynamodb_table_name,
    )

    event_publisher = SNSEventPublisher(
        sns_client=aws_factory.get_sns_client(),
        topic_arn=settings.sns_topic_arn,
    )

    crypto_provider = KMSCryptoProvider(
        kms_client=aws_factory.get_kms_client(),
        key_id=settings.kms_key_id,
    )

    # Verify KMS key access
    if not crypto_provider.verify_key_access():
        raise RuntimeError(f"Cannot access KMS key: {settings.kms_key_id}")

    # Create application service
    processor_service = FileProcessorService(
        file_storage=file_storage,
        metadata_repo=metadata_repo,
        event_publisher=event_publisher,
        crypto_provider=crypto_provider,
        max_file_size_bytes=settings.max_file_size_bytes,
    )

    # Create inbound adapter (driving)
    consumer = SQSConsumer(
        sqs_client=aws_factory.get_sqs_client(),
        queue_url=settings.sqs_queue_url,
        handler=processor_service.handle,
        max_messages=settings.sqs_max_messages,
        wait_time_seconds=settings.sqs_wait_time_seconds,
        visibility_timeout=settings.sqs_visibility_timeout,
    )

    logger.info("Application components created successfully")
    return consumer


def main() -> int:
    """
    Main entry point.

    Returns:
        Exit code (0 for success, 1 for error).
    """
    try:
        # Load settings
        settings = get_settings()

        # Configure logging
        configure_logging(
            level=settings.log_level,
            json_format=settings.use_json_logging,
            service_name="fsamp-processor",
        )

        logger.info(
            "Starting FSAMP Processor",
            environment=settings.environment,
            region=settings.aws_region,
            is_local=settings.is_local,
        )

        # Enforce FIPS mode when required
        enforce_fips(settings.should_require_fips)

        # Create application
        consumer = create_application(settings)

        # Start consumer (blocking)
        logger.info("Starting SQS consumer...")
        consumer.start_blocking()

        logger.info("FSAMP Processor stopped gracefully")
        return 0

    except KeyboardInterrupt:
        logger.info("Received interrupt signal, shutting down...")
        return 0

    except Exception as e:
        logger.exception("Fatal error", error=str(e))
        return 1


if __name__ == "__main__":
    sys.exit(main())
