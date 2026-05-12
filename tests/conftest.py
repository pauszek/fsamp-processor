# =============================================================================
# Pytest Configuration & Fixtures
# =============================================================================
"""
Shared fixtures for all tests.
Schema v1.1.0 compliant - FIPS 140-3.
"""

import os
from collections.abc import Generator
from datetime import UTC, datetime
from uuid import UUID, uuid4

import boto3
import pytest
from moto import mock_aws

from processor.domain.events import (
    SCHEMA_VERSION,
    EventSource,
    EventType,
    FileEvent,
    FileMetadata,
    SecurityContext,
    StorageLocation,
)

# =============================================================================
# Test Constants - Schema v1.1.0
# =============================================================================

SAMPLE_CHECKSUM_SHA256 = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
SAMPLE_KMS_ARN = "arn:aws:kms:us-west-2:123456789012:key/12345678-1234-1234-1234-123456789012"


# =============================================================================
# Environment Setup
# =============================================================================


@pytest.fixture(autouse=True)
def aws_credentials() -> None:
    """Mock AWS credentials for moto."""
    os.environ["AWS_ACCESS_KEY_ID"] = "testing"
    os.environ["AWS_SECRET_ACCESS_KEY"] = "testing"
    os.environ["AWS_SECURITY_TOKEN"] = "testing"
    os.environ["AWS_SESSION_TOKEN"] = "testing"
    os.environ["AWS_DEFAULT_REGION"] = "us-west-2"


# =============================================================================
# AWS Mock Fixtures
# =============================================================================


@pytest.fixture
def mock_aws_services() -> Generator[None]:
    """Start all AWS mocks."""
    with mock_aws():
        yield


@pytest.fixture
def s3_client(mock_aws_services: None) -> boto3.client:
    """Create mock S3 client."""
    client = boto3.client("s3", region_name="us-west-2")
    # Create test bucket (idempotent - ignore if already exists)
    try:
        client.create_bucket(
            Bucket="test-bucket",
            CreateBucketConfiguration={"LocationConstraint": "us-west-2"},
        )
    except client.exceptions.BucketAlreadyOwnedByYou:
        pass  # Bucket exists from previous test in same mock session
    return client


@pytest.fixture
def sqs_client(mock_aws_services: None) -> boto3.client:
    """Create mock SQS client."""
    client = boto3.client("sqs", region_name="us-west-2")
    return client


@pytest.fixture
def sns_client(mock_aws_services: None) -> boto3.client:
    """Create mock SNS client."""
    client = boto3.client("sns", region_name="us-west-2")
    return client


@pytest.fixture
def dynamodb_client(mock_aws_services: None) -> boto3.client:
    """Create mock DynamoDB client."""
    client = boto3.client("dynamodb", region_name="us-west-2")
    return client


@pytest.fixture
def kms_client(mock_aws_services: None) -> boto3.client:
    """Create mock KMS client."""
    client = boto3.client("kms", region_name="us-west-2")
    return client


# =============================================================================
# Resource Fixtures
# =============================================================================


@pytest.fixture
def test_bucket(s3_client: boto3.client) -> str:
    """Return the test bucket name."""
    return "test-bucket"


@pytest.fixture
def test_queue_url(sqs_client: boto3.client) -> str:
    """Create a test SQS queue and return its URL."""
    response = sqs_client.create_queue(
        QueueName="test-queue",
        Attributes={
            "VisibilityTimeout": "300",
            "MessageRetentionPeriod": "86400",
        },
    )
    return response["QueueUrl"]


@pytest.fixture
def test_topic_arn(sns_client: boto3.client) -> str:
    """Create a test SNS topic and return its ARN."""
    response = sns_client.create_topic(Name="test-topic")
    return response["TopicArn"]


@pytest.fixture
def test_table_name(dynamodb_client: boto3.client) -> str:
    """Create a test DynamoDB table and return its name."""
    table_name = "test-metadata"

    dynamodb_client.create_table(
        TableName=table_name,
        KeySchema=[
            {"AttributeName": "PK", "KeyType": "HASH"},
            {"AttributeName": "SK", "KeyType": "RANGE"},
        ],
        AttributeDefinitions=[
            {"AttributeName": "PK", "AttributeType": "S"},
            {"AttributeName": "SK", "AttributeType": "S"},
            {"AttributeName": "GSI1PK", "AttributeType": "S"},
            {"AttributeName": "GSI1SK", "AttributeType": "S"},
        ],
        GlobalSecondaryIndexes=[
            {
                "IndexName": "GSI1",
                "KeySchema": [
                    {"AttributeName": "GSI1PK", "KeyType": "HASH"},
                    {"AttributeName": "GSI1SK", "KeyType": "RANGE"},
                ],
                "Projection": {"ProjectionType": "ALL"},
            },
        ],
        BillingMode="PAY_PER_REQUEST",
    )

    return table_name


@pytest.fixture
def test_kms_key_id(kms_client: boto3.client) -> str:
    """Create a test KMS key and return its ID."""
    response = kms_client.create_key(
        Description="Test key for FSAMP Processor",
        KeyUsage="ENCRYPT_DECRYPT",
    )
    return response["KeyMetadata"]["KeyId"]


# =============================================================================
# Domain Object Fixtures
# =============================================================================


@pytest.fixture
def sample_event_id() -> UUID:
    """Generate a sample event ID (UUID)."""
    return uuid4()


@pytest.fixture
def sample_correlation_id() -> UUID:
    """Generate a sample correlation ID (UUID per schema v1.1.0)."""
    return uuid4()


@pytest.fixture
def sample_file_metadata() -> FileMetadata:
    """Create sample file metadata with SHA-256 checksum (FIPS 180-4)."""
    return FileMetadata(
        original_filename="test-document.pdf",
        file_size_bytes=1024,
        mime_type="application/pdf",
        checksum_sha256=SAMPLE_CHECKSUM_SHA256,
    )


@pytest.fixture
def sample_storage_location(test_bucket: str) -> StorageLocation:
    """Create sample storage location."""
    return StorageLocation(
        bucket_name=test_bucket,
        object_key="uploads/2024/01/test-document.pdf",
    )


@pytest.fixture
def sample_security_context() -> SecurityContext:
    """Create sample security context with valid KMS ARN (schema v1.1.0)."""
    return SecurityContext(
        is_encrypted=True,
        encryption_algorithm="AES/GCM/NoPadding",
        kms_key_id=SAMPLE_KMS_ARN,
    )


@pytest.fixture
def sample_file_event(
    sample_event_id: UUID,
    sample_correlation_id: UUID,
    sample_file_metadata: FileMetadata,
    sample_storage_location: StorageLocation,
    sample_security_context: SecurityContext,
) -> FileEvent:
    """Create a sample file event (schema v1.1.0)."""
    return FileEvent(
        schema_version=SCHEMA_VERSION,
        file_id=sample_event_id,
        event_id=sample_event_id,
        correlation_id=sample_correlation_id,
        timestamp=datetime.now(UTC),
        source=EventSource.PROCESSOR,
        event_type=EventType.FILE_UPLOADED,
        file_metadata=sample_file_metadata,
        storage_location=sample_storage_location,
        security_context=sample_security_context,
    )


@pytest.fixture
def sample_file_content() -> bytes:
    """Create sample file content."""
    return b"%PDF-1.4 Sample PDF content for testing purposes." + b"\x00" * 100
