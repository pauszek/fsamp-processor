from datetime import UTC, datetime
from unittest.mock import MagicMock

import pytest
from botocore.exceptions import ClientError

from processor.adapters.outbound.dynamodb_repo import DynamoDBMetadataRepository
from processor.domain.exceptions import StorageError
from processor.domain.models import MetadataRecord, ProcessingClaim, ProcessingStatus


@pytest.fixture
def record() -> MetadataRecord:
    return MetadataRecord(
        file_id="file-123",
        timestamp=datetime.now(UTC).isoformat(),
        correlation_id="corr-456",
        original_filename="document.pdf",
        file_size_bytes=42,
        mime_type="application/pdf",
        bucket_name="files-bucket",
        object_key="uploads/document.pdf",
        status=ProcessingStatus.PROCESSING,
        checksum_sha256="a" * 64,
        kms_key_id="arn:aws:kms:us-west-2:123456789012:key/key-id",
    )


@pytest.fixture
def client() -> MagicMock:
    return MagicMock()


@pytest.fixture
def repo(client: MagicMock) -> DynamoDBMetadataRepository:
    return DynamoDBMetadataRepository(client, "metadata-table")


def test_claim_processing_is_atomic_and_returns_fence(
    repo: DynamoDBMetadataRepository,
    client: MagicMock,
    record: MetadataRecord,
) -> None:
    client.update_item.return_value = {
        "Attributes": {
            "processorClaimVersion": {"N": "7"},
            "processorClaimExpiresAt": {"N": "1234567890"},
        }
    }

    claim = repo.claim_processing(record, "event-456", lease_seconds=330)

    assert claim is not None
    assert claim.event_id == "event-456"
    assert claim.version == 7
    request = client.update_item.call_args.kwargs
    assert request["Key"] == {
        "PK": {"S": "FILE#file-123"},
        "SK": {"S": "METADATA"},
    }
    assert "attribute_exists(PK)" not in request["ConditionExpression"]
    assert "if_not_exists(#initial" in request["UpdateExpression"]
    assert "originalFilename" in request["ExpressionAttributeNames"].values()
    assert "#claimExpiresAt <= :nowEpoch" in request["ConditionExpression"]
    assert "#lastProcessedEventId <> :eventId" in request["ConditionExpression"]
    assert request["ExpressionAttributeNames"]["#claimExpiresAt"] == ("processorClaimExpiresAt")
    assert request["ReturnValues"] == "ALL_NEW"


def test_claim_processing_reports_contention_without_overwriting(
    repo: DynamoDBMetadataRepository,
    client: MagicMock,
    record: MetadataRecord,
) -> None:
    client.update_item.side_effect = ClientError(
        {"Error": {"Code": "ConditionalCheckFailedException", "Message": "busy"}},
        "UpdateItem",
    )

    assert repo.claim_processing(record, "event-456", lease_seconds=330) is None


def test_claimed_save_requires_matching_token_and_version(
    repo: DynamoDBMetadataRepository,
    client: MagicMock,
    record: MetadataRecord,
) -> None:
    claim = ProcessingClaim(
        event_id="event-456",
        token="claim-token",
        version=7,
        expires_at_epoch=1234567890,
    )

    repo.save(record, claim=claim)

    request = client.update_item.call_args.kwargs
    assert "#claimToken = :claimToken" in request["ConditionExpression"]
    assert "#claimVersion = :claimVersion" in request["ConditionExpression"]
    assert "REMOVE #claimToken" in request["UpdateExpression"]
    assert request["ExpressionAttributeNames"]["#claimToken"] == "processorClaimToken"


def test_save_updates_shared_current_state(
    repo: DynamoDBMetadataRepository,
    client: MagicMock,
    record: MetadataRecord,
) -> None:
    repo.save(record)

    request = client.update_item.call_args.kwargs
    assert request["Key"] == {
        "PK": {"S": "FILE#file-123"},
        "SK": {"S": "METADATA"},
    }
    assert "if_not_exists(#createdAt" in request["UpdateExpression"]
    assert "originalFilename" in request["ExpressionAttributeNames"].values()
    assert record.updated_at is not None


def test_save_preserves_gateway_created_by(
    repo: DynamoDBMetadataRepository,
    client: MagicMock,
    record: MetadataRecord,
) -> None:
    record.created_by = "subject-123"
    repo.save(record)
    request = client.update_item.call_args.kwargs
    assert "#createdBy = if_not_exists(#createdBy, :createdBy)" in request["UpdateExpression"]


def test_save_wraps_non_retryable_client_error(
    repo: DynamoDBMetadataRepository,
    client: MagicMock,
    record: MetadataRecord,
) -> None:
    client.update_item.side_effect = ClientError(
        {"Error": {"Code": "ValidationException", "Message": "bad"}},
        "UpdateItem",
    )
    with pytest.raises(StorageError):
        repo.save(record)
    assert client.update_item.call_count == 1


def test_get_by_id_reads_fixed_key_consistently(
    repo: DynamoDBMetadataRepository,
    client: MagicMock,
    record: MetadataRecord,
) -> None:
    client.get_item.return_value = {"Item": record.to_dynamodb_item()}
    restored = repo.get_by_id(record.file_id)
    assert restored is not None
    assert restored.original_filename == record.original_filename
    assert client.get_item.call_args.kwargs["ConsistentRead"] is True
    assert client.get_item.call_args.kwargs["Key"]["SK"] == {"S": "METADATA"}


def test_get_by_id_missing(repo: DynamoDBMetadataRepository, client: MagicMock) -> None:
    client.get_item.return_value = {}
    assert repo.get_by_id("missing") is None
