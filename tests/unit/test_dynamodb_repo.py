from datetime import UTC, datetime
from unittest.mock import MagicMock

import pytest
from botocore.exceptions import ClientError

from processor.adapters.outbound.dynamodb_repo import DynamoDBMetadataRepository
from processor.domain.exceptions import StorageError
from processor.domain.models import MetadataRecord, ProcessingStatus


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


def test_get_history_is_current_state_only(
    repo: DynamoDBMetadataRepository,
    client: MagicMock,
    record: MetadataRecord,
) -> None:
    client.get_item.return_value = {"Item": record.to_dynamodb_item()}
    history = repo.get_history(record.file_id, limit=10)
    assert len(history) == 1
    assert history[0].file_id == record.file_id
    assert repo.get_history(record.file_id, limit=0) == []


def test_update_status_uses_metadata_sort_key(
    repo: DynamoDBMetadataRepository,
    client: MagicMock,
) -> None:
    repo.update_status("file-123", "ignored", "COMPLETED")
    request = client.update_item.call_args.kwargs
    assert request["Key"]["SK"] == {"S": "METADATA"}
    assert "processedAt" in request["UpdateExpression"]


def test_query_by_status_returns_metadata_entities(
    repo: DynamoDBMetadataRepository,
    client: MagicMock,
    record: MetadataRecord,
) -> None:
    item = record.to_dynamodb_item()
    client.query.return_value = {"Items": [item, {"entityType": {"S": "OUTBOX"}}]}
    records = repo.query_by_status("PROCESSING")
    assert len(records) == 1
    assert records[0].file_id == record.file_id


def test_increment_retry_count_uses_current_state_key(
    repo: DynamoDBMetadataRepository,
    client: MagicMock,
) -> None:
    client.update_item.return_value = {"Attributes": {"retryCount": {"N": "3"}}}
    assert repo.increment_retry_count("file-123", "ignored") == 3
    assert client.update_item.call_args.kwargs["Key"]["SK"] == {"S": "METADATA"}
