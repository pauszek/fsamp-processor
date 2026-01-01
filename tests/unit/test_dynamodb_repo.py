# =============================================================================
# Unit Tests for DynamoDB Metadata Repository Adapter
# =============================================================================
"""Tests for DynamoDB Metadata Repository adapter."""

from datetime import datetime
from unittest.mock import MagicMock

import pytest
from botocore.exceptions import ClientError

from processor.adapters.outbound.dynamodb_repo import DynamoDBMetadataRepository
from processor.domain.exceptions import StorageError
from processor.domain.models import MetadataRecord, ProcessingStatus


class TestDynamoDBMetadataRepositoryInit:
    """Tests for DynamoDBMetadataRepository initialization."""

    def test_init(self) -> None:
        """Test initialization."""
        client = MagicMock()
        repo = DynamoDBMetadataRepository(
            dynamodb_client=client,
            table_name="test-table",
        )

        assert repo._client is client
        assert repo._table_name == "test-table"


class TestDynamoDBMetadataRepositorySave:
    """Tests for save method."""

    @pytest.fixture
    def repo(self) -> DynamoDBMetadataRepository:
        """Create repository for testing."""
        client = MagicMock()
        return DynamoDBMetadataRepository(
            dynamodb_client=client,
            table_name="test-table",
        )

    @pytest.fixture
    def metadata_record(self) -> MetadataRecord:
        """Create sample metadata record."""
        return MetadataRecord(
            file_id="file-123",
            timestamp=datetime.utcnow().isoformat(),
            correlation_id="corr-456",
            original_filename="test.pdf",
            file_size_bytes=1024,
            mime_type="application/pdf",
            bucket_name="test-bucket",
            object_key="test-key",
            status=ProcessingStatus.PENDING,
        )

    def test_save_success(
        self,
        repo: DynamoDBMetadataRepository,
        metadata_record: MetadataRecord,
    ) -> None:
        """Test successful save."""
        repo.save(metadata_record)

        repo._client.put_item.assert_called_once()
        call_kwargs = repo._client.put_item.call_args.kwargs
        assert call_kwargs["TableName"] == "test-table"
        assert "Item" in call_kwargs

    def test_save_sets_timestamps(
        self,
        repo: DynamoDBMetadataRepository,
        metadata_record: MetadataRecord,
    ) -> None:
        """Test that save sets updated_at and created_at."""
        metadata_record.created_at = None

        repo.save(metadata_record)

        assert metadata_record.updated_at is not None
        assert metadata_record.created_at is not None

    def test_save_error(
        self,
        repo: DynamoDBMetadataRepository,
        metadata_record: MetadataRecord,
    ) -> None:
        """Test save with error."""
        repo._client.put_item.side_effect = ClientError(
            {"Error": {"Code": "InternalServerError"}},
            "PutItem",
        )

        with pytest.raises(StorageError):
            repo.save(metadata_record)


class TestDynamoDBMetadataRepositoryGetById:
    """Tests for get_by_id method."""

    @pytest.fixture
    def repo(self) -> DynamoDBMetadataRepository:
        """Create repository for testing."""
        client = MagicMock()
        return DynamoDBMetadataRepository(
            dynamodb_client=client,
            table_name="test-table",
        )

    def test_get_by_id_success(
        self, repo: DynamoDBMetadataRepository
    ) -> None:
        """Test successful get by ID."""
        timestamp = datetime.utcnow().isoformat()
        repo._client.query.return_value = {
            "Items": [
                {
                    "PK": {"S": "FILE#file-123"},
                    "SK": {"S": f"TS#{timestamp}"},
                    "fileId": {"S": "file-123"},
                    "timestamp": {"S": timestamp},
                    "correlationId": {"S": "corr-456"},
                    "originalFilename": {"S": "test.pdf"},
                    "fileSizeBytes": {"N": "1024"},
                    "mimeType": {"S": "application/pdf"},
                    "bucketName": {"S": "test-bucket"},
                    "objectKey": {"S": "test-key"},
                    "status": {"S": "PENDING"},
                }
            ]
        }

        record = repo.get_by_id("file-123")

        assert record is not None
        assert record.file_id == "file-123"

    def test_get_by_id_not_found(
        self, repo: DynamoDBMetadataRepository
    ) -> None:
        """Test get by ID when not found."""
        repo._client.query.return_value = {"Items": []}

        record = repo.get_by_id("missing-file")

        assert record is None

    def test_get_by_id_error(
        self, repo: DynamoDBMetadataRepository
    ) -> None:
        """Test get by ID with error."""
        repo._client.query.side_effect = ClientError(
            {"Error": {"Code": "InternalServerError"}},
            "Query",
        )

        with pytest.raises(StorageError):
            repo.get_by_id("file-123")


class TestDynamoDBMetadataRepositoryGetHistory:
    """Tests for get_history method."""

    @pytest.fixture
    def repo(self) -> DynamoDBMetadataRepository:
        """Create repository for testing."""
        client = MagicMock()
        return DynamoDBMetadataRepository(
            dynamodb_client=client,
            table_name="test-table",
        )

    def test_get_history_success(
        self, repo: DynamoDBMetadataRepository
    ) -> None:
        """Test successful get history."""
        timestamp = datetime.utcnow().isoformat()
        repo._client.query.return_value = {
            "Items": [
                {
                    "PK": {"S": "FILE#file-123"},
                    "SK": {"S": f"TS#{timestamp}"},
                    "fileId": {"S": "file-123"},
                    "timestamp": {"S": timestamp},
                    "correlationId": {"S": "corr-456"},
                    "originalFilename": {"S": "test.pdf"},
                    "fileSizeBytes": {"N": "1024"},
                    "mimeType": {"S": "application/pdf"},
                    "bucketName": {"S": "test-bucket"},
                    "objectKey": {"S": "test-key"},
                    "status": {"S": "PENDING"},
                },
                {
                    "PK": {"S": "FILE#file-123"},
                    "SK": {"S": f"TS#{timestamp}"},
                    "fileId": {"S": "file-123"},
                    "timestamp": {"S": timestamp},
                    "correlationId": {"S": "corr-456"},
                    "originalFilename": {"S": "test.pdf"},
                    "fileSizeBytes": {"N": "1024"},
                    "mimeType": {"S": "application/pdf"},
                    "bucketName": {"S": "test-bucket"},
                    "objectKey": {"S": "test-key"},
                    "status": {"S": "COMPLETED"},
                },
            ]
        }

        records = repo.get_history("file-123", limit=10)

        assert len(records) == 2

    def test_get_history_empty(
        self, repo: DynamoDBMetadataRepository
    ) -> None:
        """Test get history when empty."""
        repo._client.query.return_value = {"Items": []}

        records = repo.get_history("file-123")

        assert records == []

    def test_get_history_error(
        self, repo: DynamoDBMetadataRepository
    ) -> None:
        """Test get history with error."""
        repo._client.query.side_effect = ClientError(
            {"Error": {"Code": "InternalServerError"}},
            "Query",
        )

        with pytest.raises(StorageError):
            repo.get_history("file-123")


class TestDynamoDBMetadataRepositoryUpdateStatus:
    """Tests for update_status method."""

    @pytest.fixture
    def repo(self) -> DynamoDBMetadataRepository:
        """Create repository for testing."""
        client = MagicMock()
        return DynamoDBMetadataRepository(
            dynamodb_client=client,
            table_name="test-table",
        )

    def test_update_status_success(
        self, repo: DynamoDBMetadataRepository
    ) -> None:
        """Test successful status update."""
        repo.update_status("file-123", "2024-01-01T00:00:00", "COMPLETED")

        repo._client.update_item.assert_called_once()

    def test_update_status_with_error_message(
        self, repo: DynamoDBMetadataRepository
    ) -> None:
        """Test status update with error message."""
        repo.update_status(
            "file-123",
            "2024-01-01T00:00:00",
            "FAILED",
            error_message="Processing failed",
        )

        call_kwargs = repo._client.update_item.call_args.kwargs
        assert ":error" in call_kwargs["ExpressionAttributeValues"]

    def test_update_status_completed_sets_processed_at(
        self, repo: DynamoDBMetadataRepository
    ) -> None:
        """Test that COMPLETED status sets processedAt."""
        repo.update_status("file-123", "2024-01-01T00:00:00", "COMPLETED")

        call_kwargs = repo._client.update_item.call_args.kwargs
        assert ":processed" in call_kwargs["ExpressionAttributeValues"]

    def test_update_status_error(
        self, repo: DynamoDBMetadataRepository
    ) -> None:
        """Test status update with error."""
        repo._client.update_item.side_effect = ClientError(
            {"Error": {"Code": "InternalServerError"}},
            "UpdateItem",
        )

        with pytest.raises(StorageError):
            repo.update_status("file-123", "timestamp", "COMPLETED")


class TestDynamoDBMetadataRepositoryQueryByStatus:
    """Tests for query_by_status method."""

    @pytest.fixture
    def repo(self) -> DynamoDBMetadataRepository:
        """Create repository for testing."""
        client = MagicMock()
        return DynamoDBMetadataRepository(
            dynamodb_client=client,
            table_name="test-table",
        )

    def test_query_by_status_success(
        self, repo: DynamoDBMetadataRepository
    ) -> None:
        """Test successful query by status."""
        timestamp = datetime.utcnow().isoformat()
        repo._client.query.return_value = {
            "Items": [
                {
                    "PK": {"S": "FILE#file-123"},
                    "SK": {"S": f"TS#{timestamp}"},
                    "fileId": {"S": "file-123"},
                    "timestamp": {"S": timestamp},
                    "correlationId": {"S": "corr-456"},
                    "originalFilename": {"S": "test.pdf"},
                    "fileSizeBytes": {"N": "1024"},
                    "mimeType": {"S": "application/pdf"},
                    "bucketName": {"S": "test-bucket"},
                    "objectKey": {"S": "test-key"},
                    "status": {"S": "PENDING"},
                }
            ]
        }

        records = repo.query_by_status("PENDING", limit=50)

        assert len(records) == 1
        repo._client.query.assert_called_once()

    def test_query_by_status_empty(
        self, repo: DynamoDBMetadataRepository
    ) -> None:
        """Test query by status when empty."""
        repo._client.query.return_value = {"Items": []}

        records = repo.query_by_status("FAILED")

        assert records == []

    def test_query_by_status_error(
        self, repo: DynamoDBMetadataRepository
    ) -> None:
        """Test query by status with error."""
        repo._client.query.side_effect = ClientError(
            {"Error": {"Code": "InternalServerError"}},
            "Query",
        )

        with pytest.raises(StorageError):
            repo.query_by_status("PENDING")


class TestDynamoDBMetadataRepositoryIncrementRetryCount:
    """Tests for increment_retry_count method."""

    @pytest.fixture
    def repo(self) -> DynamoDBMetadataRepository:
        """Create repository for testing."""
        client = MagicMock()
        return DynamoDBMetadataRepository(
            dynamodb_client=client,
            table_name="test-table",
        )

    def test_increment_retry_count_success(
        self, repo: DynamoDBMetadataRepository
    ) -> None:
        """Test successful retry count increment."""
        repo._client.update_item.return_value = {
            "Attributes": {"retryCount": {"N": "3"}}
        }

        count = repo.increment_retry_count("file-123", "2024-01-01T00:00:00")

        assert count == 3

    def test_increment_retry_count_error(
        self, repo: DynamoDBMetadataRepository
    ) -> None:
        """Test retry count increment with error."""
        repo._client.update_item.side_effect = ClientError(
            {"Error": {"Code": "InternalServerError"}},
            "UpdateItem",
        )

        with pytest.raises(StorageError):
            repo.increment_retry_count("file-123", "timestamp")
