# =============================================================================
# Integration Tests for DynamoDB Repository
# =============================================================================
"""Tests for DynamoDBMetadataRepository with mocked AWS."""

import pytest
from datetime import datetime

from processor.adapters.outbound.dynamodb_repo import DynamoDBMetadataRepository
from processor.domain.models import MetadataRecord, ProcessingStatus


class TestDynamoDBMetadataRepository:
    """Integration tests for DynamoDB metadata repository."""

    @pytest.fixture
    def repo(self, dynamodb_client, test_table_name: str) -> DynamoDBMetadataRepository:
        """Create DynamoDB repository."""
        return DynamoDBMetadataRepository(
            dynamodb_client=dynamodb_client,
            table_name=test_table_name,
        )

    @pytest.fixture
    def sample_record(self) -> MetadataRecord:
        """Create sample metadata record."""
        return MetadataRecord(
            file_id="test-file-001",
            timestamp=datetime.utcnow().isoformat(),
            correlation_id="corr-integration-test",
            original_filename="integration-test.pdf",
            file_size_bytes=4096,
            mime_type="application/pdf",
            bucket_name="test-bucket",
            object_key="test/integration-test.pdf",
            status=ProcessingStatus.PENDING,
            is_encrypted=True,
        )

    def test_save_and_get(
        self, repo: DynamoDBMetadataRepository, sample_record: MetadataRecord
    ) -> None:
        """Test saving and retrieving a record."""
        # Save
        repo.save(sample_record)

        # Get
        retrieved = repo.get_by_id(sample_record.file_id)

        assert retrieved is not None
        assert retrieved.file_id == sample_record.file_id
        assert retrieved.correlation_id == sample_record.correlation_id
        assert retrieved.original_filename == sample_record.original_filename

    def test_get_nonexistent(self, repo: DynamoDBMetadataRepository) -> None:
        """Test getting a record that doesn't exist."""
        result = repo.get_by_id("nonexistent-id")
        assert result is None

    def test_get_history(
        self, repo: DynamoDBMetadataRepository
    ) -> None:
        """Test getting record history."""
        file_id = "history-test-file"

        # Save multiple records with different timestamps
        for i in range(5):
            record = MetadataRecord(
                file_id=file_id,
                timestamp=f"2024-01-0{i+1}T12:00:00Z",
                correlation_id=f"corr-{i}",
                original_filename="history-test.pdf",
                file_size_bytes=1000 + i,
                mime_type="application/pdf",
                bucket_name="bucket",
                object_key="key",
                status=ProcessingStatus.COMPLETED,
            )
            repo.save(record)

        # Get history
        history = repo.get_history(file_id, limit=10)

        assert len(history) == 5
        # Should be newest first (descending)
        assert history[0].timestamp > history[-1].timestamp

    def test_update_status(
        self, repo: DynamoDBMetadataRepository, sample_record: MetadataRecord
    ) -> None:
        """Test updating record status."""
        # Save initial record
        repo.save(sample_record)

        # Update status
        repo.update_status(
            file_id=sample_record.file_id,
            timestamp=sample_record.timestamp,
            status=ProcessingStatus.COMPLETED.value,
        )

        # Verify update
        retrieved = repo.get_by_id(sample_record.file_id)
        assert retrieved is not None
        assert retrieved.status == ProcessingStatus.COMPLETED

    def test_update_status_with_error(
        self, repo: DynamoDBMetadataRepository, sample_record: MetadataRecord
    ) -> None:
        """Test updating status with error message."""
        repo.save(sample_record)

        repo.update_status(
            file_id=sample_record.file_id,
            timestamp=sample_record.timestamp,
            status=ProcessingStatus.FAILED.value,
            error_message="Processing failed: timeout",
        )

        retrieved = repo.get_by_id(sample_record.file_id)
        assert retrieved is not None
        assert retrieved.status == ProcessingStatus.FAILED
        assert retrieved.error_message == "Processing failed: timeout"

    def test_query_by_status(
        self, repo: DynamoDBMetadataRepository
    ) -> None:
        """Test querying records by status using GSI."""
        # Create records with different statuses
        for i, status in enumerate([
            ProcessingStatus.PENDING,
            ProcessingStatus.PENDING,
            ProcessingStatus.COMPLETED,
            ProcessingStatus.FAILED,
        ]):
            record = MetadataRecord(
                file_id=f"status-query-{i}",
                timestamp=datetime.utcnow().isoformat(),
                correlation_id=f"corr-{i}",
                original_filename=f"file-{i}.pdf",
                file_size_bytes=1000,
                mime_type="application/pdf",
                bucket_name="bucket",
                object_key=f"key-{i}",
                status=status,
            )
            repo.save(record)

        # Query pending records
        pending = repo.query_by_status(ProcessingStatus.PENDING.value)
        assert len(pending) == 2

        # Query completed records
        completed = repo.query_by_status(ProcessingStatus.COMPLETED.value)
        assert len(completed) == 1
