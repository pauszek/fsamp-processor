from datetime import datetime

import pytest

from processor.adapters.outbound.dynamodb_repo import DynamoDBMetadataRepository
from processor.domain.models import MetadataRecord, ProcessingStatus

pytestmark = pytest.mark.integration


class TestDynamoDBMetadataRepository:
    @pytest.fixture
    def repo(
        self, localstack_dynamodb_client, localstack_table_name: str
    ) -> DynamoDBMetadataRepository:
        return DynamoDBMetadataRepository(
            dynamodb_client=localstack_dynamodb_client,
            table_name=localstack_table_name,
        )

    @pytest.fixture
    def sample_record(self) -> MetadataRecord:
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
        repo.save(sample_record)

        retrieved = repo.get_by_id(sample_record.file_id)

        assert retrieved is not None
        assert retrieved.file_id == sample_record.file_id
        assert retrieved.correlation_id == sample_record.correlation_id
        assert retrieved.original_filename == sample_record.original_filename

    def test_get_nonexistent(self, repo: DynamoDBMetadataRepository) -> None:
        result = repo.get_by_id("nonexistent-id")
        assert result is None

    def test_get_history(self, repo: DynamoDBMetadataRepository) -> None:
        file_id = "history-test-file"

        for i in range(5):
            record = MetadataRecord(
                file_id=file_id,
                timestamp=f"2024-01-0{i + 1}T12:00:00Z",
                correlation_id=f"corr-{i}",
                original_filename="history-test.pdf",
                file_size_bytes=1000 + i,
                mime_type="application/pdf",
                bucket_name="bucket",
                object_key="key",
                status=ProcessingStatus.COMPLETED,
            )
            repo.save(record)

        history = repo.get_history(file_id, limit=10)

        assert len(history) == 5
        assert history[0].timestamp > history[-1].timestamp

    def test_update_status(
        self, repo: DynamoDBMetadataRepository, sample_record: MetadataRecord
    ) -> None:
        repo.save(sample_record)

        repo.update_status(
            file_id=sample_record.file_id,
            timestamp=sample_record.timestamp,
            status=ProcessingStatus.COMPLETED.value,
        )

        retrieved = repo.get_by_id(sample_record.file_id)
        assert retrieved is not None
        assert retrieved.status == ProcessingStatus.COMPLETED

    def test_update_status_with_error(
        self, repo: DynamoDBMetadataRepository, sample_record: MetadataRecord
    ) -> None:
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

    def test_query_by_status(self, repo: DynamoDBMetadataRepository) -> None:
        for i, status in enumerate(
            [
                ProcessingStatus.PENDING,
                ProcessingStatus.PENDING,
                ProcessingStatus.COMPLETED,
                ProcessingStatus.FAILED,
            ]
        ):
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

        pending = repo.query_by_status(ProcessingStatus.PENDING.value)
        assert len(pending) == 2

        completed = repo.query_by_status(ProcessingStatus.COMPLETED.value)
        assert len(completed) == 1
