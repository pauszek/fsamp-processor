from datetime import UTC, datetime

import pytest

from processor.adapters.outbound.dynamodb_repo import DynamoDBMetadataRepository
from processor.adapters.outbound.outbox_repo import DynamoDBOutboxRepository
from processor.domain.events import EventType, FileEvent, ProcessingResultDetails
from processor.domain.exceptions import StorageError
from processor.domain.models import MetadataRecord, OutboxEvent, ProcessingStatus

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

    def test_repeated_saves_update_current_state(self, repo: DynamoDBMetadataRepository) -> None:
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

        current = repo.get_history(file_id, limit=10)

        assert len(current) == 1
        assert current[0].correlation_id == "corr-4"
        assert current[0].file_size_bytes == 1004

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

    def test_claim_takeover_fences_the_expired_worker(
        self,
        repo: DynamoDBMetadataRepository,
        sample_record: MetadataRecord,
        localstack_dynamodb_client,
        localstack_table_name: str,
    ) -> None:
        repo.save(sample_record)
        first = repo.claim_processing(sample_record.file_id, "event-1", lease_seconds=330)
        assert first is not None
        assert repo.claim_processing(sample_record.file_id, "event-1", lease_seconds=330) is None

        localstack_dynamodb_client.update_item(
            TableName=localstack_table_name,
            Key={
                "PK": {"S": f"FILE#{sample_record.file_id}"},
                "SK": {"S": "METADATA"},
            },
            UpdateExpression="SET processorClaimExpiresAt = :expired",
            ExpressionAttributeValues={":expired": {"N": "0"}},
        )
        takeover = repo.claim_processing(sample_record.file_id, "event-1", lease_seconds=330)
        assert takeover is not None
        assert takeover.version == first.version + 1
        assert takeover.token != first.token

        sample_record.status = ProcessingStatus.COMPLETED
        sample_record.last_processed_event_id = "event-1"
        with pytest.raises(StorageError):
            repo.save(sample_record, claim=first)

        repo.save(sample_record, claim=takeover)
        item = localstack_dynamodb_client.get_item(
            TableName=localstack_table_name,
            Key={
                "PK": {"S": f"FILE#{sample_record.file_id}"},
                "SK": {"S": "METADATA"},
            },
            ConsistentRead=True,
        )["Item"]
        assert item["status"]["S"] == ProcessingStatus.COMPLETED.value
        assert item["lastProcessedEventId"]["S"] == "event-1"
        assert "processorClaimToken" not in item

    def test_claim_takeover_fences_metadata_and_outbox_transaction(
        self,
        repo: DynamoDBMetadataRepository,
        sample_record: MetadataRecord,
        sample_file_event: FileEvent,
        localstack_dynamodb_client,
        localstack_table_name: str,
    ) -> None:
        sample_record.file_id = sample_file_event.file_id_str
        repo.save(sample_record)
        first = repo.claim_processing(
            sample_record.file_id,
            sample_file_event.event_id_str,
            lease_seconds=330,
        )
        assert first is not None

        localstack_dynamodb_client.update_item(
            TableName=localstack_table_name,
            Key={
                "PK": {"S": f"FILE#{sample_record.file_id}"},
                "SK": {"S": "METADATA"},
            },
            UpdateExpression="SET processorClaimExpiresAt = :expired",
            ExpressionAttributeValues={":expired": {"N": "0"}},
        )
        takeover = repo.claim_processing(
            sample_record.file_id,
            sample_file_event.event_id_str,
            lease_seconds=330,
        )
        assert takeover is not None

        completion = sample_file_event.with_new_event_type(
            EventType.ANALYSIS_COMPLETED,
            processing_result=ProcessingResultDetails(
                is_safe=True,
                findings=[],
                processed_at=datetime.now(UTC),
            ),
        )
        outbox_event = OutboxEvent.from_file_event(completion)
        sample_record.status = ProcessingStatus.COMPLETED
        sample_record.last_processed_event_id = sample_file_event.event_id_str
        outbox_repo = DynamoDBOutboxRepository(
            localstack_dynamodb_client,
            localstack_table_name,
            localstack_table_name,
        )

        with pytest.raises(StorageError):
            outbox_repo.save_with_outbox(sample_record, outbox_event, claim=first)

        outbox_key = {
            "PK": {"S": str(outbox_event.outbox_partition)},
            "SK": {"S": f"EVENT#{outbox_event.event_id}"},
        }
        assert "Item" not in localstack_dynamodb_client.get_item(
            TableName=localstack_table_name,
            Key=outbox_key,
            ConsistentRead=True,
        )

        outbox_repo.save_with_outbox(sample_record, outbox_event, claim=takeover)
        assert "Item" in localstack_dynamodb_client.get_item(
            TableName=localstack_table_name,
            Key=outbox_key,
            ConsistentRead=True,
        )

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
