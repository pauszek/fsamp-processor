# =============================================================================
# Unit Tests for Outbox Repository Adapter
# =============================================================================
"""Tests for DynamoDB Outbox Repository adapter."""

from datetime import datetime
from unittest.mock import MagicMock

import pytest
from botocore.exceptions import ClientError

from processor.adapters.outbound.outbox_repo import DynamoDBOutboxRepository
from processor.domain.exceptions import StorageError
from processor.domain.models import (
    MetadataRecord,
    OutboxEvent,
    OutboxEventType,
    ProcessingStatus,
)


class TestDynamoDBOutboxRepositoryInit:
    """Tests for DynamoDBOutboxRepository initialization."""

    def test_init(self) -> None:
        """Test initialization."""
        client = MagicMock()
        repo = DynamoDBOutboxRepository(
            dynamodb_client=client,
            metadata_table_name="metadata-table",
            outbox_table_name="outbox-table",
        )

        assert repo._client is client
        assert repo._metadata_table_name == "metadata-table"
        assert repo._outbox_table_name == "outbox-table"


class TestDynamoDBOutboxRepositorySaveWithOutbox:
    """Tests for save_with_outbox method."""

    @pytest.fixture
    def repo(self) -> DynamoDBOutboxRepository:
        """Create repository for testing."""
        client = MagicMock()
        return DynamoDBOutboxRepository(
            dynamodb_client=client,
            metadata_table_name="metadata-table",
            outbox_table_name="outbox-table",
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
            status=ProcessingStatus.COMPLETED,
        )

    @pytest.fixture
    def outbox_event(self) -> OutboxEvent:
        """Create sample outbox event."""
        return OutboxEvent(
            event_id="event-789",
            event_type=OutboxEventType.FILE_PROCESSED,
            aggregate_id="file-123",
            aggregate_type="FileProcessing",
            payload={"file_id": "file-123"},
        )

    def test_save_with_outbox_success(
        self,
        repo: DynamoDBOutboxRepository,
        metadata_record: MetadataRecord,
        outbox_event: OutboxEvent,
    ) -> None:
        """Test successful transactional save."""
        repo.save_with_outbox(metadata_record, outbox_event)

        repo._client.transact_write_items.assert_called_once()
        call_kwargs = repo._client.transact_write_items.call_args.kwargs
        assert len(call_kwargs["TransactItems"]) == 2

    def test_save_with_outbox_duplicate(
        self,
        repo: DynamoDBOutboxRepository,
        metadata_record: MetadataRecord,
        outbox_event: OutboxEvent,
    ) -> None:
        """Test handling duplicate (conditional check failure)."""
        repo._client.transact_write_items.side_effect = ClientError(
            {
                "Error": {"Code": "TransactionCanceledException"},
                "CancellationReasons": [{"Code": "ConditionalCheckFailed"}],
            },
            "TransactWriteItems",
        )

        # Should not raise - duplicate is handled gracefully
        repo.save_with_outbox(metadata_record, outbox_event)

    def test_save_with_outbox_error(
        self,
        repo: DynamoDBOutboxRepository,
        metadata_record: MetadataRecord,
        outbox_event: OutboxEvent,
    ) -> None:
        """Test save with error."""
        repo._client.transact_write_items.side_effect = ClientError(
            {"Error": {"Code": "InternalServerError"}},
            "TransactWriteItems",
        )

        with pytest.raises(StorageError):
            repo.save_with_outbox(metadata_record, outbox_event)


class TestDynamoDBOutboxRepositoryGetPendingEvents:
    """Tests for get_pending_events method."""

    @pytest.fixture
    def repo(self) -> DynamoDBOutboxRepository:
        """Create repository for testing."""
        client = MagicMock()
        return DynamoDBOutboxRepository(
            dynamodb_client=client,
            metadata_table_name="metadata-table",
            outbox_table_name="outbox-table",
        )

    def test_get_pending_events_success(self, repo: DynamoDBOutboxRepository) -> None:
        """Test getting pending events."""
        repo._client.query.return_value = {
            "Items": [
                {
                    "PK": {"S": "OUTBOX#FileProcessing"},
                    "SK": {"S": "EVENT#event-1"},
                    "eventId": {"S": "event-1"},
                    "eventType": {"S": "FILE_PROCESSED"},
                    "aggregateId": {"S": "file-1"},
                    "aggregateType": {"S": "FileProcessing"},
                    "payload": {"S": '{"file_id": "file-1"}'},
                    "status": {"S": "PENDING"},
                    "createdAt": {"S": "2024-01-01T00:00:00"},
                }
            ]
        }

        events = repo.get_pending_events(limit=50)

        assert len(events) == 1
        repo._client.query.assert_called_once()

    def test_get_pending_events_empty(self, repo: DynamoDBOutboxRepository) -> None:
        """Test getting pending events when none exist."""
        repo._client.query.return_value = {"Items": []}

        events = repo.get_pending_events()

        assert events == []

    def test_get_pending_events_error(self, repo: DynamoDBOutboxRepository) -> None:
        """Test getting pending events with error."""
        repo._client.query.side_effect = ClientError(
            {"Error": {"Code": "InternalServerError"}},
            "Query",
        )

        with pytest.raises(StorageError):
            repo.get_pending_events()


class TestDynamoDBOutboxRepositoryMarkPublished:
    """Tests for mark_published method."""

    @pytest.fixture
    def repo(self) -> DynamoDBOutboxRepository:
        """Create repository for testing."""
        client = MagicMock()
        return DynamoDBOutboxRepository(
            dynamodb_client=client,
            metadata_table_name="metadata-table",
            outbox_table_name="outbox-table",
        )

    def test_mark_published_success(self, repo: DynamoDBOutboxRepository) -> None:
        """Test marking event as published."""
        repo.mark_published("event-123")

        repo._client.update_item.assert_called_once()
        call_kwargs = repo._client.update_item.call_args.kwargs
        assert call_kwargs["TableName"] == "outbox-table"
        assert ":status" in call_kwargs["ExpressionAttributeValues"]

    def test_mark_published_with_aggregate_type(self, repo: DynamoDBOutboxRepository) -> None:
        """Test marking event as published with custom aggregate type."""
        repo.mark_published("event-123", aggregate_type="CustomType")

        call_kwargs = repo._client.update_item.call_args.kwargs
        key = call_kwargs["Key"]
        assert key["PK"]["S"] == "OUTBOX#CustomType"

    def test_mark_published_error(self, repo: DynamoDBOutboxRepository) -> None:
        """Test marking event with error."""
        repo._client.update_item.side_effect = ClientError(
            {"Error": {"Code": "InternalServerError"}},
            "UpdateItem",
        )

        with pytest.raises(StorageError):
            repo.mark_published("event-123")


class TestDynamoDBOutboxRepositoryMarkFailed:
    """Tests for mark_failed method."""

    @pytest.fixture
    def repo(self) -> DynamoDBOutboxRepository:
        """Create repository for testing."""
        client = MagicMock()
        return DynamoDBOutboxRepository(
            dynamodb_client=client,
            metadata_table_name="metadata-table",
            outbox_table_name="outbox-table",
        )

    def test_mark_failed_success(self, repo: DynamoDBOutboxRepository) -> None:
        """Test marking event as failed."""
        repo.mark_failed("event-123", "Test error message")

        repo._client.update_item.assert_called_once()
        call_kwargs = repo._client.update_item.call_args.kwargs
        assert ":error" in call_kwargs["ExpressionAttributeValues"]
        assert ":status" in call_kwargs["ExpressionAttributeValues"]

    def test_mark_failed_with_aggregate_type(self, repo: DynamoDBOutboxRepository) -> None:
        """Test marking event as failed with custom aggregate type."""
        repo.mark_failed("event-123", "Error", aggregate_type="CustomType")

        call_kwargs = repo._client.update_item.call_args.kwargs
        key = call_kwargs["Key"]
        assert key["PK"]["S"] == "OUTBOX#CustomType"

    def test_mark_failed_error(self, repo: DynamoDBOutboxRepository) -> None:
        """Test marking event failed with error."""
        repo._client.update_item.side_effect = ClientError(
            {"Error": {"Code": "InternalServerError"}},
            "UpdateItem",
        )

        with pytest.raises(StorageError):
            repo.mark_failed("event-123", "Error")


class TestDynamoDBOutboxRepositoryGetFailedEvents:
    """Tests for get_failed_events method."""

    @pytest.fixture
    def repo(self) -> DynamoDBOutboxRepository:
        """Create repository for testing."""
        client = MagicMock()
        return DynamoDBOutboxRepository(
            dynamodb_client=client,
            metadata_table_name="metadata-table",
            outbox_table_name="outbox-table",
        )

    def test_get_failed_events_success(self, repo: DynamoDBOutboxRepository) -> None:
        """Test getting failed events."""
        repo._client.query.return_value = {
            "Items": [
                {
                    "PK": {"S": "OUTBOX#FileProcessing"},
                    "SK": {"S": "EVENT#event-1"},
                    "eventId": {"S": "event-1"},
                    "eventType": {"S": "FILE_PROCESSED"},
                    "aggregateId": {"S": "file-1"},
                    "aggregateType": {"S": "FileProcessing"},
                    "payload": {"S": '{"file_id": "file-1"}'},
                    "status": {"S": "FAILED"},
                    "createdAt": {"S": "2024-01-01T00:00:00"},
                    "errorMessage": {"S": "Some error"},
                }
            ]
        }

        events = repo.get_failed_events(limit=50)

        assert len(events) == 1
        repo._client.query.assert_called_once()

    def test_get_failed_events_empty(self, repo: DynamoDBOutboxRepository) -> None:
        """Test getting failed events when none exist."""
        repo._client.query.return_value = {"Items": []}

        events = repo.get_failed_events()

        assert events == []

    def test_get_failed_events_error(self, repo: DynamoDBOutboxRepository) -> None:
        """Test getting failed events with error."""
        repo._client.query.side_effect = ClientError(
            {"Error": {"Code": "InternalServerError"}},
            "Query",
        )

        with pytest.raises(StorageError):
            repo.get_failed_events()


class TestDynamoDBOutboxRepositoryDeleteOldPublished:
    """Tests for delete_old_published method."""

    @pytest.fixture
    def repo(self) -> DynamoDBOutboxRepository:
        """Create repository for testing."""
        client = MagicMock()
        return DynamoDBOutboxRepository(
            dynamodb_client=client,
            metadata_table_name="metadata-table",
            outbox_table_name="outbox-table",
        )

    def test_delete_old_published_success(self, repo: DynamoDBOutboxRepository) -> None:
        """Test deleting old published events."""
        repo._client.query.return_value = {
            "Items": [
                {"PK": {"S": "OUTBOX#FileProcessing"}, "SK": {"S": "EVENT#event-1"}},
                {"PK": {"S": "OUTBOX#FileProcessing"}, "SK": {"S": "EVENT#event-2"}},
            ]
        }

        deleted = repo.delete_old_published(older_than_hours=24)

        assert deleted == 2
        repo._client.batch_write_item.assert_called_once()

    def test_delete_old_published_no_events(self, repo: DynamoDBOutboxRepository) -> None:
        """Test deleting when no old events exist."""
        repo._client.query.return_value = {"Items": []}

        deleted = repo.delete_old_published()

        assert deleted == 0
        repo._client.batch_write_item.assert_not_called()

    def test_delete_old_published_error(self, repo: DynamoDBOutboxRepository) -> None:
        """Test delete with error."""
        repo._client.query.side_effect = ClientError(
            {"Error": {"Code": "InternalServerError"}},
            "Query",
        )

        with pytest.raises(StorageError):
            repo.delete_old_published()


class TestDynamoDBOutboxRepositoryUpdateMetadataWithOutbox:
    """Tests for update_metadata_with_outbox method."""

    @pytest.fixture
    def repo(self) -> DynamoDBOutboxRepository:
        """Create repository for testing."""
        client = MagicMock()
        return DynamoDBOutboxRepository(
            dynamodb_client=client,
            metadata_table_name="metadata-table",
            outbox_table_name="outbox-table",
        )

    @pytest.fixture
    def outbox_event(self) -> OutboxEvent:
        """Create sample outbox event."""
        return OutboxEvent(
            event_id="event-789",
            event_type=OutboxEventType.FILE_PROCESSED,
            aggregate_id="file-123",
            aggregate_type="FileProcessing",
            payload={"file_id": "file-123"},
        )

    def test_update_metadata_with_outbox_success(
        self,
        repo: DynamoDBOutboxRepository,
        outbox_event: OutboxEvent,
    ) -> None:
        """Test updating metadata with outbox event."""
        repo.update_metadata_with_outbox(
            file_id="file-123",
            timestamp="2024-01-01T00:00:00",
            status="COMPLETED",
            outbox_event=outbox_event,
        )

        repo._client.transact_write_items.assert_called_once()
        call_kwargs = repo._client.transact_write_items.call_args.kwargs
        assert len(call_kwargs["TransactItems"]) == 2

    def test_update_metadata_with_outbox_with_error_message(
        self,
        repo: DynamoDBOutboxRepository,
        outbox_event: OutboxEvent,
    ) -> None:
        """Test updating metadata with error message."""
        repo.update_metadata_with_outbox(
            file_id="file-123",
            timestamp="2024-01-01T00:00:00",
            status="FAILED",
            outbox_event=outbox_event,
            error_message="Processing failed",
        )

        call_kwargs = repo._client.transact_write_items.call_args.kwargs
        update_item = call_kwargs["TransactItems"][0]["Update"]
        assert ":error" in update_item["ExpressionAttributeValues"]

    def test_update_metadata_with_outbox_error(
        self,
        repo: DynamoDBOutboxRepository,
        outbox_event: OutboxEvent,
    ) -> None:
        """Test update with error."""
        repo._client.transact_write_items.side_effect = ClientError(
            {"Error": {"Code": "InternalServerError"}},
            "TransactWriteItems",
        )

        with pytest.raises(StorageError):
            repo.update_metadata_with_outbox(
                file_id="file-123",
                timestamp="2024-01-01T00:00:00",
                status="COMPLETED",
                outbox_event=outbox_event,
            )
