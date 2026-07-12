from datetime import UTC, datetime

import pytest

from processor.domain.events import (
    EventType,
    FailureDetails,
    FileEvent,
    ProcessingResultDetails,
)
from processor.domain.models import (
    AnalysisResult,
    FileContent,
    MetadataRecord,
    OutboxEvent,
    OutboxEventType,
    OutboxStatus,
    ProcessingResult,
    ProcessingStatus,
)


class TestProcessingResult:
    def test_create_result(self) -> None:
        result = ProcessingResult(
            event_id="event-123",
            correlation_id="corr-456",
            status=ProcessingStatus.IN_PROGRESS,
            started_at=datetime.utcnow(),
        )

        assert result.event_id == "event-123"
        assert result.status == ProcessingStatus.IN_PROGRESS
        assert result.completed_at is None

    def test_is_success(self) -> None:
        success = ProcessingResult(
            event_id="1",
            correlation_id="1",
            status=ProcessingStatus.COMPLETED,
            started_at=datetime.utcnow(),
        )
        failed = ProcessingResult(
            event_id="2",
            correlation_id="2",
            status=ProcessingStatus.FAILED,
            started_at=datetime.utcnow(),
        )

        assert success.is_success is True
        assert failed.is_success is False

    def test_with_completion(self) -> None:
        original = ProcessingResult(
            event_id="event-1",
            correlation_id="corr-1",
            status=ProcessingStatus.IN_PROGRESS,
            started_at=datetime.utcnow(),
        )

        completed = original.with_completion(
            status=ProcessingStatus.COMPLETED,
            metadata={"key": "value"},
        )

        assert original.status == ProcessingStatus.IN_PROGRESS
        assert original.completed_at is None

        assert completed.status == ProcessingStatus.COMPLETED
        assert completed.completed_at is not None
        assert completed.metadata["key"] == "value"

    def test_duration_calculation(self) -> None:
        started = datetime(2024, 1, 1, 12, 0, 0)
        completed = datetime(2024, 1, 1, 12, 0, 1, 500000)  # 1.5 seconds later

        result = ProcessingResult(
            event_id="1",
            correlation_id="1",
            status=ProcessingStatus.COMPLETED,
            started_at=started,
            completed_at=completed,
        )

        assert result.duration_ms == 1500

    def test_duration_is_none_until_completed(self) -> None:
        result = ProcessingResult(
            event_id="1",
            correlation_id="1",
            status=ProcessingStatus.IN_PROGRESS,
            started_at=datetime.now(UTC),
        )

        assert result.duration_ms is None
        assert result.is_failure is False

    def test_with_completion_preserves_retry_and_merges_metadata(self) -> None:
        original = ProcessingResult(
            event_id="event-1",
            correlation_id="corr-1",
            status=ProcessingStatus.RETRYING,
            started_at=datetime.now(UTC),
            retry_count=2,
            metadata={"source": "sqs"},
        )

        failed = original.with_completion(
            status=ProcessingStatus.FAILED,
            error_message="boom",
            error_code="PROCESSING_FAILED",
            metadata={"attempt": 3},
        )

        assert failed.is_failure is True
        assert failed.retry_count == 2
        assert failed.error_message == "boom"
        assert failed.error_code == "PROCESSING_FAILED"
        assert failed.metadata == {"source": "sqs", "attempt": 3}


class TestFileContent:
    def test_create_content(self) -> None:
        content = FileContent(
            data=b"test content",
            content_type="text/plain",
            content_length=12,
            etag="abc123",
        )

        assert content.data == b"test content"
        assert content.content_type == "text/plain"
        assert content.is_encrypted is False

    def test_encrypted_content(self) -> None:
        content = FileContent(
            data=b"encrypted",
            encryption_algorithm="aws:kms",
            kms_key_id="key-123",
        )

        assert content.is_encrypted is True


class TestMetadataRecord:
    @pytest.fixture
    def sample_record(self) -> MetadataRecord:
        return MetadataRecord(
            file_id="file-123",
            timestamp="2024-01-01T12:00:00Z",
            correlation_id="corr-456",
            original_filename="test.pdf",
            file_size_bytes=1024,
            mime_type="application/pdf",
            bucket_name="test-bucket",
            object_key="path/to/file.pdf",
            status=ProcessingStatus.PENDING,
        )

    def test_to_dynamodb_item(self, sample_record: MetadataRecord) -> None:
        item = sample_record.to_dynamodb_item()

        assert item["PK"]["S"] == "FILE#file-123"
        assert item["SK"]["S"] == "METADATA"
        assert item["entityType"]["S"] == "FILE_METADATA"
        assert item["correlationId"]["S"] == "corr-456"
        assert item["originalFilename"]["S"] == "test.pdf"
        assert item["fileSizeBytes"]["N"] == "1024"
        assert item["status"]["S"] == "PENDING"

    def test_to_dynamodb_item_includes_optional_fields(self) -> None:
        record = MetadataRecord(
            file_id="file-123",
            timestamp="2026-05-12T00:00:00+00:00",
            correlation_id="corr-123",
            original_filename="sample.txt",
            file_size_bytes=42,
            mime_type="text/plain",
            bucket_name="bucket",
            object_key="object",
            status=ProcessingStatus.COMPLETED,
            file_hash="a" * 64,
            kms_key_id="arn:aws:kms:us-west-2:123456789012:key/abc",
            is_safe=True,
            scan_findings=["clean"],
            created_at="2026-05-12T00:00:00+00:00",
            updated_at="2026-05-12T00:01:00+00:00",
            processed_at="2026-05-12T00:02:00+00:00",
            error_message="previous transient error",
            error_code="TRANSIENT",
            ttl=123456,
        )

        item = record.to_dynamodb_item()

        assert item["mimeType"]["S"] == "text/plain"
        assert item["fileHash"]["S"] == "a" * 64
        assert item["kmsKeyId"]["S"].endswith("key/abc")
        assert item["isSafe"]["BOOL"] is True
        assert item["scanFindings"]["SS"] == ["clean"]
        assert item["createdAt"]["S"] == "2026-05-12T00:00:00+00:00"
        assert item["updatedAt"]["S"] == "2026-05-12T00:01:00+00:00"
        assert item["processedAt"]["S"] == "2026-05-12T00:02:00+00:00"
        assert item["errorMessage"]["S"] == "previous transient error"
        assert item["errorCode"]["S"] == "TRANSIENT"
        assert item["ttl"]["N"] == "123456"

    def test_from_dynamodb_item(self) -> None:
        item = {
            "PK": {"S": "FILE#file-abc"},
            "SK": {"S": "METADATA"},
            "correlationId": {"S": "corr-xyz"},
            "originalFilename": {"S": "document.pdf"},
            "fileSizeBytes": {"N": "2048"},
            "bucketName": {"S": "my-bucket"},
            "objectKey": {"S": "docs/document.pdf"},
            "status": {"S": "COMPLETED"},
            "isEncrypted": {"BOOL": True},
            "retryCount": {"N": "2"},
            "updatedAt": {"S": "2024-06-15T10:30:00Z"},
        }

        record = MetadataRecord.from_dynamodb_item(item)

        assert record.file_id == "file-abc"
        assert record.timestamp == "2024-06-15T10:30:00Z"
        assert record.correlation_id == "corr-xyz"
        assert record.file_size_bytes == 2048
        assert record.status == ProcessingStatus.COMPLETED
        assert record.retry_count == 2

    def test_roundtrip_conversion(self, sample_record: MetadataRecord) -> None:
        item = sample_record.to_dynamodb_item()
        restored = MetadataRecord.from_dynamodb_item(item)

        assert restored.file_id == sample_record.file_id
        assert restored.correlation_id == sample_record.correlation_id
        assert restored.original_filename == sample_record.original_filename


class TestAnalysisResult:
    def test_create_safe_result(self) -> None:
        result = AnalysisResult(
            file_hash_sha256="abc123def456",
            is_safe=True,
            scan_engine="test-engine",
        )

        assert result.is_safe is True
        assert result.findings == []

    def test_create_unsafe_result(self) -> None:
        result = AnalysisResult(
            file_hash_sha256="xyz789",
            is_safe=False,
            scan_engine="av-scanner",
            findings=["Potential malware detected", "Suspicious macro"],
        )

        assert result.is_safe is False
        assert len(result.findings) == 2


class TestOutboxEvent:
    def test_create_defaults_message_group_to_aggregate_id(self) -> None:
        event = OutboxEvent.create(
            event_type=OutboxEventType.ANALYSIS_COMPLETED,
            aggregate_id="file-123",
            payload={"fileId": "file-123"},
        )

        assert event.status == OutboxStatus.PENDING
        assert event.message_group_id == "file-123"

    def test_file_failed_factory(self, sample_file_event: FileEvent) -> None:
        failure = sample_file_event.with_new_event_type(
            EventType.PROCESSING_FAILED,
            failure=FailureDetails(
                code="SCAN_FAILED",
                message="scanner unavailable",
                failed_at=datetime.now(UTC),
                retryable=True,
            ),
        )
        event = OutboxEvent.from_file_event(failure)

        assert event.event_type == OutboxEventType.PROCESSING_FAILED
        assert event.payload["failure"]["code"] == "SCAN_FAILED"
        assert event.payload["failure"]["message"] == "scanner unavailable"
        assert "failedAt" in event.payload["failure"]

    def test_file_quarantined_factory(self, sample_file_event: FileEvent) -> None:
        completed = sample_file_event.with_new_event_type(
            EventType.ANALYSIS_COMPLETED,
            processing_result=ProcessingResultDetails(
                is_safe=False,
                findings=["signature-match"],
                processed_at=datetime.now(UTC),
            ),
        )
        event = OutboxEvent.from_file_event(completed)

        assert event.event_type == OutboxEventType.ANALYSIS_COMPLETED
        assert event.payload["processingResult"]["isSafe"] is False
        assert event.payload["processingResult"]["findings"] == ["signature-match"]

    def test_to_dynamodb_item_includes_optional_fields(self) -> None:
        event = OutboxEvent(
            event_id="event-123",
            event_type=OutboxEventType.ANALYSIS_COMPLETED,
            aggregate_id="file-123",
            published_at="2026-05-12T00:00:00+00:00",
            retry_count=2,
            last_error="temporary failure",
            message_group_id="group-1",
            ttl=123456,
        )

        item = event.to_dynamodb_item()

        assert item["publishedAt"]["S"] == "2026-05-12T00:00:00+00:00"
        assert item["lastError"]["S"] == "temporary failure"
        assert item["messageGroupId"]["S"] == "group-1"
        assert item["ttl"]["N"] == "123456"

    def test_dynamodb_value_supports_wire_and_deserialized_values(self) -> None:
        assert OutboxEvent._dynamodb_value({"S": "value"}) == "value"
        assert OutboxEvent._dynamodb_value({"N": "7"}) == "7"
        assert OutboxEvent._dynamodb_value({"BOOL": True}) is True
        assert OutboxEvent._dynamodb_value({"NULL": True}) is None
        assert OutboxEvent._dynamodb_value({"nested": "value"}) == {"nested": "value"}
        assert OutboxEvent._dynamodb_value("plain") == "plain"

    def test_from_dynamodb_item_supports_deserialized_stream_image(self) -> None:
        item = {
            "eventId": "event-123",
            "eventType": "ANALYSIS_COMPLETED",
            "aggregateId": "file-123",
            "payload": {"fileId": "file-123"},
            "status": "PENDING",
            "createdAt": "2026-05-12T00:00:00+00:00",
            "retryCount": 2,
            "publishedAt": "2026-05-12T01:00:00+00:00",
            "lastError": "previous failure",
            "messageGroupId": "group-1",
            "ttl": "123456",
        }

        event = OutboxEvent.from_dynamodb_item(item)

        assert event.event_id == "event-123"
        assert event.aggregate_type == "FileProcessing"
        assert event.payload == {"fileId": "file-123"}
        assert event.status == OutboxStatus.PENDING
        assert event.retry_count == 2
        assert event.ttl == 123456

    def test_from_dynamodb_stream_record_requires_new_image(self) -> None:
        with pytest.raises(ValueError, match="No NewImage"):
            OutboxEvent.from_dynamodb_stream_record({"dynamodb": {}})

    def test_from_dynamodb_stream_record_reads_new_image(self) -> None:
        item = OutboxEvent.create(
            event_type=OutboxEventType.ANALYSIS_COMPLETED,
            aggregate_id="file-123",
            payload={"fileId": "file-123"},
        ).to_dynamodb_item()

        event = OutboxEvent.from_dynamodb_stream_record({"dynamodb": {"NewImage": item}})

        assert event.aggregate_id == "file-123"

    def test_mark_published_and_failed_update_status_fields(self) -> None:
        event = OutboxEvent.create(
            event_type=OutboxEventType.ANALYSIS_COMPLETED,
            aggregate_id="file-123",
            payload={},
        )

        assert event.mark_published() is event
        assert event.status == OutboxStatus.PUBLISHED
        assert event.published_at is not None
        assert event.ttl is not None

        assert event.mark_failed("sns down") is event
        assert event.status == OutboxStatus.FAILED
        assert event.last_error == "sns down"
        assert event.retry_count == 1

    def test_sns_message_and_attributes(self, sample_file_event: FileEvent) -> None:
        completed = sample_file_event.with_new_event_type(
            EventType.ANALYSIS_COMPLETED,
            processing_result=ProcessingResultDetails(
                is_safe=True,
                findings=[],
                processed_at=datetime.now(UTC),
            ),
        )
        event = OutboxEvent.from_file_event(completed)

        assert event.to_sns_message()["eventType"] == "ANALYSIS_COMPLETED"
        assert event.to_sns_message()["eventId"] == event.event_id
        assert event.to_sns_attributes()["aggregateId"]["StringValue"] == (
            sample_file_event.file_id_str
        )
