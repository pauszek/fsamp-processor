# =============================================================================
# Unit Tests for Domain Models
# =============================================================================
"""Tests for domain models and value objects."""

from datetime import datetime

import pytest

from processor.domain.models import (
    AnalysisResult,
    FileContent,
    MetadataRecord,
    ProcessingResult,
    ProcessingStatus,
)


class TestProcessingResult:
    """Tests for ProcessingResult value object."""

    def test_create_result(self) -> None:
        """Test creating a processing result."""
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
        """Test is_success property."""
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
        """Test immutable completion update."""
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

        # Original unchanged
        assert original.status == ProcessingStatus.IN_PROGRESS
        assert original.completed_at is None

        # New object updated
        assert completed.status == ProcessingStatus.COMPLETED
        assert completed.completed_at is not None
        assert completed.metadata["key"] == "value"

    def test_duration_calculation(self) -> None:
        """Test duration calculation."""
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


class TestFileContent:
    """Tests for FileContent value object."""

    def test_create_content(self) -> None:
        """Test creating file content."""
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
        """Test encrypted content detection."""
        content = FileContent(
            data=b"encrypted",
            encryption_algorithm="aws:kms",
            kms_key_id="key-123",
        )

        assert content.is_encrypted is True


class TestMetadataRecord:
    """Tests for MetadataRecord."""

    @pytest.fixture
    def sample_record(self) -> MetadataRecord:
        """Create sample metadata record."""
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
        """Test conversion to DynamoDB item format."""
        item = sample_record.to_dynamodb_item()

        assert item["PK"]["S"] == "FILE#file-123"
        assert item["SK"]["S"] == "TS#2024-01-01T12:00:00Z"
        assert item["correlationId"]["S"] == "corr-456"
        assert item["originalFilename"]["S"] == "test.pdf"
        assert item["fileSizeBytes"]["N"] == "1024"
        assert item["status"]["S"] == "PENDING"

    def test_from_dynamodb_item(self) -> None:
        """Test creation from DynamoDB item format."""
        item = {
            "PK": {"S": "FILE#file-abc"},
            "SK": {"S": "TS#2024-06-15T10:30:00Z"},
            "correlationId": {"S": "corr-xyz"},
            "originalFilename": {"S": "document.pdf"},
            "fileSizeBytes": {"N": "2048"},
            "bucketName": {"S": "my-bucket"},
            "objectKey": {"S": "docs/document.pdf"},
            "status": {"S": "COMPLETED"},
            "isEncrypted": {"BOOL": True},
            "retryCount": {"N": "2"},
        }

        record = MetadataRecord.from_dynamodb_item(item)

        assert record.file_id == "file-abc"
        assert record.timestamp == "2024-06-15T10:30:00Z"
        assert record.correlation_id == "corr-xyz"
        assert record.file_size_bytes == 2048
        assert record.status == ProcessingStatus.COMPLETED
        assert record.retry_count == 2

    def test_roundtrip_conversion(self, sample_record: MetadataRecord) -> None:
        """Test roundtrip DynamoDB conversion."""
        item = sample_record.to_dynamodb_item()
        restored = MetadataRecord.from_dynamodb_item(item)

        assert restored.file_id == sample_record.file_id
        assert restored.correlation_id == sample_record.correlation_id
        assert restored.original_filename == sample_record.original_filename


class TestAnalysisResult:
    """Tests for AnalysisResult value object."""

    def test_create_safe_result(self) -> None:
        """Test creating safe analysis result."""
        result = AnalysisResult(
            file_hash_sha256="abc123def456",
            is_safe=True,
            scan_engine="test-engine",
        )

        assert result.is_safe is True
        assert result.findings == []

    def test_create_unsafe_result(self) -> None:
        """Test creating unsafe analysis result with findings."""
        result = AnalysisResult(
            file_hash_sha256="xyz789",
            is_safe=False,
            scan_engine="av-scanner",
            findings=["Potential malware detected", "Suspicious macro"],
        )

        assert result.is_safe is False
        assert len(result.findings) == 2
