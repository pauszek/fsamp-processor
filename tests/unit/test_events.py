# =============================================================================
# Unit Tests for Domain Events
# =============================================================================
"""Tests for Pydantic event models."""

import pytest
from datetime import datetime
from uuid import uuid4

from pydantic import ValidationError

from processor.domain.events import (
    EventType,
    FileEvent,
    FileMetadata,
    SecurityContext,
    StorageLocation,
    SQSMessageWrapper,
)


class TestFileMetadata:
    """Tests for FileMetadata model."""

    def test_valid_metadata(self) -> None:
        """Test creating valid file metadata."""
        metadata = FileMetadata(
            original_filename="document.pdf",
            file_size_bytes=1024,
            mime_type="application/pdf",
        )

        assert metadata.original_filename == "document.pdf"
        assert metadata.file_size_bytes == 1024
        assert metadata.mime_type == "application/pdf"

    def test_metadata_from_camel_case(self) -> None:
        """Test creating metadata from camelCase keys (JSON)."""
        metadata = FileMetadata.model_validate({
            "originalFilename": "test.txt",
            "fileSizeBytes": 500,
            "mimeType": "text/plain",
        })

        assert metadata.original_filename == "test.txt"
        assert metadata.file_size_bytes == 500

    def test_invalid_filename_empty(self) -> None:
        """Test that empty filename is rejected."""
        with pytest.raises(ValidationError):
            FileMetadata(
                original_filename="",
                file_size_bytes=100,
            )

    def test_invalid_file_size_negative(self) -> None:
        """Test that negative file size is rejected."""
        with pytest.raises(ValidationError):
            FileMetadata(
                original_filename="test.txt",
                file_size_bytes=-1,
            )

    def test_optional_mime_type(self) -> None:
        """Test that mime_type is optional."""
        metadata = FileMetadata(
            original_filename="unknown.bin",
            file_size_bytes=256,
        )

        assert metadata.mime_type is None


class TestStorageLocation:
    """Tests for StorageLocation model."""

    def test_valid_location(self) -> None:
        """Test creating valid storage location."""
        location = StorageLocation(
            bucket_name="my-bucket",
            object_key="path/to/file.pdf",
        )

        assert location.bucket_name == "my-bucket"
        assert location.object_key == "path/to/file.pdf"

    def test_s3_uri_property(self) -> None:
        """Test S3 URI generation."""
        location = StorageLocation(
            bucket_name="fsamp-files",
            object_key="uploads/2024/doc.pdf",
        )

        assert location.s3_uri == "s3://fsamp-files/uploads/2024/doc.pdf"

    def test_location_from_camel_case(self) -> None:
        """Test creating location from camelCase keys."""
        location = StorageLocation.model_validate({
            "bucketName": "test-bucket",
            "objectKey": "test/key.txt",
        })

        assert location.bucket_name == "test-bucket"
        assert location.object_key == "test/key.txt"


class TestSecurityContext:
    """Tests for SecurityContext model."""

    def test_default_encrypted(self) -> None:
        """Test that is_encrypted defaults to True."""
        context = SecurityContext()

        assert context.is_encrypted is True
        assert context.encryption_algorithm is None
        assert context.kms_key_id is None

    def test_full_context(self) -> None:
        """Test fully populated security context."""
        context = SecurityContext(
            is_encrypted=True,
            encryption_algorithm="AES/GCM/NoPadding",
            kms_key_id="arn:aws:kms:us-west-2:123456789:key/abc-123",
        )

        assert context.is_encrypted is True
        assert context.encryption_algorithm == "AES/GCM/NoPadding"
        assert "abc-123" in context.kms_key_id


class TestFileEvent:
    """Tests for FileEvent model."""

    @pytest.fixture
    def valid_event_data(self) -> dict:
        """Create valid event data dict."""
        return {
            "eventId": str(uuid4()),
            "correlationId": "corr-12345",
            "timestamp": datetime.utcnow().isoformat(),
            "eventType": "FILE_UPLOADED",
            "fileMetadata": {
                "originalFilename": "test.pdf",
                "fileSizeBytes": 2048,
                "mimeType": "application/pdf",
            },
            "storageLocation": {
                "bucketName": "test-bucket",
                "objectKey": "uploads/test.pdf",
            },
            "securityContext": {
                "isEncrypted": True,
                "encryptionAlgorithm": "AES/GCM/NoPadding",
            },
        }

    def test_valid_event(self, valid_event_data: dict) -> None:
        """Test creating valid file event."""
        event = FileEvent.model_validate(valid_event_data)

        assert event.event_type == EventType.FILE_UPLOADED
        assert event.correlation_id == "corr-12345"
        assert event.file_metadata.original_filename == "test.pdf"

    def test_event_immutable(self, valid_event_data: dict) -> None:
        """Test that event is immutable (frozen)."""
        event = FileEvent.model_validate(valid_event_data)

        with pytest.raises(ValidationError):
            event.correlation_id = "new-id"

    def test_with_new_event_type(self, valid_event_data: dict) -> None:
        """Test creating new event with different type."""
        original = FileEvent.model_validate(valid_event_data)
        updated = original.with_new_event_type(EventType.ANALYSIS_COMPLETED)

        assert original.event_type == EventType.FILE_UPLOADED
        assert updated.event_type == EventType.ANALYSIS_COMPLETED
        assert original.event_id == updated.event_id  # Same ID
        assert updated.timestamp >= original.timestamp  # New timestamp

    def test_invalid_event_type(self, valid_event_data: dict) -> None:
        """Test that invalid event type is rejected."""
        valid_event_data["eventType"] = "INVALID_TYPE"

        with pytest.raises(ValidationError):
            FileEvent.model_validate(valid_event_data)

    def test_serialize_to_json(self, valid_event_data: dict) -> None:
        """Test serializing event to JSON with camelCase."""
        event = FileEvent.model_validate(valid_event_data)
        json_dict = event.model_dump(mode="json", by_alias=True)

        assert "eventId" in json_dict
        assert "correlationId" in json_dict
        assert "fileMetadata" in json_dict
        assert "originalFilename" in json_dict["fileMetadata"]


class TestSQSMessageWrapper:
    """Tests for SQS message wrapper."""

    def test_direct_event_extraction(self) -> None:
        """Test extracting FileEvent from direct message body."""
        event_data = {
            "eventId": str(uuid4()),
            "correlationId": "test-123",
            "timestamp": datetime.utcnow().isoformat(),
            "eventType": "FILE_UPLOADED",
            "fileMetadata": {
                "originalFilename": "test.pdf",
                "fileSizeBytes": 1024,
            },
            "storageLocation": {
                "bucketName": "bucket",
                "objectKey": "key",
            },
            "securityContext": {
                "isEncrypted": True,
            },
        }

        import orjson
        wrapper = SQSMessageWrapper(
            message_id="msg-123",
            body=orjson.dumps(event_data).decode(),
        )

        event = wrapper.get_file_event()
        assert event.correlation_id == "test-123"

    def test_sns_wrapped_event_extraction(self) -> None:
        """Test extracting FileEvent from SNS notification wrapper."""
        event_data = {
            "eventId": str(uuid4()),
            "correlationId": "sns-test",
            "timestamp": datetime.utcnow().isoformat(),
            "eventType": "FILE_UPLOADED",
            "fileMetadata": {
                "originalFilename": "sns-test.pdf",
                "fileSizeBytes": 2048,
            },
            "storageLocation": {
                "bucketName": "bucket",
                "objectKey": "key",
            },
            "securityContext": {
                "isEncrypted": True,
            },
        }

        import orjson
        sns_notification = {
            "Type": "Notification",
            "MessageId": "sns-msg-123",
            "TopicArn": "arn:aws:sns:us-west-2:123:topic",
            "Message": orjson.dumps(event_data).decode(),
            "Timestamp": datetime.utcnow().isoformat(),
        }

        wrapper = SQSMessageWrapper(
            message_id="sqs-msg-123",
            body=orjson.dumps(sns_notification).decode(),
        )

        event = wrapper.get_file_event()
        assert event.correlation_id == "sns-test"
        assert event.file_metadata.original_filename == "sns-test.pdf"
