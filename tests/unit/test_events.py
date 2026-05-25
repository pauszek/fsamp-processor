from datetime import UTC, datetime
from uuid import uuid4

import orjson
import pytest
from pydantic import ValidationError

from processor.domain.events import (
    SCHEMA_VERSION,
    EventSource,
    EventType,
    FileEvent,
    FileMetadata,
    SecurityContext,
    SQSMessageWrapper,
    StorageLocation,
)

SAMPLE_CHECKSUM = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
SAMPLE_KMS_ARN = "arn:aws:kms:us-west-2:123456789012:key/12345678-1234-1234-1234-123456789012"


def create_valid_file_metadata(**overrides) -> FileMetadata:
    defaults = {
        "original_filename": "document.pdf",
        "file_size_bytes": 1024,
        "mime_type": "application/pdf",
        "checksum_sha256": SAMPLE_CHECKSUM,
    }
    defaults.update(overrides)
    return FileMetadata(**defaults)


def create_valid_security_context(**overrides) -> SecurityContext:
    defaults = {
        "is_encrypted": True,
        "encryption_algorithm": "AES/GCM/NoPadding",
        "kms_key_id": SAMPLE_KMS_ARN,
    }
    defaults.update(overrides)
    return SecurityContext(**defaults)


def create_valid_event_data() -> dict:
    return {
        "schemaVersion": SCHEMA_VERSION,
        "fileId": str(uuid4()),
        "eventId": str(uuid4()),
        "correlationId": str(uuid4()),
        "timestamp": datetime.now(UTC).isoformat(),
        "source": "fsamp-processor",
        "eventType": "FILE_UPLOADED",
        "fileMetadata": {
            "originalFilename": "test.pdf",
            "fileSizeBytes": 2048,
            "mimeType": "application/pdf",
            "checksumSHA256": SAMPLE_CHECKSUM,
        },
        "storageLocation": {
            "bucketName": "fsamp-bucket",
            "objectKey": "uploads/test.pdf",
        },
        "securityContext": {
            "isEncrypted": True,
            "encryptionAlgorithm": "AES/GCM/NoPadding",
            "kmsKeyId": SAMPLE_KMS_ARN,
        },
    }


class TestFileMetadata:
    def test_valid_metadata(self) -> None:
        metadata = create_valid_file_metadata()

        assert metadata.original_filename == "document.pdf"
        assert metadata.file_size_bytes == 1024
        assert metadata.mime_type == "application/pdf"
        assert metadata.checksum_sha256 == SAMPLE_CHECKSUM

    def test_metadata_from_camel_case(self) -> None:
        metadata = FileMetadata.model_validate(
            {
                "originalFilename": "test.txt",
                "fileSizeBytes": 500,
                "mimeType": "text/plain",
                "checksumSHA256": SAMPLE_CHECKSUM,
            }
        )

        assert metadata.original_filename == "test.txt"
        assert metadata.file_size_bytes == 500

    def test_invalid_filename_empty(self) -> None:
        with pytest.raises(ValidationError):
            create_valid_file_metadata(original_filename="")

    def test_invalid_file_size_negative(self) -> None:
        with pytest.raises(ValidationError):
            create_valid_file_metadata(file_size_bytes=-1)

    def test_file_size_max_100mb(self) -> None:
        with pytest.raises(ValidationError):
            create_valid_file_metadata(file_size_bytes=104857601)

    def test_checksum_is_required(self) -> None:
        with pytest.raises(ValidationError) as exc_info:
            FileMetadata(
                original_filename="test.pdf",
                file_size_bytes=1024,
                mime_type="application/pdf",
            )
        assert "checksum" in str(exc_info.value).lower()

    def test_invalid_checksum_format(self) -> None:
        with pytest.raises(ValidationError):
            create_valid_file_metadata(checksum_sha256="not-a-valid-sha256")


class TestStorageLocation:
    def test_valid_location(self) -> None:
        location = StorageLocation(
            bucket_name="my-bucket",
            object_key="path/to/file.pdf",
        )

        assert location.bucket_name == "my-bucket"
        assert location.object_key == "path/to/file.pdf"

    def test_s3_uri_property(self) -> None:
        location = StorageLocation(
            bucket_name="fsamp-files",
            object_key="uploads/2024/doc.pdf",
        )

        assert location.s3_uri == "s3://fsamp-files/uploads/2024/doc.pdf"

    def test_location_from_camel_case(self) -> None:
        location = StorageLocation.model_validate(
            {
                "bucketName": "fsamp-bucket",
                "objectKey": "test/key.txt",
            }
        )

        assert location.bucket_name == "fsamp-bucket"
        assert location.object_key == "test/key.txt"


class TestSecurityContext:
    def test_full_context(self) -> None:
        context = create_valid_security_context()

        assert context.is_encrypted is True
        assert context.encryption_algorithm == "AES/GCM/NoPadding"
        assert context.kms_key_id == SAMPLE_KMS_ARN

    def test_encryption_must_be_true(self) -> None:
        with pytest.raises(ValidationError):
            SecurityContext(
                is_encrypted=False,  # Not allowed
                encryption_algorithm="AES/GCM/NoPadding",
                kms_key_id=SAMPLE_KMS_ARN,
            )

    def test_only_aes_gcm_allowed(self) -> None:
        with pytest.raises(ValidationError):
            SecurityContext(
                is_encrypted=True,
                encryption_algorithm="AES/CBC/PKCS5Padding",  # Not allowed
                kms_key_id=SAMPLE_KMS_ARN,
            )

    def test_kms_key_required(self) -> None:
        with pytest.raises(ValidationError) as exc_info:
            SecurityContext(
                is_encrypted=True,
                encryption_algorithm="AES/GCM/NoPadding",
            )
        assert "kms" in str(exc_info.value).lower()

    def test_kms_arn_format_validated(self) -> None:
        with pytest.raises(ValidationError):
            SecurityContext(
                is_encrypted=True,
                encryption_algorithm="AES/GCM/NoPadding",
                kms_key_id="invalid-arn",
            )


class TestFileEvent:
    def test_valid_event(self) -> None:
        event_data = create_valid_event_data()
        event = FileEvent.model_validate(event_data)

        assert event.schema_version == SCHEMA_VERSION
        assert event.event_type == EventType.FILE_UPLOADED
        assert event.source == EventSource.PROCESSOR
        assert event.file_metadata.original_filename == "test.pdf"

    def test_event_immutable(self) -> None:
        event_data = create_valid_event_data()
        event = FileEvent.model_validate(event_data)

        with pytest.raises(ValidationError):
            event.event_type = EventType.FILE_SCANNED

    def test_with_new_event_type(self) -> None:
        event_data = create_valid_event_data()
        original = FileEvent.model_validate(event_data)
        updated = original.with_new_event_type(EventType.ANALYSIS_COMPLETED)

        assert original.event_type == EventType.FILE_UPLOADED
        assert updated.event_type == EventType.ANALYSIS_COMPLETED
        assert original.event_id == updated.event_id  # Same ID
        original_ts = (
            original.timestamp.replace(tzinfo=UTC)
            if original.timestamp.tzinfo is None
            else original.timestamp
        )
        updated_ts = (
            updated.timestamp.replace(tzinfo=UTC)
            if updated.timestamp.tzinfo is None
            else updated.timestamp
        )
        assert updated_ts >= original_ts  # New timestamp
        assert updated.source == EventSource.PROCESSOR  # Updated source

    def test_invalid_event_type(self) -> None:
        event_data = create_valid_event_data()
        event_data["eventType"] = "INVALID_TYPE"

        with pytest.raises(ValidationError):
            FileEvent.model_validate(event_data)

    def test_serialize_to_json(self) -> None:
        event_data = create_valid_event_data()
        event = FileEvent.model_validate(event_data)
        json_dict = event.model_dump(mode="json", by_alias=True)

        assert "schemaVersion" in json_dict
        assert "fileId" in json_dict
        assert "eventId" in json_dict
        assert "correlationId" in json_dict
        assert "source" in json_dict
        assert "fileMetadata" in json_dict
        assert "checksumSHA256" in json_dict["fileMetadata"]

    def test_correlation_id_must_be_uuid(self) -> None:
        event_data = create_valid_event_data()
        event_data["correlationId"] = "not-a-uuid"

        with pytest.raises(ValidationError) as exc_info:
            FileEvent.model_validate(event_data)
        assert "correlation" in str(exc_info.value).lower()

    def test_source_must_be_valid(self) -> None:
        event_data = create_valid_event_data()
        event_data["source"] = "invalid-source"

        with pytest.raises(ValidationError):
            FileEvent.model_validate(event_data)


class TestSQSMessageWrapper:
    def test_direct_event_extraction(self) -> None:
        event_data = create_valid_event_data()

        wrapper = SQSMessageWrapper(
            message_id="msg-123",
            body=orjson.dumps(event_data).decode(),
        )

        event = wrapper.get_file_event()
        assert event.schema_version == SCHEMA_VERSION
        assert event.file_metadata.checksum_sha256 == SAMPLE_CHECKSUM

    def test_sns_wrapped_event_extraction(self) -> None:
        event_data = create_valid_event_data()

        sns_notification = {
            "Type": "Notification",
            "MessageId": "sns-msg-123",
            "TopicArn": "arn:aws:sns:us-west-2:123:topic",
            "Message": orjson.dumps(event_data).decode(),
            "Timestamp": datetime.now(UTC).isoformat(),
        }

        wrapper = SQSMessageWrapper(
            message_id="sqs-msg-123",
            body=orjson.dumps(sns_notification).decode(),
        )

        event = wrapper.get_file_event()
        assert event.schema_version == SCHEMA_VERSION
        assert event.source == EventSource.PROCESSOR
