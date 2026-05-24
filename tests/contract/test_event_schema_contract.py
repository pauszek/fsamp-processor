import json
from datetime import UTC, datetime
from pathlib import Path
from uuid import uuid4

import pytest

from processor.domain.events import (
    SCHEMA_VERSION,
    EventSource,
    EventType,
    FileEvent,
    FileMetadata,
    SecurityContext,
    StorageLocation,
)

try:
    from jsonschema import Draft7Validator, validate

    HAS_JSONSCHEMA = True
except ImportError:
    HAS_JSONSCHEMA = False


PROJECT_ROOT = Path(__file__).parent.parent.parent
SCHEMA_PATHS = [
    PROJECT_ROOT / "schema" / "event.schema.json",  # Downloaded schema (CI & local)
    PROJECT_ROOT.parent / "fsamp-event-schema" / "event.schema.json",  # Sibling repo
]


def _find_schema_path() -> Path | None:
    for path in SCHEMA_PATHS:
        if path.exists():
            return path
    return None


@pytest.fixture
def event_schema() -> dict:
    schema_path = _find_schema_path()
    if schema_path is None:
        pytest.skip(
            "Event schema not found. Run './scripts/download-schema.sh' or ensure "
            "fsamp-event-schema repo is available as sibling directory."
        )

    with open(schema_path) as f:
        return json.load(f)


def _create_valid_event(
    event_type: EventType = EventType.FILE_UPLOADED,
    source: EventSource = EventSource.PROCESSOR,
) -> FileEvent:
    return FileEvent(
        schema_version=SCHEMA_VERSION,
        file_id=uuid4(),
        event_id=uuid4(),
        correlation_id=uuid4(),
        timestamp=datetime.now(UTC),
        source=source,
        event_type=event_type,
        file_metadata=FileMetadata(
            original_filename="document.pdf",
            file_size_bytes=1024000,
            mime_type="application/pdf",
            checksum_sha256="e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855",
        ),
        storage_location=StorageLocation(
            bucket_name="fsamp-files-dev",
            object_key="uploads/2026/01/abc123.pdf",
        ),
        security_context=SecurityContext(
            is_encrypted=True,
            encryption_algorithm="AES/GCM/NoPadding",
            kms_key_id="arn:aws:kms:us-west-2:123456789012:key/12345678-1234-1234-1234-123456789012",
        ),
    )


@pytest.fixture
def sample_file_event() -> FileEvent:
    return _create_valid_event()


@pytest.mark.skipif(not HAS_JSONSCHEMA, reason="jsonschema not installed")
class TestEventSchemaContract:
    def test_schema_is_valid_json_schema(self, event_schema: dict) -> None:
        Draft7Validator.check_schema(event_schema)

    def test_schema_version_matches(self, event_schema: dict) -> None:
        schema_version = event_schema["properties"]["schemaVersion"]["const"]
        assert (
            schema_version == SCHEMA_VERSION
        ), f"Schema version mismatch. Expected: {SCHEMA_VERSION}, Got: {schema_version}"

    def test_domain_event_matches_schema(
        self, event_schema: dict, sample_file_event: FileEvent
    ) -> None:
        event_json = json.loads(sample_file_event.model_dump_json(by_alias=True, exclude_none=True))
        validate(instance=event_json, schema=event_schema)

    def test_all_event_types_are_valid(self, event_schema: dict) -> None:
        schema_event_types = set(event_schema["properties"]["eventType"]["enum"])
        domain_event_types = {e.value for e in EventType}

        missing = schema_event_types - domain_event_types
        assert not missing, f"Domain missing event types from schema: {missing}"

    def test_required_fields_present(self, event_schema: dict) -> None:
        required_fields = set(event_schema.get("required", []))
        expected_required = {
            "schemaVersion",
            "fileId",
            "eventId",
            "correlationId",
            "timestamp",
            "source",
            "eventType",
            "fileMetadata",
            "storageLocation",
            "securityContext",
        }

        assert required_fields == expected_required, (
            f"Schema required fields mismatch. "
            f"Expected: {expected_required}, Got: {required_fields}"
        )

    def test_source_must_be_valid_enum(self, event_schema: dict) -> None:
        allowed_sources = set(event_schema["properties"]["source"]["enum"])
        expected_sources = {e.value for e in EventSource}

        assert (
            allowed_sources == expected_sources
        ), f"Source enum mismatch. Schema: {allowed_sources}, Domain: {expected_sources}"

    @pytest.mark.parametrize("event_type", list(EventType))
    def test_all_event_types_produce_valid_json(
        self,
        event_schema: dict,
        event_type: EventType,
    ) -> None:
        event = _create_valid_event(event_type=event_type)
        event_json = json.loads(event.model_dump_json(by_alias=True, exclude_none=True))
        validate(instance=event_json, schema=event_schema)
        assert event_json["eventType"] == event_type.value


class TestFIPSCompliance:
    def test_encryption_is_mandatory(self, event_schema: dict) -> None:
        is_encrypted = event_schema["properties"]["securityContext"]["properties"]["isEncrypted"]
        assert (
            is_encrypted.get("const") is True
        ), "isEncrypted must be const: true for the FIPS-oriented posture"

    def test_only_aes_gcm_allowed(self, event_schema: dict) -> None:
        encryption_alg = event_schema["properties"]["securityContext"]["properties"][
            "encryptionAlgorithm"
        ]
        assert (
            encryption_alg.get("const") == "AES/GCM/NoPadding"
        ), "Only AES/GCM/NoPadding should be allowed for FIPS 140-3"

    def test_kms_key_required(self, event_schema: dict) -> None:
        security_required = event_schema["properties"]["securityContext"].get("required", [])
        assert "kmsKeyId" in security_required, "kmsKeyId must be required"
        assert "isEncrypted" in security_required, "isEncrypted must be required"
        assert "encryptionAlgorithm" in security_required, "encryptionAlgorithm must be required"

    def test_checksum_sha256_required(self, event_schema: dict) -> None:
        file_metadata_required = event_schema["properties"]["fileMetadata"].get("required", [])
        assert (
            "checksumSHA256" in file_metadata_required
        ), "checksumSHA256 must be required for FIPS 180-4 alignment"

    def test_checksum_format_is_valid(self, sample_file_event: FileEvent) -> None:
        checksum = sample_file_event.file_metadata.checksum_sha256
        assert len(checksum) == 64, "SHA-256 must be 64 characters"
        assert all(c in "0123456789abcdef" for c in checksum), "SHA-256 must be lowercase hex"


class TestBackwardsCompatibility:
    def test_correlation_id_must_be_uuid(self) -> None:
        from pydantic import ValidationError

        with pytest.raises(ValidationError) as exc_info:
            FileEvent(
                schema_version=SCHEMA_VERSION,
                file_id=uuid4(),
                event_id=uuid4(),
                correlation_id="not-a-uuid",  # Invalid - must be UUID
                timestamp=datetime.now(UTC),
                source=EventSource.PROCESSOR,
                event_type=EventType.FILE_UPLOADED,
                file_metadata=FileMetadata(
                    original_filename="test.pdf",
                    file_size_bytes=1000,
                    mime_type="application/pdf",
                    checksum_sha256="e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855",
                ),
                storage_location=StorageLocation(
                    bucket_name="fsamp-bucket",
                    object_key="key",
                ),
                security_context=SecurityContext(
                    is_encrypted=True,
                    encryption_algorithm="AES/GCM/NoPadding",
                    kms_key_id="arn:aws:kms:us-west-2:123456789012:key/12345678-1234-1234-1234-123456789012",
                ),
            )

        assert "correlation_id" in str(exc_info.value).lower()

    def test_legacy_event_without_file_id_rejected(self) -> None:
        from pydantic import ValidationError

        event_data = json.loads(
            _create_valid_event().model_dump_json(by_alias=True, exclude_none=True)
        )
        event_data.pop("fileId")

        with pytest.raises(ValidationError) as exc_info:
            FileEvent.model_validate(event_data)

        assert "fileid" in str(exc_info.value).lower() or "file_id" in str(exc_info.value).lower()

    def test_additional_properties_rejected(self) -> None:
        from pydantic import ValidationError

        event_data = {
            "schemaVersion": SCHEMA_VERSION,
            "fileId": str(uuid4()),
            "eventId": str(uuid4()),
            "correlationId": str(uuid4()),
            "timestamp": datetime.now(UTC).isoformat(),
            "source": "fsamp-processor",
            "eventType": "FILE_UPLOADED",
            "fileMetadata": {
                "originalFilename": "test.pdf",
                "fileSizeBytes": 1000,
                "checksumSHA256": "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855",
            },
            "storageLocation": {
                "bucketName": "fsamp-bucket",
                "objectKey": "key",
            },
            "securityContext": {
                "isEncrypted": True,
                "encryptionAlgorithm": "AES/GCM/NoPadding",
                "kmsKeyId": "arn:aws:kms:us-west-2:123456789012:key/12345678-1234-1234-1234-123456789012",
            },
            "unknownField": "should be rejected",
        }

        with pytest.raises(ValidationError) as exc_info:
            FileEvent.model_validate(event_data)

        assert "unknownField" in str(exc_info.value)

    def test_unencrypted_files_rejected(self) -> None:
        from pydantic import ValidationError

        with pytest.raises(ValidationError):
            SecurityContext(
                is_encrypted=False,  # Not allowed
                encryption_algorithm="AES/GCM/NoPadding",
                kms_key_id="arn:aws:kms:us-west-2:123456789012:key/12345678-1234-1234-1234-123456789012",
            )

    def test_aes_cbc_rejected(self) -> None:
        from pydantic import ValidationError

        with pytest.raises(ValidationError):
            SecurityContext(
                is_encrypted=True,
                encryption_algorithm="AES/CBC/PKCS5Padding",  # Not allowed
                kms_key_id="arn:aws:kms:us-west-2:123456789012:key/12345678-1234-1234-1234-123456789012",
            )

    def test_file_size_max_100mb(self) -> None:
        from pydantic import ValidationError

        with pytest.raises(ValidationError):
            FileMetadata(
                original_filename="huge.pdf",
                file_size_bytes=104857601,  # 100MB + 1 byte
                mime_type="application/pdf",
                checksum_sha256="e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855",
            )


class TestSchemaValidation:
    @pytest.mark.skipif(not HAS_JSONSCHEMA, reason="jsonschema not installed")
    def test_invalid_event_type_rejected(self, event_schema: dict) -> None:
        from jsonschema import ValidationError as JsonSchemaError

        invalid_json = {
            "schemaVersion": "1.1.0",
            "fileId": str(uuid4()),
            "eventId": str(uuid4()),
            "correlationId": str(uuid4()),
            "timestamp": datetime.now(UTC).isoformat(),
            "source": "fsamp-processor",
            "eventType": "INVALID_TYPE",  # Not in enum
            "fileMetadata": {
                "originalFilename": "test.pdf",
                "fileSizeBytes": 1000,
                "checksumSHA256": "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855",
            },
            "storageLocation": {
                "bucketName": "fsamp-bucket",
                "objectKey": "key",
            },
            "securityContext": {
                "isEncrypted": True,
                "encryptionAlgorithm": "AES/GCM/NoPadding",
                "kmsKeyId": "arn:aws:kms:us-west-2:123456789012:key/12345678-1234-1234-1234-123456789012",
            },
        }

        with pytest.raises(JsonSchemaError):
            validate(instance=invalid_json, schema=event_schema)

    @pytest.mark.skipif(not HAS_JSONSCHEMA, reason="jsonschema not installed")
    def test_missing_source_rejected(self, event_schema: dict) -> None:
        from jsonschema import ValidationError as JsonSchemaError

        invalid_json = {
            "schemaVersion": "1.1.0",
            "fileId": str(uuid4()),
            "eventId": str(uuid4()),
            "correlationId": str(uuid4()),
            "timestamp": datetime.now(UTC).isoformat(),
            "eventType": "FILE_UPLOADED",
            "fileMetadata": {
                "originalFilename": "test.pdf",
                "fileSizeBytes": 1000,
                "checksumSHA256": "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855",
            },
            "storageLocation": {
                "bucketName": "fsamp-bucket",
                "objectKey": "key",
            },
            "securityContext": {
                "isEncrypted": True,
                "encryptionAlgorithm": "AES/GCM/NoPadding",
                "kmsKeyId": "arn:aws:kms:us-west-2:123456789012:key/12345678-1234-1234-1234-123456789012",
            },
        }

        with pytest.raises(JsonSchemaError):
            validate(instance=invalid_json, schema=event_schema)

    @pytest.mark.skipif(not HAS_JSONSCHEMA, reason="jsonschema not installed")
    def test_missing_file_id_rejected(self, event_schema: dict) -> None:
        from jsonschema import ValidationError as JsonSchemaError

        invalid_json = json.loads(
            _create_valid_event().model_dump_json(by_alias=True, exclude_none=True)
        )
        invalid_json.pop("fileId")

        with pytest.raises(JsonSchemaError):
            validate(instance=invalid_json, schema=event_schema)
