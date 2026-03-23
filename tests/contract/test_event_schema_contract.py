# =============================================================================
# Contract Tests - Event Schema Compliance (v1.0.0)
# =============================================================================
"""
Contract tests ensuring domain events comply with the shared JSON Schema.

These tests serve as a contract between:
- fsamp-gateway (Java producer)
- fsamp-processor (Python consumer)

Both services validate against the same schema from fsamp-event-schema repo.

Schema Version: 1.0.0
FIPS 140-3 Compliance: Required
"""

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

# Try to import jsonschema - it's optional but recommended
try:
    from jsonschema import Draft7Validator, validate

    HAS_JSONSCHEMA = True
except ImportError:
    HAS_JSONSCHEMA = False


# Schema location paths (in order of preference)
PROJECT_ROOT = Path(__file__).parent.parent.parent
SCHEMA_PATHS = [
    PROJECT_ROOT / "schema" / "event.schema.json",  # Downloaded schema (CI & local)
    PROJECT_ROOT.parent / "fsamp-event-schema" / "event.schema.json",  # Sibling repo
]


def _find_schema_path() -> Path | None:
    """Find the first available schema path."""
    for path in SCHEMA_PATHS:
        if path.exists():
            return path
    return None


@pytest.fixture
def event_schema() -> dict:
    """Load the shared event schema v1.0.0."""
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
    """Create a valid FileEvent conforming to schema v1.0.0."""
    return FileEvent(
        schema_version=SCHEMA_VERSION,
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
    """Create a valid sample FileEvent for testing."""
    return _create_valid_event()


@pytest.mark.skipif(not HAS_JSONSCHEMA, reason="jsonschema not installed")
class TestEventSchemaContract:
    """Contract tests for event schema compliance v1.0.0."""

    def test_schema_is_valid_json_schema(self, event_schema: dict) -> None:
        """Verify the schema itself is valid JSON Schema Draft 7."""
        Draft7Validator.check_schema(event_schema)

    def test_schema_version_matches(self, event_schema: dict) -> None:
        """Verify schema version is 1.0.0."""
        schema_version = event_schema["properties"]["schemaVersion"]["const"]
        assert (
            schema_version == SCHEMA_VERSION
        ), f"Schema version mismatch. Expected: {SCHEMA_VERSION}, Got: {schema_version}"

    def test_domain_event_matches_schema(
        self, event_schema: dict, sample_file_event: FileEvent
    ) -> None:
        """
        Verify that our domain FileEvent model produces JSON
        that validates against the shared schema.

        This is the critical contract test - ensuring our Pydantic
        model produces compatible output with Java Gateway.
        """
        event_json = json.loads(sample_file_event.model_dump_json(by_alias=True, exclude_none=True))
        validate(instance=event_json, schema=event_schema)

    def test_all_event_types_are_valid(self, event_schema: dict) -> None:
        """Verify all EventType enum values match schema enum."""
        schema_event_types = set(event_schema["properties"]["eventType"]["enum"])
        domain_event_types = {e.value for e in EventType}

        missing = schema_event_types - domain_event_types
        assert not missing, f"Domain missing event types from schema: {missing}"

    def test_required_fields_present(self, event_schema: dict) -> None:
        """Verify required fields in schema v1.0.0 match our domain model."""
        required_fields = set(event_schema.get("required", []))
        expected_required = {
            "schemaVersion",
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
        """Verify source field only allows valid services."""
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
        """Test each event type produces valid JSON."""
        event = _create_valid_event(event_type=event_type)
        event_json = json.loads(event.model_dump_json(by_alias=True, exclude_none=True))
        validate(instance=event_json, schema=event_schema)
        assert event_json["eventType"] == event_type.value


class TestFIPSCompliance:
    """Tests ensuring FIPS 140-3 compliance in schema."""

    def test_encryption_is_mandatory(self, event_schema: dict) -> None:
        """Verify isEncrypted must always be true (FIPS requirement)."""
        is_encrypted = event_schema["properties"]["securityContext"]["properties"]["isEncrypted"]
        assert (
            is_encrypted.get("const") is True
        ), "isEncrypted must be const: true for FIPS compliance"

    def test_only_aes_gcm_allowed(self, event_schema: dict) -> None:
        """Verify only AES-256-GCM is allowed (NIST SP 800-38D)."""
        encryption_alg = event_schema["properties"]["securityContext"]["properties"][
            "encryptionAlgorithm"
        ]
        assert (
            encryption_alg.get("const") == "AES/GCM/NoPadding"
        ), "Only AES/GCM/NoPadding should be allowed for FIPS 140-3"

    def test_kms_key_required(self, event_schema: dict) -> None:
        """Verify KMS key is required for envelope encryption."""
        security_required = event_schema["properties"]["securityContext"].get("required", [])
        assert "kmsKeyId" in security_required, "kmsKeyId must be required"
        assert "isEncrypted" in security_required, "isEncrypted must be required"
        assert "encryptionAlgorithm" in security_required, "encryptionAlgorithm must be required"

    def test_checksum_sha256_required(self, event_schema: dict) -> None:
        """Verify SHA-256 checksum is required (FIPS 180-4)."""
        file_metadata_required = event_schema["properties"]["fileMetadata"].get("required", [])
        assert (
            "checksumSHA256" in file_metadata_required
        ), "checksumSHA256 must be required for FIPS 180-4 compliance"

    def test_checksum_format_is_valid(self, sample_file_event: FileEvent) -> None:
        """Verify checksum is valid SHA-256 format (64 hex chars)."""
        checksum = sample_file_event.file_metadata.checksum_sha256
        assert len(checksum) == 64, "SHA-256 must be 64 characters"
        assert all(c in "0123456789abcdef" for c in checksum), "SHA-256 must be lowercase hex"


class TestBackwardsCompatibility:
    """Tests for strict validation - no backwards compatibility for security."""

    def test_correlation_id_must_be_uuid(self) -> None:
        """Verify that correlationId must be UUID (schema v1.0.0 change)."""
        from pydantic import ValidationError

        # Old string format - should fail at construction
        with pytest.raises(ValidationError) as exc_info:
            FileEvent(
                schema_version=SCHEMA_VERSION,
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

    def test_additional_properties_rejected(self) -> None:
        """Verify that additional unknown properties are rejected (strict mode)."""
        from pydantic import ValidationError

        event_data = {
            "schemaVersion": SCHEMA_VERSION,
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
        """Verify that unencrypted files are rejected (FIPS requirement)."""
        from pydantic import ValidationError

        with pytest.raises(ValidationError):
            SecurityContext(
                is_encrypted=False,  # Not allowed
                encryption_algorithm="AES/GCM/NoPadding",
                kms_key_id="arn:aws:kms:us-west-2:123456789012:key/12345678-1234-1234-1234-123456789012",
            )

    def test_aes_cbc_rejected(self) -> None:
        """Verify that AES-CBC is rejected (not FIPS 140-3 recommended)."""
        from pydantic import ValidationError

        with pytest.raises(ValidationError):
            SecurityContext(
                is_encrypted=True,
                encryption_algorithm="AES/CBC/PKCS5Padding",  # Not allowed
                kms_key_id="arn:aws:kms:us-west-2:123456789012:key/12345678-1234-1234-1234-123456789012",
            )

    def test_file_size_max_100mb(self) -> None:
        """Verify file size is capped at 100MB."""
        from pydantic import ValidationError

        with pytest.raises(ValidationError):
            FileMetadata(
                original_filename="huge.pdf",
                file_size_bytes=104857601,  # 100MB + 1 byte
                mime_type="application/pdf",
                checksum_sha256="e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855",
            )


class TestSchemaValidation:
    """Negative tests - invalid data should be rejected."""

    @pytest.mark.skipif(not HAS_JSONSCHEMA, reason="jsonschema not installed")
    def test_invalid_event_type_rejected(self, event_schema: dict) -> None:
        """Verify invalid event type is rejected by schema."""
        from jsonschema import ValidationError as JsonSchemaError

        invalid_json = {
            "schemaVersion": "1.0.0",
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
        """Verify missing source field is rejected by schema."""
        from jsonschema import ValidationError as JsonSchemaError

        invalid_json = {
            "schemaVersion": "1.0.0",
            "eventId": str(uuid4()),
            "correlationId": str(uuid4()),
            "timestamp": datetime.now(UTC).isoformat(),
            # "source" is missing
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
