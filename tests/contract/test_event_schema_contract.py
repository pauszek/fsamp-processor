# =============================================================================
# Event Schema Contract Tests
# =============================================================================
"""
Contract tests validating that domain events conform to the shared
fsamp-event-schema JSON Schema definition.

This ensures compatibility between Gateway (producer) and Processor (consumer).

Enterprise Pattern: Contract Testing
- Single source of truth for event schema (fsamp-event-schema repo)
- Both producer and consumer validate against same schema
- Prevents breaking changes in event structure
- Schema version pinned in schema.version file

Usage:
    # Download schema first (CI does this automatically)
    ./scripts/download-schema.sh
    
    # Run contract tests
    pytest tests/contract/
"""

import json
from datetime import datetime, timezone
from pathlib import Path
from uuid import uuid4

import pytest

try:
    import jsonschema
    from jsonschema import Draft7Validator, validate
    HAS_JSONSCHEMA = True
except ImportError:
    HAS_JSONSCHEMA = False

from processor.domain.events import (
    EventType,
    FileEvent,
    FileMetadata,
    SecurityContext,
    StorageLocation,
)


# Schema locations (priority order)
PROJECT_ROOT = Path(__file__).parent.parent.parent
SCHEMA_PATHS = [
    PROJECT_ROOT / "schema" / "event.schema.json",  # Downloaded schema (CI & local)
    PROJECT_ROOT.parent / "fsamp-event-schema" / "event.schema.json",  # Sibling repo (local dev)
]


def _find_schema_path() -> Path | None:
    """Find the first available schema path."""
    for path in SCHEMA_PATHS:
        if path.exists():
            return path
    return None


@pytest.fixture
def event_schema() -> dict:
    """Load the shared event schema."""
    schema_path = _find_schema_path()
    if schema_path is None:
        pytest.skip(
            f"Event schema not found. Run './scripts/download-schema.sh' or ensure "
            f"fsamp-event-schema repo is available as sibling directory."
        )
    
    with open(schema_path) as f:
        return json.load(f)


@pytest.fixture
def sample_file_event() -> FileEvent:
    """Create a valid sample FileEvent."""
    return FileEvent(
        event_id=uuid4(),
        correlation_id="corr-12345",
        timestamp=datetime.now(timezone.utc),
        event_type=EventType.FILE_UPLOADED,
        file_metadata=FileMetadata(
            original_filename="document.pdf",
            file_size_bytes=1024000,
            mime_type="application/pdf",
        ),
        storage_location=StorageLocation(
            bucket_name="fsamp-files-dev",
            object_key="uploads/2026/01/abc123.pdf",
        ),
        security_context=SecurityContext(
            is_encrypted=True,
            encryption_algorithm="AES/GCM/NoPadding",
            kms_key_id="arn:aws:kms:us-west-2:123456789:key/abc-123",
        ),
    )


@pytest.mark.skipif(not HAS_JSONSCHEMA, reason="jsonschema not installed")
class TestEventSchemaContract:
    """Contract tests for event schema compliance."""

    def test_schema_is_valid_json_schema(self, event_schema: dict) -> None:
        """Verify the schema itself is valid JSON Schema Draft 7."""
        # This will raise if schema is invalid
        Draft7Validator.check_schema(event_schema)

    def test_domain_event_matches_schema(
        self, 
        event_schema: dict, 
        sample_file_event: FileEvent
    ) -> None:
        """
        Verify that our domain FileEvent model produces JSON 
        that validates against the shared schema.
        
        This is the critical contract test - ensuring our Pydantic
        model produces compatible output.
        """
        # Export event to JSON (using camelCase aliases for contract)
        event_json = json.loads(sample_file_event.model_dump_json(by_alias=True))
        
        # Validate against schema - raises on failure
        validate(instance=event_json, schema=event_schema)

    def test_all_event_types_are_valid(self, event_schema: dict) -> None:
        """Verify all EventType enum values match schema enum."""
        schema_event_types = set(event_schema["properties"]["eventType"]["enum"])
        domain_event_types = {e.value for e in EventType}
        
        # Domain should have all schema types (can have more for internal use)
        missing = schema_event_types - domain_event_types
        assert not missing, f"Domain missing event types from schema: {missing}"

    def test_required_fields_present(self, event_schema: dict) -> None:
        """Verify required fields in schema match our domain model."""
        required_fields = set(event_schema.get("required", []))
        expected_required = {
            "eventId",
            "correlationId",  # Required for distributed tracing
            "timestamp", 
            "eventType",
            "fileMetadata",
            "storageLocation",
            "securityContext",
        }
        
        assert required_fields == expected_required, (
            f"Schema required fields mismatch. "
            f"Expected: {expected_required}, Got: {required_fields}"
        )

    def test_file_uploaded_event_contract(
        self, 
        event_schema: dict,
    ) -> None:
        """Test FILE_UPLOADED event type contract."""
        event = FileEvent(
            event_id=uuid4(),
            correlation_id="upload-test-123",
            timestamp=datetime.now(timezone.utc),
            event_type=EventType.FILE_UPLOADED,
            file_metadata=FileMetadata(
                original_filename="report.xlsx",
                file_size_bytes=50000,
                mime_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            ),
            storage_location=StorageLocation(
                bucket_name="fsamp-files",
                object_key="uploads/report.xlsx",
            ),
            security_context=SecurityContext(
                is_encrypted=True,
                encryption_algorithm="AES/GCM/NoPadding",
            ),
        )
        
        event_json = json.loads(event.model_dump_json(by_alias=True, exclude_none=True))
        validate(instance=event_json, schema=event_schema)
        
        # Verify specific contract expectations
        assert event_json["eventType"] == "FILE_UPLOADED"
        assert event_json["securityContext"]["isEncrypted"] is True

    def test_processing_failed_event_contract(
        self,
        event_schema: dict,
    ) -> None:
        """Test PROCESSING_FAILED event type contract."""
        event = FileEvent(
            event_id=uuid4(),
            correlation_id="failed-123",
            timestamp=datetime.now(timezone.utc),
            event_type=EventType.PROCESSING_FAILED,
            file_metadata=FileMetadata(
                original_filename="malware.exe",
                file_size_bytes=999999,
                mime_type="application/x-executable",
            ),
            storage_location=StorageLocation(
                bucket_name="fsamp-quarantine",
                object_key="quarantine/malware.exe",
            ),
            security_context=SecurityContext(
                is_encrypted=True,
            ),
        )
        
        event_json = json.loads(event.model_dump_json(by_alias=True, exclude_none=True))
        validate(instance=event_json, schema=event_schema)
        
        assert event_json["eventType"] == "PROCESSING_FAILED"

    def test_encryption_algorithms_match_schema(
        self,
        event_schema: dict,
    ) -> None:
        """Verify encryption algorithms in schema are FIPS-compliant."""
        security_props = event_schema["properties"]["securityContext"]["properties"]
        allowed_algorithms = security_props["encryptionAlgorithm"]["enum"]
        
        # All algorithms should be FIPS 140-3 approved
        fips_approved = {"AES/GCM/NoPadding", "AES/CBC/PKCS5Padding"}
        
        for alg in allowed_algorithms:
            assert alg in fips_approved, f"Non-FIPS algorithm in schema: {alg}"


class TestBackwardsCompatibility:
    """Tests ensuring strict validation for event schema."""

    def test_correlation_id_is_required(self) -> None:
        """
        Verify that events without correlationId are rejected.
        correlationId is required for proper distributed tracing.
        """
        from pydantic import ValidationError
        
        # Event without correlationId - should fail validation
        legacy_event_data = {
            "eventId": str(uuid4()),
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "eventType": "FILE_UPLOADED",
            "fileMetadata": {
                "originalFilename": "legacy.pdf",
                "fileSizeBytes": 1000,
            },
            "storageLocation": {
                "bucketName": "bucket",
                "objectKey": "key",
            },
            "securityContext": {
                "isEncrypted": True,
            },
        }
        
        # Should reject - correlationId is required for FIPS traceability
        with pytest.raises(ValidationError) as exc_info:
            FileEvent.model_validate(legacy_event_data)
        
        assert "correlationId" in str(exc_info.value)

    def test_additional_properties_rejected(self) -> None:
        """Verify that additional unknown properties are rejected (strict mode)."""
        from pydantic import ValidationError
        
        event_data = {
            "eventId": str(uuid4()),
            "correlationId": "test",
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "eventType": "FILE_UPLOADED",
            "fileMetadata": {
                "originalFilename": "test.pdf",
                "fileSizeBytes": 1000,
            },
            "storageLocation": {
                "bucketName": "bucket",
                "objectKey": "key",
            },
            "securityContext": {
                "isEncrypted": True,
            },
            # Unknown field - should be rejected in strict mode
            "unknownField": "some value",
        }
        
        # Should reject - extra="forbid" enforces strict schema
        with pytest.raises(ValidationError) as exc_info:
            FileEvent.model_validate(event_data)
        
        assert "unknownField" in str(exc_info.value)
