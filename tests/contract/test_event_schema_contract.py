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


# Path to the shared schema (relative to workspace root)
SCHEMA_PATH = Path(__file__).parent.parent.parent.parent.parent / "fsamp-event-schema" / "event.schema.json"


@pytest.fixture
def event_schema() -> dict:
    """Load the shared event schema."""
    if not SCHEMA_PATH.exists():
        pytest.skip(f"Event schema not found at {SCHEMA_PATH}")
    
    with open(SCHEMA_PATH) as f:
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
        
        event_json = json.loads(event.model_dump_json(by_alias=True))
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
        
        event_json = json.loads(event.model_dump_json(by_alias=True))
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
    """Tests ensuring backwards compatibility with older event versions."""

    def test_optional_correlation_id_for_legacy_events(self) -> None:
        """
        Verify that events without correlationId are still valid.
        This supports backwards compatibility with older producers.
        """
        # Legacy event without correlationId
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
        
        # Should parse without error (correlationId defaults)
        event = FileEvent.model_validate(legacy_event_data)
        assert event.correlation_id is not None  # Should have default

    def test_additional_properties_ignored(self) -> None:
        """Verify that additional unknown properties don't break parsing."""
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
            # Unknown future field - should be ignored
            "futureField": "some value",
            "anotherFutureField": {"nested": "data"},
        }
        
        # Should parse without error
        event = FileEvent.model_validate(event_data)
        assert event.event_type == EventType.FILE_UPLOADED
