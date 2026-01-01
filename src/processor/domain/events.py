# =============================================================================
# Domain Events - Pydantic Models (Schema v1.0.0)
# =============================================================================
"""
Event models matching the FSAMP event.schema.json specification v1.0.0.
These are the core domain events flowing through the system.

FIPS 140-3 Compliance:
- Only AES-256-GCM encryption allowed (NIST SP 800-38D)
- SHA-256 checksums required (FIPS 180-4)
- All files must be encrypted
"""

from datetime import datetime
from enum import StrEnum
from typing import Annotated, Literal
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field


# Schema version for forward compatibility
SCHEMA_VERSION = "1.0.0"


class EventType(StrEnum):
    """Discriminator for the type of event action."""

    FILE_UPLOADED = "FILE_UPLOADED"
    FILE_SCANNED = "FILE_SCANNED"
    ANALYSIS_COMPLETED = "ANALYSIS_COMPLETED"
    PROCESSING_FAILED = "PROCESSING_FAILED"


class EventSource(StrEnum):
    """Identifier of the service that produced this event."""

    GATEWAY = "fsamp-gateway"
    PROCESSOR = "fsamp-processor"


class FileMetadata(BaseModel):
    """Business metadata regarding the processed file."""

    model_config = ConfigDict(frozen=True, extra="forbid", populate_by_name=True)

    original_filename: Annotated[
        str,
        Field(
            min_length=1,
            max_length=255,
            pattern=r"^[^<>:\"/\\|?*\x00-\x1f]+$",
            alias="originalFilename",
            description="Original name of the uploaded file",
        ),
    ]
    file_size_bytes: Annotated[
        int,
        Field(
            ge=0,
            le=104857600,  # 100MB max
            alias="fileSizeBytes",
            description="Size of the file in bytes (max 100MB)",
        ),
    ]
    mime_type: Annotated[
        str | None,
        Field(
            default=None,
            pattern=r"^[a-z]+/[a-z0-9.+-]+$",
            alias="mimeType",
            description="MIME type of the file",
        ),
    ] = None
    checksum_sha256: Annotated[
        str,
        Field(
            pattern=r"^[a-f0-9]{64}$",
            alias="checksumSHA256",
            description="SHA-256 hash of file content (FIPS 180-4)",
        ),
    ]


class StorageLocation(BaseModel):
    """
    Pointer to the physical storage location.
    Implements the Claim-Check Pattern for large payloads.
    """

    model_config = ConfigDict(frozen=True, extra="forbid", populate_by_name=True)

    bucket_name: Annotated[
        str,
        Field(
            min_length=3,
            max_length=63,
            pattern=r"^[a-z0-9][a-z0-9.-]*[a-z0-9]$",
            alias="bucketName",
            description="S3 bucket name",
        ),
    ]
    object_key: Annotated[
        str,
        Field(
            min_length=1,
            max_length=1024,
            alias="objectKey",
            description="S3 object key (path)",
        ),
    ]
    region: Annotated[
        str | None,
        Field(
            default=None,
            pattern=r"^[a-z]{2}-[a-z]+-\d$",
            description="AWS region where the bucket is located",
        ),
    ] = None

    @property
    def s3_uri(self) -> str:
        """Return the full S3 URI."""
        return f"s3://{self.bucket_name}/{self.object_key}"


class SecurityContext(BaseModel):
    """
    Cryptographic metadata required for FIPS 140-3 compliance.
    
    Only AES-256-GCM is permitted per NIST SP 800-38D.
    All files MUST be encrypted - unencrypted files are rejected.
    """

    model_config = ConfigDict(frozen=True, extra="forbid", populate_by_name=True)

    is_encrypted: Annotated[
        Literal[True],  # Must always be True - FIPS requirement
        Field(
            alias="isEncrypted",
            description="Whether the file is encrypted (must be true)",
        ),
    ]
    encryption_algorithm: Annotated[
        Literal["AES/GCM/NoPadding"],  # Only AES-GCM allowed
        Field(
            alias="encryptionAlgorithm",
            description="FIPS 140-3 compliant algorithm (AES-256-GCM only)",
        ),
    ]
    kms_key_id: Annotated[
        str,
        Field(
            pattern=r"^arn:aws:kms:[a-z0-9-]+:\d{12}:key/[a-f0-9-]{36}$",
            alias="kmsKeyId",
            description="ARN of the AWS KMS key for envelope encryption",
        ),
    ]


class FileEvent(BaseModel):
    """
    Standard event definition for FSAMP platform file processing flow.
    Schema version: 1.0.0
    
    This is the main event schema used for inter-service communication.
    Compliant with FIPS 140-3 cryptographic requirements.
    """

    model_config = ConfigDict(
        frozen=True,
        extra="forbid",
        populate_by_name=True,  # Allow both snake_case and camelCase
    )

    schema_version: Annotated[
        Literal["1.0.0"],
        Field(
            alias="schemaVersion",
            description="Schema version for forward compatibility",
        ),
    ]
    event_id: Annotated[
        UUID,
        Field(
            alias="eventId",
            description="Unique identifier for the event (UUID v4)",
        ),
    ]
    correlation_id: Annotated[
        UUID,
        Field(
            alias="correlationId",
            description="Trace ID for request tracking (UUID v4)",
        ),
    ]
    timestamp: Annotated[
        datetime,
        Field(
            description="Event occurrence timestamp (ISO 8601 UTC)",
        ),
    ]
    source: Annotated[
        EventSource,
        Field(
            description="Service that produced this event",
        ),
    ]
    event_type: Annotated[
        EventType,
        Field(
            alias="eventType",
            description="Discriminator for the type of event action",
        ),
    ]
    file_metadata: Annotated[
        FileMetadata,
        Field(
            alias="fileMetadata",
            description="Business metadata regarding the processed file",
        ),
    ]
    storage_location: Annotated[
        StorageLocation,
        Field(
            alias="storageLocation",
            description="Pointer to the physical storage location (Claim-Check Pattern)",
        ),
    ]
    security_context: Annotated[
        SecurityContext,
        Field(
            alias="securityContext",
            description="Cryptographic metadata required for FIPS 140-3 compliance",
        ),
    ]

    def with_new_event_type(self, event_type: EventType) -> "FileEvent":
        """Create a new event with updated event type (immutable pattern)."""
        return self.model_copy(
            update={
                "event_type": event_type,
                "timestamp": datetime.utcnow(),
                "source": EventSource.PROCESSOR,
            }
        )


class SQSMessageWrapper(BaseModel):
    """
    Wrapper for SQS message containing the actual event.
    SNS wraps the message when publishing to SQS.
    """

    model_config = ConfigDict(extra="ignore", populate_by_name=True)

    message_id: Annotated[str, Field(alias="MessageId")]
    receipt_handle: str | None = None
    body: str  # JSON string containing FileEvent or SNS notification
    attributes: dict[str, str] = Field(default_factory=dict)
    message_attributes: dict[str, dict[str, str]] = Field(
        default_factory=dict, alias="MessageAttributes"
    )

    def get_file_event(self) -> FileEvent:
        """
        Extract FileEvent from SQS message body.
        Handles both direct events and SNS-wrapped events.
        """
        import orjson

        body_data = orjson.loads(self.body)

        # Check if this is an SNS notification wrapper
        if "Type" in body_data and body_data.get("Type") == "Notification":
            # SNS wraps the actual message in 'Message' field as JSON string
            event_data = orjson.loads(body_data["Message"])
        else:
            # Direct event (for testing or direct SQS publishing)
            event_data = body_data

        return FileEvent.model_validate(event_data)
