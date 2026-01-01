# =============================================================================
# Domain Events - Pydantic Models
# =============================================================================
"""
Event models matching the FSAMP event.schema.json specification.
These are the core domain events flowing through the system.
"""

from datetime import datetime
from enum import StrEnum
from typing import Annotated
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field


class EventType(StrEnum):
    """Discriminator for the type of event action."""

    FILE_UPLOADED = "FILE_UPLOADED"
    FILE_SCANNED = "FILE_SCANNED"
    ANALYSIS_COMPLETED = "ANALYSIS_COMPLETED"
    PROCESSING_FAILED = "PROCESSING_FAILED"


class FileMetadata(BaseModel):
    """Business metadata regarding the processed file."""

    model_config = ConfigDict(frozen=True, extra="forbid", populate_by_name=True)

    original_filename: Annotated[
        str,
        Field(
            min_length=1,
            max_length=255,
            alias="originalFilename",
            description="Original name of the uploaded file",
        ),
    ]
    file_size_bytes: Annotated[
        int,
        Field(
            ge=0,
            alias="fileSizeBytes",
            description="Size of the file in bytes",
        ),
    ]
    mime_type: Annotated[
        str | None,
        Field(
            default=None,
            alias="mimeType",
            description="MIME type of the file",
            examples=["application/pdf", "image/png"],
        ),
    ] = None


class StorageLocation(BaseModel):
    """
    Pointer to the physical storage location.
    Implements the Claim-Check Pattern for large payloads.
    """

    model_config = ConfigDict(frozen=True, extra="forbid", populate_by_name=True)

    bucket_name: Annotated[
        str,
        Field(
            min_length=1,
            alias="bucketName",
            description="S3 bucket name",
        ),
    ]
    object_key: Annotated[
        str,
        Field(
            min_length=1,
            alias="objectKey",
            description="S3 object key (path)",
        ),
    ]

    @property
    def s3_uri(self) -> str:
        """Return the full S3 URI."""
        return f"s3://{self.bucket_name}/{self.object_key}"


class SecurityContext(BaseModel):
    """
    Cryptographic metadata required for FIPS 140-3 compliance.
    Contains encryption details for the file payload.
    """

    model_config = ConfigDict(frozen=True, extra="forbid", populate_by_name=True)

    is_encrypted: Annotated[
        bool,
        Field(
            default=True,
            alias="isEncrypted",
            description="Whether the file is encrypted",
        ),
    ] = True
    encryption_algorithm: Annotated[
        str | None,
        Field(
            default=None,
            alias="encryptionAlgorithm",
            description="FIPS-compliant algorithm used for payload encryption",
            examples=["AES/GCM/NoPadding", "AES/CBC/PKCS5Padding"],
        ),
    ] = None
    kms_key_id: Annotated[
        str | None,
        Field(
            default=None,
            alias="kmsKeyId",
            description="ARN of the AWS KMS key used for envelope encryption",
        ),
    ] = None


class FileEvent(BaseModel):
    """
    Standard event definition for FSAMP platform file processing flow.
    This is the main event schema used for inter-service communication.
    """

    model_config = ConfigDict(
        frozen=True,
        extra="forbid",
        populate_by_name=True,  # Allow both snake_case and camelCase
    )

    event_id: Annotated[
        UUID,
        Field(
            alias="eventId",
            description="Unique identifier for the event (UUID format)",
        ),
    ]
    correlation_id: Annotated[
        str,
        Field(
            min_length=1,
            alias="correlationId",
            description="Trace ID used to track the request across microservices",
        ),
    ]
    timestamp: Annotated[
        datetime,
        Field(
            description="Event occurrence timestamp (ISO 8601 UTC)",
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
