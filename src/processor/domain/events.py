"""
Event models matching the FSAMP event.schema.json specification v1.2.0.
These are the core domain events flowing through the system.

FIPS 140-3-oriented constraints:
- Only AES-256-GCM encryption allowed (NIST SP 800-38D)
- SHA-256 checksums required (FIPS 180-4)
- All files must be encrypted
"""

from __future__ import annotations

from datetime import UTC, datetime
from enum import StrEnum
from typing import Annotated, Any, Literal, Self
from uuid import NAMESPACE_URL, UUID, uuid5

import orjson
from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

from processor.domain.exceptions import EventValidationError

SCHEMA_VERSION = "1.2.0"


def redact_filename_for_logs(original: str | None) -> str:
    """Return a privacy-safe filename representation for logs."""
    if original is None or not original.strip():
        return "<unknown>"

    dot = original.rfind(".")
    extension = original[dot:] if 0 < dot < len(original) - 1 else ""
    return f"<redacted len={len(original)} ext={extension}>"


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
            ge=1,
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

    @property
    def redacted_filename(self) -> str:
        """Privacy-safe filename representation for logs and audit traces."""
        return redact_filename_for_logs(self.original_filename)


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
            pattern=r"^[a-z0-9][a-z0-9.-]{1,61}[a-z0-9]$",
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
            pattern=r"^[a-z]{2}(?:-[a-z0-9]+)+-\d$",
            description="AWS region where the bucket is located",
        ),
    ] = None

    @field_validator("bucket_name")
    @classmethod
    def validate_bucket_name(cls, value: str) -> str:
        labels = value.split(".")
        if ".." in value or any(label.startswith("-") or label.endswith("-") for label in labels):
            raise ValueError("bucketName contains an invalid DNS label")
        if len(labels) == 4 and all(label.isdigit() and int(label) <= 255 for label in labels):
            raise ValueError("bucketName must not be formatted as an IPv4 address")
        return value

    @property
    def s3_uri(self) -> str:
        """Return the full S3 URI."""
        return f"s3://{self.bucket_name}/{self.object_key}"


class SecurityContext(BaseModel):
    """
    Cryptographic metadata required by the FIPS 140-3-oriented posture.

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
            description="FIPS 140-3-oriented algorithm constraint (AES-256-GCM only)",
        ),
    ]
    kms_key_id: Annotated[
        str,
        Field(
            pattern=(
                r"^arn:(aws|aws-us-gov):kms:[a-z0-9-]+:\d{12}:key/"
                r"(?:[a-f0-9]{8}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{12}"
                r"|mrk-[a-f0-9]{32})$"
            ),
            alias="kmsKeyId",
            description="ARN of the AWS KMS key for envelope encryption",
        ),
    ]


class ProcessingResultDetails(BaseModel):
    """Canonical result carried by ``ANALYSIS_COMPLETED`` events."""

    model_config = ConfigDict(frozen=True, extra="forbid", populate_by_name=True)

    is_safe: Annotated[bool, Field(alias="isSafe")]
    findings: Annotated[list[str], Field(default_factory=list, max_length=100)]
    processed_at: Annotated[datetime, Field(alias="processedAt")]
    file_hash_sha256: Annotated[
        str | None,
        Field(default=None, pattern=r"^[a-f0-9]{64}$", alias="fileHashSHA256"),
    ] = None
    scan_engine: Annotated[
        str | None,
        Field(default=None, min_length=1, max_length=100, alias="scanEngine"),
    ] = None

    @field_validator("findings")
    @classmethod
    def validate_findings(cls, findings: list[str]) -> list[str]:
        if any(not finding or len(finding) > 1000 for finding in findings):
            raise ValueError("findings must contain non-empty strings no longer than 1000 chars")
        return findings

    @field_validator("processed_at")
    @classmethod
    def validate_processed_at(cls, value: datetime) -> datetime:
        return _require_utc(value, "processedAt")


class FailureDetails(BaseModel):
    """Canonical terminal/retryable failure details."""

    model_config = ConfigDict(frozen=True, extra="forbid", populate_by_name=True)

    code: Annotated[str, Field(min_length=1, max_length=100)]
    message: Annotated[str, Field(min_length=1, max_length=2000)]
    failed_at: Annotated[datetime, Field(alias="failedAt")]
    retryable: bool

    @field_validator("failed_at")
    @classmethod
    def validate_failed_at(cls, value: datetime) -> datetime:
        return _require_utc(value, "failedAt")


def _require_utc(value: datetime, field_name: str) -> datetime:
    """Reject naive/non-UTC timestamps and normalize the UTC representation."""
    offset = value.utcoffset()
    if value.tzinfo is None or offset is None:
        raise ValueError(f"{field_name} must be timezone-aware UTC")
    if offset.total_seconds() != 0:
        raise ValueError(f"{field_name} must use UTC")
    return value.astimezone(UTC)


class FileEvent(BaseModel):
    """
    Standard event definition for FSAMP platform file processing flow.
    Schema version: 1.2.0

    This is the main event schema used for inter-service communication.
    Includes constraints used by the FIPS 140-3-oriented security posture.
    """

    model_config = ConfigDict(
        frozen=True,
        extra="forbid",
        populate_by_name=True,  # Allow both snake_case and camelCase
    )

    schema_version: Annotated[
        Literal["1.2.0"],
        Field(
            alias="schemaVersion",
            description="Schema version for forward compatibility",
        ),
    ]
    file_id: Annotated[
        UUID,
        Field(
            alias="fileId",
            description="Stable aggregate identifier for the file across services (UUID v4)",
        ),
    ]
    event_id: Annotated[
        UUID,
        Field(
            alias="eventId",
            description="Unique identifier for this event occurrence (UUID v4)",
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
            description="Cryptographic metadata required for the FIPS 140-3-oriented posture",
        ),
    ]
    processing_result: Annotated[
        ProcessingResultDetails | None,
        Field(default=None, alias="processingResult"),
    ] = None
    failure: FailureDetails | None = None

    @field_validator("file_id", "correlation_id")
    @classmethod
    def require_uuid4(cls, value: UUID) -> UUID:
        if value.version != 4:
            raise ValueError("identifier must be a UUID v4")
        return value

    @field_validator("event_id")
    @classmethod
    def require_event_uuid(cls, value: UUID) -> UUID:
        if value.version not in {4, 5}:
            raise ValueError("eventId must be a UUID v4 or v5")
        return value

    @field_validator("timestamp")
    @classmethod
    def validate_timestamp(cls, value: datetime) -> datetime:
        return _require_utc(value, "timestamp")

    @model_validator(mode="after")
    def validate_event_variant(self) -> Self:
        expected_source = (
            EventSource.GATEWAY
            if self.event_type == EventType.FILE_UPLOADED
            else EventSource.PROCESSOR
        )
        if self.source != expected_source:
            raise ValueError(f"{self.event_type.value} must be produced by {expected_source.value}")

        if self.event_type == EventType.ANALYSIS_COMPLETED:
            if self.processing_result is None or self.failure is not None:
                raise ValueError("ANALYSIS_COMPLETED requires processingResult and forbids failure")
        elif self.event_type == EventType.PROCESSING_FAILED:
            if self.failure is None or self.processing_result is not None:
                raise ValueError("PROCESSING_FAILED requires failure and forbids processingResult")
        elif self.processing_result is not None or self.failure is not None:
            raise ValueError(f"{self.event_type.value} forbids processingResult and failure")
        return self

    @property
    def event_id_str(self) -> str:
        """String representation of event_id for serialization boundaries."""
        return str(self.event_id)

    @property
    def file_id_str(self) -> str:
        """String representation of file_id for serialization boundaries."""
        return str(self.file_id)

    @property
    def correlation_id_str(self) -> str:
        """String representation of correlation_id for serialization boundaries."""
        return str(self.correlation_id)

    def with_new_event_type(
        self,
        event_type: EventType,
        *,
        processing_result: ProcessingResultDetails | None = None,
        failure: FailureDetails | None = None,
        storage_location: StorageLocation | None = None,
        idempotency_discriminator: str | None = None,
    ) -> FileEvent:
        """Create a validated output event with a stable UUIDv5 id.

        A deterministic occurrence id makes a replay of the same input event
        observable as the same output event, which is required for idempotent
        at-least-once processing.
        """
        event_occurrence = f"urn:fsamp:event:{self.event_id_str}:{event_type.value}"
        if idempotency_discriminator is not None:
            event_occurrence += f":{idempotency_discriminator}"
        output_event_id = uuid5(NAMESPACE_URL, event_occurrence)
        data: dict[str, Any] = self.model_dump()
        data.update(
            {
                "event_id": output_event_id,
                "event_type": event_type,
                "timestamp": datetime.now(UTC),
                "source": (
                    EventSource.GATEWAY
                    if event_type == EventType.FILE_UPLOADED
                    else EventSource.PROCESSOR
                ),
                "processing_result": processing_result,
                "failure": failure,
                "storage_location": storage_location or self.storage_location,
            }
        )
        return FileEvent.model_validate(data)


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
        try:
            body_data = orjson.loads(self.body)

            if "Type" in body_data and body_data.get("Type") == "Notification":
                event_data = orjson.loads(body_data["Message"])
            else:
                event_data = body_data

            return FileEvent.model_validate(event_data)
        except Exception as error:
            raise EventValidationError(
                message="SQS body is not a valid canonical FSAMP event",
                event_id=self.message_id,
                cause=error,
            ) from error
