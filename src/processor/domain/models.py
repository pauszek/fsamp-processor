"""
Core domain models representing the state and results of file processing.
Implements Outbox Pattern for reliable event publishing.
"""

from __future__ import annotations

import hashlib
import json
import uuid
from dataclasses import dataclass, field
from datetime import UTC, datetime
from enum import StrEnum
from typing import Any

from processor.domain.events import EventType, FileEvent

OUTBOX_SHARD_COUNT = 16
OUTBOX_DEFAULT_RETENTION_SECONDS = 30 * 24 * 60 * 60


def outbox_shard(aggregate_id: str) -> str:
    """Return the stable two-character shard shared by all FSAMP outboxes."""
    digest = hashlib.sha256(aggregate_id.encode("utf-8")).digest()
    return f"{(digest[0] & 0xFF) % OUTBOX_SHARD_COUNT:02x}"


class ProcessingStatus(StrEnum):
    """Status of file processing."""

    PENDING = "PENDING"
    UPLOADED = "UPLOADED"
    SCANNING = "SCANNING"
    PROCESSING = "PROCESSING"
    # Backward-compatible aliases. Persisted values use the shared gateway vocabulary.
    IN_PROGRESS = "PROCESSING"
    COMPLETED = "COMPLETED"
    FAILED = "FAILED"
    RETRYING = "PROCESSING"


class OutboxStatus(StrEnum):
    """Status of outbox event for reliable publishing."""

    PENDING = "PENDING"
    PUBLISHING = "PUBLISHING"
    PUBLISHED = "PUBLISHED"
    FAILED = "FAILED"


class OutboxEventType(StrEnum):
    """Types of events in the outbox."""

    FILE_UPLOADED = "FILE_UPLOADED"
    FILE_SCANNED = "FILE_SCANNED"
    ANALYSIS_COMPLETED = "ANALYSIS_COMPLETED"
    PROCESSING_FAILED = "PROCESSING_FAILED"


@dataclass(frozen=True, slots=True)
class ProcessingClaim:
    """Token-fenced lease for one processor attempt."""

    event_id: str
    token: str
    version: int
    expires_at_epoch: int


@dataclass(frozen=True, slots=True)
class ProcessingResult:
    """
    Result of processing a single file.
    Immutable value object containing processing outcome.
    """

    event_id: str
    correlation_id: str
    status: ProcessingStatus
    started_at: datetime
    completed_at: datetime | None = None
    error_message: str | None = None
    error_code: str | None = None
    retry_count: int = 0
    metadata: dict[str, Any] = field(default_factory=dict)

    @property
    def duration_ms(self) -> int | None:
        """Calculate processing duration in milliseconds."""
        if self.completed_at is None:
            return None
        delta = self.completed_at - self.started_at
        return int(delta.total_seconds() * 1000)

    def with_completion(
        self,
        status: ProcessingStatus,
        error_message: str | None = None,
        error_code: str | None = None,
        metadata: dict[str, Any] | None = None,
    ) -> ProcessingResult:
        """Create a new result with completion data (immutable pattern)."""
        return ProcessingResult(
            event_id=self.event_id,
            correlation_id=self.correlation_id,
            status=status,
            started_at=self.started_at,
            completed_at=datetime.now(UTC),
            error_message=error_message,
            error_code=error_code,
            retry_count=self.retry_count,
            metadata={**self.metadata, **(metadata or {})},
        )


@dataclass(frozen=True, slots=True)
class FileContent:
    """
    Represents downloaded file content with metadata.
    Used after retrieving file from S3.
    """

    data: bytes
    content_type: str | None = None
    content_length: int = 0
    etag: str | None = None
    encryption_algorithm: str | None = None
    kms_key_id: str | None = None

    @property
    def is_encrypted(self) -> bool:
        """Check if file was server-side encrypted."""
        return self.encryption_algorithm is not None


@dataclass(frozen=True, slots=True)
class AnalysisResult:
    """Result of file analysis/scanning."""

    file_hash_sha256: str
    is_safe: bool
    scan_engine: str = "internal"
    findings: list[str] = field(default_factory=list)
    analyzed_at: datetime = field(default_factory=lambda: datetime.now(UTC))
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass(slots=True)
class MetadataRecord:
    """
    Metadata record stored in DynamoDB.
    Represents the persistent state of a processed file.
    """

    file_id: str

    timestamp: str

    correlation_id: str
    original_filename: str
    file_size_bytes: int
    mime_type: str | None
    bucket_name: str
    object_key: str
    status: ProcessingStatus

    checksum_sha256: str | None = None
    file_hash: str | None = None
    is_encrypted: bool = True
    encryption_algorithm: str = "AES/GCM/NoPadding"
    kms_key_id: str | None = None

    is_safe: bool | None = None
    scan_findings: list[str] = field(default_factory=list)

    created_at: str | None = None
    created_by: str | None = None
    updated_at: str | None = None
    processed_at: str | None = None

    error_message: str | None = None
    error_code: str | None = None
    retry_count: int = 0
    last_processed_event_id: str | None = None

    ttl: int | None = None

    def to_dynamodb_item(self) -> dict[str, Any]:
        """Convert to DynamoDB item format."""
        item: dict[str, Any] = {
            "PK": {"S": f"FILE#{self.file_id}"},
            "SK": {"S": "METADATA"},
            "entityType": {"S": "FILE_METADATA"},
            "fileId": {"S": self.file_id},
            "correlationId": {"S": self.correlation_id},
            "originalFilename": {"S": self.original_filename},
            "fileSizeBytes": {"N": str(self.file_size_bytes)},
            "bucketName": {"S": self.bucket_name},
            "objectKey": {"S": self.object_key},
            "status": {"S": self.status.value},
            "isEncrypted": {"BOOL": self.is_encrypted},
            "encryptionAlgorithm": {"S": self.encryption_algorithm},
            "retryCount": {"N": str(self.retry_count)},
        }

        if self.mime_type:
            item["mimeType"] = {"S": self.mime_type}
        if self.checksum_sha256:
            item["checksumSHA256"] = {"S": self.checksum_sha256}
            item["checksumAlgorithm"] = {"S": "SHA256"}
        if self.file_hash:
            item["fileHash"] = {"S": self.file_hash}
        if self.kms_key_id:
            item["kmsKeyId"] = {"S": self.kms_key_id}
        if self.is_safe is not None:
            item["isSafe"] = {"BOOL": self.is_safe}
        if self.scan_findings:
            item["scanFindings"] = {"SS": self.scan_findings}
        if self.created_at:
            item["createdAt"] = {"S": self.created_at}
        if self.created_by:
            item["createdBy"] = {"S": self.created_by}
        if self.updated_at:
            item["updatedAt"] = {"S": self.updated_at}
        if self.processed_at:
            item["processedAt"] = {"S": self.processed_at}
        if self.error_message:
            item["errorMessage"] = {"S": self.error_message}
        if self.error_code:
            item["errorCode"] = {"S": self.error_code}
        if self.ttl:
            item["ttl"] = {"N": str(self.ttl)}
        if self.last_processed_event_id:
            item["lastProcessedEventId"] = {"S": self.last_processed_event_id}

        return item

    @classmethod
    def from_dynamodb_item(cls, item: dict[str, Any]) -> MetadataRecord:
        """Create from DynamoDB item format."""
        pk = item["PK"]["S"]  # FILE#<file_id>
        file_id = item.get("fileId", {}).get("S") or pk.removeprefix("FILE#")
        timestamp = (
            item.get("updatedAt", {}).get("S")
            or item.get("createdAt", {}).get("S")
            or datetime.now(UTC).isoformat()
        )
        original_filename = item.get("originalFilename", {}).get("S") or item.get(
            "fileName", {}
        ).get("S")
        if not original_filename:
            raise ValueError("DynamoDB metadata record is missing originalFilename")

        return cls(
            file_id=file_id,
            timestamp=timestamp,
            correlation_id=item["correlationId"]["S"],
            original_filename=original_filename,
            file_size_bytes=int(item["fileSizeBytes"]["N"]),
            mime_type=item.get("mimeType", {}).get("S"),
            bucket_name=item["bucketName"]["S"],
            object_key=item["objectKey"]["S"],
            status=ProcessingStatus(item["status"]["S"]),
            checksum_sha256=(
                item.get("checksumSHA256", {}).get("S") or item.get("checksum", {}).get("S")
            ),
            file_hash=item.get("fileHash", {}).get("S"),
            is_encrypted=item.get("isEncrypted", {}).get("BOOL", True),
            encryption_algorithm=item.get("encryptionAlgorithm", {}).get("S", "AES/GCM/NoPadding"),
            kms_key_id=item.get("kmsKeyId", {}).get("S"),
            is_safe=item.get("isSafe", {}).get("BOOL"),
            scan_findings=list(item.get("scanFindings", {}).get("SS", [])),
            created_at=item.get("createdAt", {}).get("S"),
            created_by=item.get("createdBy", {}).get("S"),
            updated_at=item.get("updatedAt", {}).get("S"),
            processed_at=item.get("processedAt", {}).get("S"),
            error_message=item.get("errorMessage", {}).get("S"),
            error_code=item.get("errorCode", {}).get("S"),
            retry_count=int(item.get("retryCount", {}).get("N", "0")),
            last_processed_event_id=item.get("lastProcessedEventId", {}).get("S"),
            ttl=int(item["ttl"]["N"]) if "ttl" in item else None,
        )


@dataclass(slots=True)
class OutboxEvent:
    """
    Outbox event for reliable event publishing (Transactional Outbox Pattern).

    The outbox pattern ensures that database writes and event publishing
    are atomic - either both happen or neither happens. Events are first
    written to the outbox table as part of the same transaction as the
    business data, then published asynchronously by a separate process.

    Benefits:
    - At-least-once delivery guarantee
    - Event replay capability
    - Audit trail
    - Decoupled from message broker availability
    """

    event_id: str

    event_type: OutboxEventType

    aggregate_id: str
    aggregate_type: str = "FileProcessing"

    payload: dict[str, Any] = field(default_factory=dict)

    status: OutboxStatus = OutboxStatus.PENDING

    created_at: str = field(default_factory=lambda: datetime.now(UTC).isoformat())
    published_at: str | None = None

    retry_count: int = 0
    last_error: str | None = None

    message_group_id: str | None = None

    ttl: int | None = None

    outbox_partition: str | None = None
    outbox_shard: str | None = None

    def __post_init__(self) -> None:
        if self.outbox_partition is None:
            self.outbox_partition = f"OUTBOX#{self.aggregate_type}#{self.aggregate_id}"
        if self.outbox_shard is None:
            self.outbox_shard = outbox_shard(self.aggregate_id)

    @classmethod
    def create(
        cls,
        event_type: OutboxEventType,
        aggregate_id: str,
        payload: dict[str, Any],
        aggregate_type: str = "FileProcessing",
        message_group_id: str | None = None,
    ) -> OutboxEvent:
        """Factory method to create a new outbox event."""
        return cls(
            event_id=str(uuid.uuid4()),
            event_type=event_type,
            aggregate_id=aggregate_id,
            aggregate_type=aggregate_type,
            payload=payload,
            message_group_id=message_group_id or aggregate_id,
        )

    @classmethod
    def from_file_event(
        cls,
        event: FileEvent,
        *,
        aggregate_type: str = "FileProcessing",
    ) -> OutboxEvent:
        """Create an outbox row whose SNS body is the canonical event itself."""
        if event.event_type not in {
            EventType.ANALYSIS_COMPLETED,
            EventType.PROCESSING_FAILED,
            EventType.FILE_SCANNED,
        }:
            raise ValueError(f"Unsupported processor output event: {event.event_type.value}")
        payload = event.model_dump(mode="json", by_alias=True, exclude_none=True)
        # Revalidate the wire shape here so invalid payloads can never enter the outbox.
        canonical = FileEvent.model_validate(payload)
        return cls(
            event_id=canonical.event_id_str,
            event_type=OutboxEventType(canonical.event_type.value),
            aggregate_id=canonical.file_id_str,
            aggregate_type=aggregate_type,
            payload=payload,
            message_group_id=canonical.file_id_str,
            created_at=canonical.timestamp.isoformat(),
        )

    def to_dynamodb_item(self) -> dict[str, Any]:
        """Convert to DynamoDB item format for outbox table."""
        item = {
            "PK": {"S": str(self.outbox_partition)},
            "SK": {"S": f"EVENT#{self.event_id}"},
            "eventId": {"S": self.event_id},
            "eventType": {"S": self.event_type.value},
            "aggregateId": {"S": self.aggregate_id},
            "aggregateType": {"S": self.aggregate_type},
            "payload": {"S": json.dumps(self.payload)},
            "status": {"S": self.status.value},
            "createdAt": {"S": self.created_at},
            "retryCount": {"N": str(self.retry_count)},
            "outboxPartition": {"S": str(self.outbox_partition)},
            "outboxShard": {"S": str(self.outbox_shard)},
            "GSI1PK": {"S": f"STATUS#{self.status.value}#{self.outbox_shard}"},
            "GSI1SK": {"S": self.created_at},
        }

        if self.published_at:
            item["publishedAt"] = {"S": self.published_at}
        if self.last_error:
            item["lastError"] = {"S": self.last_error}
        if self.message_group_id:
            item["messageGroupId"] = {"S": self.message_group_id}
        if self.ttl:
            item["ttl"] = {"N": str(self.ttl)}

        return item

    @staticmethod
    def _dynamodb_value(attribute: Any) -> Any:
        """Read either boto3 DynamoDB wire attributes or deserialized stream values."""
        if not isinstance(attribute, dict):
            return attribute
        if "S" in attribute:
            return attribute["S"]
        if "N" in attribute:
            return attribute["N"]
        if "BOOL" in attribute:
            return attribute["BOOL"]
        if "NULL" in attribute:
            return None
        return attribute

    @classmethod
    def from_dynamodb_item(cls, item: dict[str, Any]) -> OutboxEvent:
        """Create from DynamoDB item format."""
        payload = cls._dynamodb_value(item["payload"])
        return cls(
            event_id=str(cls._dynamodb_value(item["eventId"])),
            event_type=OutboxEventType(str(cls._dynamodb_value(item["eventType"]))),
            aggregate_id=str(cls._dynamodb_value(item["aggregateId"])),
            aggregate_type=str(cls._dynamodb_value(item.get("aggregateType")) or "FileProcessing"),
            payload=payload if isinstance(payload, dict) else json.loads(str(payload)),
            status=OutboxStatus(str(cls._dynamodb_value(item["status"]))),
            created_at=str(cls._dynamodb_value(item["createdAt"])),
            published_at=cls._dynamodb_value(item.get("publishedAt")),
            retry_count=int(cls._dynamodb_value(item.get("retryCount")) or "0"),
            last_error=cls._dynamodb_value(item.get("lastError")),
            message_group_id=cls._dynamodb_value(item.get("messageGroupId")),
            ttl=int(cls._dynamodb_value(item["ttl"])) if "ttl" in item else None,
            outbox_partition=str(
                cls._dynamodb_value(item.get("outboxPartition"))
                or cls._dynamodb_value(item.get("PK"))
            ),
            outbox_shard=(
                str(cls._dynamodb_value(item.get("outboxShard")))
                if item.get("outboxShard") is not None
                else None
            ),
        )

    def mark_published(
        self,
        retention_seconds: int = OUTBOX_DEFAULT_RETENTION_SECONDS,
    ) -> OutboxEvent:
        """Mark event as published (returns new instance for immutability in tests)."""
        self.status = OutboxStatus.PUBLISHED
        self.published_at = datetime.now(UTC).isoformat()
        self.ttl = int(datetime.now(UTC).timestamp()) + retention_seconds
        return self

    def mark_failed(self, error: str) -> OutboxEvent:
        """Mark event as failed with error message."""
        self.status = OutboxStatus.FAILED
        self.last_error = error
        self.retry_count += 1
        return self

    def to_sns_message(self) -> dict[str, Any]:
        """Return a schema-valid canonical FSAMP event as the SNS body."""
        event = FileEvent.model_validate(self.payload)
        if event.event_id_str != self.event_id:
            raise ValueError("Outbox eventId does not match canonical payload eventId")
        if event.event_type.value != self.event_type.value:
            raise ValueError("Outbox eventType does not match canonical payload eventType")
        return event.model_dump(mode="json", by_alias=True, exclude_none=True)

    def to_sns_attributes(self) -> dict[str, dict[str, str]]:
        """Generate SNS message attributes for filtering."""
        return {
            "eventType": {
                "DataType": "String",
                "StringValue": self.event_type.value,
            },
            "aggregateType": {
                "DataType": "String",
                "StringValue": self.aggregate_type,
            },
            "aggregateId": {
                "DataType": "String",
                "StringValue": self.aggregate_id,
            },
        }
