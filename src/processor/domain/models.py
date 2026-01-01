# =============================================================================
# Domain Models
# =============================================================================
"""
Core domain models representing the state and results of file processing.
Implements Outbox Pattern for reliable event publishing.
"""

from dataclasses import dataclass, field
from datetime import datetime
from enum import StrEnum
from typing import Any
import json
import uuid


class ProcessingStatus(StrEnum):
    """Status of file processing."""

    PENDING = "PENDING"
    IN_PROGRESS = "IN_PROGRESS"
    COMPLETED = "COMPLETED"
    FAILED = "FAILED"
    RETRYING = "RETRYING"


class OutboxStatus(StrEnum):
    """Status of outbox event for reliable publishing."""

    PENDING = "PENDING"
    PUBLISHED = "PUBLISHED"
    FAILED = "FAILED"


class OutboxEventType(StrEnum):
    """Types of events in the outbox."""

    FILE_PROCESSED = "FILE_PROCESSED"
    FILE_FAILED = "FILE_FAILED"
    FILE_SCAN_COMPLETED = "FILE_SCAN_COMPLETED"
    FILE_QUARANTINED = "FILE_QUARANTINED"


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
    def is_success(self) -> bool:
        """Check if processing was successful."""
        return self.status == ProcessingStatus.COMPLETED

    @property
    def is_failure(self) -> bool:
        """Check if processing failed."""
        return self.status == ProcessingStatus.FAILED

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
    ) -> "ProcessingResult":
        """Create a new result with completion data (immutable pattern)."""
        return ProcessingResult(
            event_id=self.event_id,
            correlation_id=self.correlation_id,
            status=status,
            started_at=self.started_at,
            completed_at=datetime.utcnow(),
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
    analyzed_at: datetime = field(default_factory=datetime.utcnow)
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass(slots=True)
class MetadataRecord:
    """
    Metadata record stored in DynamoDB.
    Represents the persistent state of a processed file.
    """

    # Partition Key
    file_id: str

    # Sort Key (for versioning/history)
    timestamp: str

    # Core attributes
    correlation_id: str
    original_filename: str
    file_size_bytes: int
    mime_type: str | None
    bucket_name: str
    object_key: str
    status: ProcessingStatus

    # Processing details
    file_hash: str | None = None
    is_encrypted: bool = True
    kms_key_id: str | None = None

    # Analysis results
    is_safe: bool | None = None
    scan_findings: list[str] = field(default_factory=list)

    # Timestamps
    created_at: str | None = None
    updated_at: str | None = None
    processed_at: str | None = None

    # Error tracking
    error_message: str | None = None
    error_code: str | None = None
    retry_count: int = 0

    # TTL for DynamoDB (optional cleanup)
    ttl: int | None = None

    def to_dynamodb_item(self) -> dict[str, Any]:
        """Convert to DynamoDB item format."""
        item = {
            "PK": {"S": f"FILE#{self.file_id}"},
            "SK": {"S": f"TS#{self.timestamp}"},
            "correlationId": {"S": self.correlation_id},
            "originalFilename": {"S": self.original_filename},
            "fileSizeBytes": {"N": str(self.file_size_bytes)},
            "bucketName": {"S": self.bucket_name},
            "objectKey": {"S": self.object_key},
            "status": {"S": self.status.value},
            "isEncrypted": {"BOOL": self.is_encrypted},
            "retryCount": {"N": str(self.retry_count)},
        }

        # Optional fields
        if self.mime_type:
            item["mimeType"] = {"S": self.mime_type}
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

        return item

    @classmethod
    def from_dynamodb_item(cls, item: dict[str, Any]) -> "MetadataRecord":
        """Create from DynamoDB item format."""
        # Extract PK and SK
        pk = item["PK"]["S"]  # FILE#<file_id>
        sk = item["SK"]["S"]  # TS#<timestamp>

        file_id = pk.replace("FILE#", "")
        timestamp = sk.replace("TS#", "")

        return cls(
            file_id=file_id,
            timestamp=timestamp,
            correlation_id=item["correlationId"]["S"],
            original_filename=item["originalFilename"]["S"],
            file_size_bytes=int(item["fileSizeBytes"]["N"]),
            mime_type=item.get("mimeType", {}).get("S"),
            bucket_name=item["bucketName"]["S"],
            object_key=item["objectKey"]["S"],
            status=ProcessingStatus(item["status"]["S"]),
            file_hash=item.get("fileHash", {}).get("S"),
            is_encrypted=item.get("isEncrypted", {}).get("BOOL", True),
            kms_key_id=item.get("kmsKeyId", {}).get("S"),
            is_safe=item.get("isSafe", {}).get("BOOL"),
            scan_findings=list(item.get("scanFindings", {}).get("SS", [])),
            created_at=item.get("createdAt", {}).get("S"),
            updated_at=item.get("updatedAt", {}).get("S"),
            processed_at=item.get("processedAt", {}).get("S"),
            error_message=item.get("errorMessage", {}).get("S"),
            error_code=item.get("errorCode", {}).get("S"),
            retry_count=int(item.get("retryCount", {}).get("N", "0")),
            ttl=int(item["ttl"]["N"]) if "ttl" in item else None,
        )


# =============================================================================
# Outbox Pattern Models
# =============================================================================


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

    # Unique event identifier
    event_id: str

    # Type of event (determines consumer routing)
    event_type: OutboxEventType

    # Aggregate/entity this event relates to
    aggregate_id: str
    aggregate_type: str = "FileProcessing"

    # Event payload (serialized JSON)
    payload: dict[str, Any] = field(default_factory=dict)

    # Publishing status
    status: OutboxStatus = OutboxStatus.PENDING

    # Timestamps
    created_at: str = field(default_factory=lambda: datetime.utcnow().isoformat())
    published_at: str | None = None

    # Retry tracking for failed publishes
    retry_count: int = 0
    last_error: str | None = None

    # Optional ordering key for FIFO topics
    message_group_id: str | None = None

    # TTL for cleanup after publishing (24h default after publish)
    ttl: int | None = None

    @classmethod
    def create(
        cls,
        event_type: OutboxEventType,
        aggregate_id: str,
        payload: dict[str, Any],
        aggregate_type: str = "FileProcessing",
        message_group_id: str | None = None,
    ) -> "OutboxEvent":
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
    def for_file_processed(
        cls,
        file_id: str,
        correlation_id: str,
        file_hash: str,
        is_safe: bool,
        bucket_name: str,
        object_key: str,
    ) -> "OutboxEvent":
        """Create event for successful file processing."""
        return cls.create(
            event_type=OutboxEventType.FILE_PROCESSED,
            aggregate_id=file_id,
            payload={
                "fileId": file_id,
                "correlationId": correlation_id,
                "fileHash": file_hash,
                "isSafe": is_safe,
                "bucketName": bucket_name,
                "objectKey": object_key,
                "processedAt": datetime.utcnow().isoformat(),
            },
        )

    @classmethod
    def for_file_failed(
        cls,
        file_id: str,
        correlation_id: str,
        error_code: str,
        error_message: str,
    ) -> "OutboxEvent":
        """Create event for failed file processing."""
        return cls.create(
            event_type=OutboxEventType.FILE_FAILED,
            aggregate_id=file_id,
            payload={
                "fileId": file_id,
                "correlationId": correlation_id,
                "errorCode": error_code,
                "errorMessage": error_message,
                "failedAt": datetime.utcnow().isoformat(),
            },
        )

    @classmethod
    def for_file_quarantined(
        cls,
        file_id: str,
        correlation_id: str,
        reason: str,
        findings: list[str],
    ) -> "OutboxEvent":
        """Create event for quarantined (unsafe) file."""
        return cls.create(
            event_type=OutboxEventType.FILE_QUARANTINED,
            aggregate_id=file_id,
            payload={
                "fileId": file_id,
                "correlationId": correlation_id,
                "reason": reason,
                "findings": findings,
                "quarantinedAt": datetime.utcnow().isoformat(),
            },
        )

    def to_dynamodb_item(self) -> dict[str, Any]:
        """Convert to DynamoDB item format for outbox table."""
        item = {
            "PK": {"S": f"OUTBOX#{self.aggregate_type}"},
            "SK": {"S": f"EVENT#{self.event_id}"},
            "eventId": {"S": self.event_id},
            "eventType": {"S": self.event_type.value},
            "aggregateId": {"S": self.aggregate_id},
            "aggregateType": {"S": self.aggregate_type},
            "payload": {"S": json.dumps(self.payload)},
            "status": {"S": self.status.value},
            "createdAt": {"S": self.created_at},
            "retryCount": {"N": str(self.retry_count)},
            # GSI for querying pending events
            "GSI1PK": {"S": f"STATUS#{self.status.value}"},
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

    @classmethod
    def from_dynamodb_item(cls, item: dict[str, Any]) -> "OutboxEvent":
        """Create from DynamoDB item format."""
        return cls(
            event_id=item["eventId"]["S"],
            event_type=OutboxEventType(item["eventType"]["S"]),
            aggregate_id=item["aggregateId"]["S"],
            aggregate_type=item.get("aggregateType", {}).get("S", "FileProcessing"),
            payload=json.loads(item["payload"]["S"]),
            status=OutboxStatus(item["status"]["S"]),
            created_at=item["createdAt"]["S"],
            published_at=item.get("publishedAt", {}).get("S"),
            retry_count=int(item.get("retryCount", {}).get("N", "0")),
            last_error=item.get("lastError", {}).get("S"),
            message_group_id=item.get("messageGroupId", {}).get("S"),
            ttl=int(item["ttl"]["N"]) if "ttl" in item else None,
        )

    @classmethod
    def from_dynamodb_stream_record(cls, record: dict[str, Any]) -> "OutboxEvent":
        """Create from DynamoDB Streams record (NewImage format)."""
        new_image = record.get("dynamodb", {}).get("NewImage", {})
        if not new_image:
            raise ValueError("No NewImage in DynamoDB stream record")
        return cls.from_dynamodb_item(new_image)

    def mark_published(self) -> "OutboxEvent":
        """Mark event as published (returns new instance for immutability in tests)."""
        self.status = OutboxStatus.PUBLISHED
        self.published_at = datetime.utcnow().isoformat()
        # Set TTL to 24 hours from now for cleanup
        self.ttl = int(datetime.utcnow().timestamp()) + 86400
        return self

    def mark_failed(self, error: str) -> "OutboxEvent":
        """Mark event as failed with error message."""
        self.status = OutboxStatus.FAILED
        self.last_error = error
        self.retry_count += 1
        return self

    def to_sns_message(self) -> dict[str, Any]:
        """Convert to SNS message format."""
        return {
            "eventId": self.event_id,
            "eventType": self.event_type.value,
            "aggregateId": self.aggregate_id,
            "aggregateType": self.aggregate_type,
            "payload": self.payload,
            "timestamp": self.created_at,
        }

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
