# =============================================================================
# Domain Models
# =============================================================================
"""
Core domain models representing the state and results of file processing.
"""

from dataclasses import dataclass, field
from datetime import datetime
from enum import StrEnum
from typing import Any


class ProcessingStatus(StrEnum):
    """Status of file processing."""

    PENDING = "PENDING"
    IN_PROGRESS = "IN_PROGRESS"
    COMPLETED = "COMPLETED"
    FAILED = "FAILED"
    RETRYING = "RETRYING"


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
