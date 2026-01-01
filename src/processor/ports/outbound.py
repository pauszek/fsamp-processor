# =============================================================================
# Outbound Ports (Driven Adapters)
# =============================================================================
"""
Outbound ports define interfaces for the domain to interact with external services.
These are implemented by output adapters (e.g., S3 Client, DynamoDB Repository).
"""

from abc import ABC, abstractmethod
from typing import Any

from processor.domain.events import FileEvent
from processor.domain.models import (
    AnalysisResult,
    FileContent,
    MetadataRecord,
    OutboxEvent,
    OutboxStatus,
)


class FileStorage(ABC):
    """
    Port for file storage operations.
    Implemented by adapters like S3 Client.
    """

    @abstractmethod
    def download(self, bucket_name: str, object_key: str) -> FileContent:
        """
        Download a file from storage.

        Args:
            bucket_name: The name of the bucket.
            object_key: The key (path) of the object.

        Returns:
            FileContent with the downloaded data and metadata.

        Raises:
            StorageError: If download fails.
        """
        ...

    @abstractmethod
    def upload(
        self,
        bucket_name: str,
        object_key: str,
        data: bytes,
        content_type: str | None = None,
        metadata: dict[str, str] | None = None,
    ) -> str:
        """
        Upload a file to storage.

        Args:
            bucket_name: The name of the bucket.
            object_key: The key (path) of the object.
            data: The file content.
            content_type: Optional MIME type.
            metadata: Optional metadata dict.

        Returns:
            The ETag of the uploaded object.

        Raises:
            StorageError: If upload fails.
        """
        ...

    @abstractmethod
    def exists(self, bucket_name: str, object_key: str) -> bool:
        """
        Check if a file exists in storage.

        Args:
            bucket_name: The name of the bucket.
            object_key: The key (path) of the object.

        Returns:
            True if the file exists, False otherwise.
        """
        ...

    @abstractmethod
    def delete(self, bucket_name: str, object_key: str) -> None:
        """
        Delete a file from storage.

        Args:
            bucket_name: The name of the bucket.
            object_key: The key (path) of the object.

        Raises:
            StorageError: If deletion fails.
        """
        ...

    @abstractmethod
    def get_presigned_url(
        self,
        bucket_name: str,
        object_key: str,
        expiration_seconds: int = 3600,
    ) -> str:
        """
        Generate a presigned URL for downloading a file.

        Args:
            bucket_name: The name of the bucket.
            object_key: The key (path) of the object.
            expiration_seconds: URL expiration time in seconds.

        Returns:
            The presigned URL.

        Raises:
            StorageError: If URL generation fails.
        """
        ...


class MetadataRepository(ABC):
    """
    Port for metadata persistence.
    Implemented by adapters like DynamoDB Repository.
    """

    @abstractmethod
    def save(self, record: MetadataRecord) -> None:
        """
        Save a metadata record.

        Args:
            record: The metadata record to save.

        Raises:
            StorageError: If save fails.
        """
        ...

    @abstractmethod
    def get_by_id(self, file_id: str) -> MetadataRecord | None:
        """
        Get the latest metadata record by file ID.

        Args:
            file_id: The file ID (partition key).

        Returns:
            The metadata record if found, None otherwise.

        Raises:
            StorageError: If retrieval fails.
        """
        ...

    @abstractmethod
    def get_history(self, file_id: str, limit: int = 10) -> list[MetadataRecord]:
        """
        Get metadata record history for a file.

        Args:
            file_id: The file ID (partition key).
            limit: Maximum number of records to return.

        Returns:
            List of metadata records, newest first.

        Raises:
            StorageError: If retrieval fails.
        """
        ...

    @abstractmethod
    def update_status(
        self,
        file_id: str,
        timestamp: str,
        status: str,
        error_message: str | None = None,
    ) -> None:
        """
        Update the status of a metadata record.

        Args:
            file_id: The file ID (partition key).
            timestamp: The timestamp (sort key).
            status: The new status.
            error_message: Optional error message.

        Raises:
            StorageError: If update fails.
        """
        ...

    @abstractmethod
    def query_by_status(
        self,
        status: str,
        limit: int = 100,
    ) -> list[MetadataRecord]:
        """
        Query metadata records by status (requires GSI).

        Args:
            status: The status to filter by.
            limit: Maximum number of records to return.

        Returns:
            List of metadata records.

        Raises:
            StorageError: If query fails.
        """
        ...


class EventPublisher(ABC):
    """
    Port for publishing events.
    Implemented by adapters like SNS Publisher.
    """

    @abstractmethod
    def publish(self, event: FileEvent) -> str:
        """
        Publish an event to the event bus.

        Args:
            event: The event to publish.

        Returns:
            The message ID.

        Raises:
            MessageError: If publishing fails.
        """
        ...

    @abstractmethod
    def publish_batch(self, events: list[FileEvent]) -> list[str]:
        """
        Publish multiple events in a batch.

        Args:
            events: List of events to publish.

        Returns:
            List of message IDs.

        Raises:
            MessageError: If publishing fails.
        """
        ...


class CryptoProvider(ABC):
    """
    Port for cryptographic operations.
    Implemented by adapters like KMS Crypto Provider.
    Compliant with FIPS 140-3 requirements.
    """

    @abstractmethod
    def encrypt(self, plaintext: bytes, context: dict[str, str] | None = None) -> bytes:
        """
        Encrypt data using envelope encryption.

        Args:
            plaintext: The data to encrypt.
            context: Optional encryption context for key derivation.

        Returns:
            The ciphertext.

        Raises:
            CryptoError: If encryption fails.
        """
        ...

    @abstractmethod
    def decrypt(self, ciphertext: bytes, context: dict[str, str] | None = None) -> bytes:
        """
        Decrypt data using envelope encryption.

        Args:
            ciphertext: The data to decrypt.
            context: Optional encryption context for key derivation.

        Returns:
            The plaintext.

        Raises:
            CryptoError: If decryption fails.
        """
        ...

    @abstractmethod
    def generate_data_key(
        self,
        context: dict[str, str] | None = None,
    ) -> tuple[bytes, bytes]:
        """
        Generate a data key for client-side encryption.

        Args:
            context: Optional encryption context.

        Returns:
            Tuple of (plaintext_key, encrypted_key).

        Raises:
            CryptoError: If key generation fails.
        """
        ...

    @abstractmethod
    def compute_hash(self, data: bytes, algorithm: str = "SHA-256") -> str:
        """
        Compute a cryptographic hash of data.

        Args:
            data: The data to hash.
            algorithm: The hash algorithm (SHA-256, SHA-384, SHA-512).

        Returns:
            The hex-encoded hash.

        Raises:
            CryptoError: If hashing fails.
        """
        ...


class FileAnalyzer(ABC):
    """
    Port for file analysis/scanning.
    Implemented by adapters for virus scanning, content analysis, etc.
    """

    @abstractmethod
    def analyze(self, content: FileContent) -> AnalysisResult:
        """
        Analyze file content for security threats and metadata.

        Args:
            content: The file content to analyze.

        Returns:
            AnalysisResult with findings.

        Raises:
            ProcessingError: If analysis fails.
        """
        ...


class MetricsCollector(ABC):
    """
    Port for collecting metrics.
    Implemented by adapters like CloudWatch Metrics.
    """

    @abstractmethod
    def increment_counter(
        self,
        name: str,
        value: int = 1,
        dimensions: dict[str, str] | None = None,
    ) -> None:
        """Increment a counter metric."""
        ...

    @abstractmethod
    def record_gauge(
        self,
        name: str,
        value: float,
        dimensions: dict[str, str] | None = None,
    ) -> None:
        """Record a gauge metric."""
        ...

    @abstractmethod
    def record_histogram(
        self,
        name: str,
        value: float,
        dimensions: dict[str, str] | None = None,
    ) -> None:
        """Record a histogram/timing metric."""
        ...

    @abstractmethod
    def record_processing_time(
        self,
        duration_ms: int,
        status: str,
        event_type: str,
    ) -> None:
        """Record file processing time."""
        ...


class OutboxRepository(ABC):
    """
    Port for Outbox Pattern persistence.
    Implements the Transactional Outbox Pattern for reliable event publishing.
    
    The outbox repository ensures atomicity between business data writes
    and event recording - both are written in a single transaction.
    """

    @abstractmethod
    def save_with_outbox(
        self,
        record: MetadataRecord,
        outbox_event: OutboxEvent,
    ) -> None:
        """
        Save metadata record and outbox event in a single transaction.
        
        This is the core of the Outbox Pattern - ensuring atomicity
        between data persistence and event recording.

        Args:
            record: The metadata record to save.
            outbox_event: The event to save to outbox.

        Raises:
            StorageError: If save fails.
        """
        ...

    @abstractmethod
    def get_pending_events(
        self,
        limit: int = 100,
    ) -> list[OutboxEvent]:
        """
        Get pending outbox events for publishing.
        
        Used by the outbox publisher to fetch events that need
        to be published to the message broker.

        Args:
            limit: Maximum number of events to return.

        Returns:
            List of pending outbox events, oldest first.

        Raises:
            StorageError: If query fails.
        """
        ...

    @abstractmethod
    def mark_published(
        self,
        event_id: str,
        aggregate_type: str = "FileProcessing",
    ) -> None:
        """
        Mark an outbox event as published.
        
        Called after successfully publishing to the message broker.

        Args:
            event_id: The event ID to mark.
            aggregate_type: The aggregate type for the partition key.

        Raises:
            StorageError: If update fails.
        """
        ...

    @abstractmethod
    def mark_failed(
        self,
        event_id: str,
        error: str,
        aggregate_type: str = "FileProcessing",
    ) -> None:
        """
        Mark an outbox event as failed.
        
        Called when publishing fails, increments retry count.

        Args:
            event_id: The event ID to mark.
            error: The error message.
            aggregate_type: The aggregate type for the partition key.

        Raises:
            StorageError: If update fails.
        """
        ...

    @abstractmethod
    def get_failed_events(
        self,
        limit: int = 100,
    ) -> list[OutboxEvent]:
        """
        Get failed outbox events for retry.
        
        Used to retrieve events that failed to publish for retry.

        Args:
            limit: Maximum number of events to return.

        Returns:
            List of failed outbox events.

        Raises:
            StorageError: If query fails.
        """
        ...

    @abstractmethod
    def delete_old_published(
        self,
        older_than_hours: int = 24,
    ) -> int:
        """
        Delete old published events (cleanup).
        
        DynamoDB TTL should handle this automatically, but this
        provides manual cleanup capability.

        Args:
            older_than_hours: Delete events older than this.

        Returns:
            Number of deleted events.

        Raises:
            StorageError: If deletion fails.
        """
        ...
