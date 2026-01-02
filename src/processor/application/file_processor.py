# =============================================================================
# File Processor Application Service
# =============================================================================
"""
Main application service for processing file events.
Orchestrates the workflow: download → analyze → store metadata → publish result.

Implements Outbox Pattern for reliable event publishing - metadata and outbox
events are written atomically in a single DynamoDB transaction.
"""

from datetime import datetime
from typing import Any

import structlog

from processor.domain.events import EventType, FileEvent
from processor.domain.exceptions import (
    NonRetryableError,
    ProcessingError,
    StorageError,
)
from processor.domain.models import (
    AnalysisResult,
    FileContent,
    MetadataRecord,
    OutboxEvent,
    ProcessingResult,
    ProcessingStatus,
)
from processor.ports.outbound import (
    CryptoProvider,
    EventPublisher,
    FileStorage,
    MetadataRepository,
    OutboxRepository,
)

logger = structlog.get_logger(__name__)


class FileProcessorService:
    """
    File Processor Application Service.

    Implements the main use case: processing file upload events.

    Workflow:
    1. Validate incoming event
    2. Download file from S3
    3. Verify integrity (hash check)
    4. Analyze file content
    5. Store metadata in DynamoDB with Outbox event (transactional)
    6. Outbox Publisher Lambda publishes events asynchronously

    Uses Outbox Pattern for reliable event publishing:
    - Metadata and outbox events are written atomically
    - DynamoDB Streams triggers the Outbox Publisher
    - At-least-once delivery guarantee
    """

    def __init__(
        self,
        file_storage: FileStorage,
        metadata_repo: MetadataRepository,
        event_publisher: EventPublisher,
        crypto_provider: CryptoProvider,
        outbox_repo: OutboxRepository | None = None,
        max_file_size_bytes: int = 100 * 1024 * 1024,  # 100 MB default
        use_outbox_pattern: bool = True,
    ) -> None:
        """
        Initialize File Processor Service.

        Args:
            file_storage: Storage port for file operations.
            metadata_repo: Repository port for metadata persistence.
            event_publisher: Publisher port for events (direct mode).
            crypto_provider: Crypto port for FIPS operations.
            outbox_repo: Outbox repository for transactional writes.
            max_file_size_bytes: Maximum allowed file size.
            use_outbox_pattern: Whether to use Outbox Pattern (recommended).
        """
        self._storage = file_storage
        self._metadata = metadata_repo
        self._publisher = event_publisher
        self._crypto = crypto_provider
        self._outbox = outbox_repo
        self._max_file_size = max_file_size_bytes
        self._use_outbox = use_outbox_pattern and outbox_repo is not None

        logger.info(
            "FileProcessorService initialized",
            max_file_size_mb=max_file_size_bytes / (1024 * 1024),
            outbox_enabled=self._use_outbox,
        )

    def handle(self, event: FileEvent) -> ProcessingResult:
        """
        Handle a file event (main entry point).

        Args:
            event: The file event to process.

        Returns:
            ProcessingResult with the outcome.

        Raises:
            ProcessingError: If processing fails (retryable).
            NonRetryableError: If processing fails (non-retryable).
        """
        log = logger.bind(
            event_id=event.event_id_str,
            correlation_id=event.correlation_id_str,
            event_type=event.event_type.value,
            filename=event.file_metadata.original_filename,
        )

        started_at = datetime.utcnow()
        result = ProcessingResult(
            event_id=event.event_id_str,
            correlation_id=event.correlation_id_str,
            status=ProcessingStatus.IN_PROGRESS,
            started_at=started_at,
        )

        try:
            log.info("Starting file processing")

            # Route based on event type
            if event.event_type == EventType.FILE_UPLOADED:
                result = self._process_uploaded_file(event, result, log)
            elif event.event_type == EventType.FILE_SCANNED:
                result = self._process_scanned_file(event, result, log)
            else:
                log.warning("Unhandled event type", event_type=event.event_type)
                result = result.with_completion(ProcessingStatus.COMPLETED)

            log.info(
                "File processing completed",
                status=result.status.value,
                duration_ms=result.duration_ms,
            )

            return result

        except NonRetryableError:
            raise

        except StorageError as e:
            log.error("Storage error during processing", error=str(e))
            self._handle_failure(event, str(e), "STORAGE_ERROR")
            raise ProcessingError(
                message=str(e),
                event_id=str(event.event_id),
                correlation_id=str(event.correlation_id),
                retryable=True,
                cause=e,
            ) from e

        except Exception as e:
            log.exception("Unexpected error during processing")
            self._handle_failure(event, str(e), "UNEXPECTED_ERROR")
            raise ProcessingError(
                message=f"Unexpected error: {e}",
                event_id=str(event.event_id),
                correlation_id=str(event.correlation_id),
                retryable=True,
                cause=e,
            ) from e

    def _process_uploaded_file(
        self,
        event: FileEvent,
        result: ProcessingResult,
        log: Any,
    ) -> ProcessingResult:
        """Process a newly uploaded file."""
        # Validate file size
        if event.file_metadata.file_size_bytes > self._max_file_size:
            raise NonRetryableError(
                message=f"File too large: {event.file_metadata.file_size_bytes} bytes "
                f"(max: {self._max_file_size} bytes)",
            )

        # Create initial metadata record
        timestamp = datetime.utcnow().isoformat()
        metadata_record = self._create_metadata_record(event, timestamp)
        metadata_record.status = ProcessingStatus.IN_PROGRESS

        # Save initial status (non-transactional, just metadata)
        self._metadata.save(metadata_record)
        log.debug("Initial metadata record saved")

        # Download file from S3
        file_content = self._storage.download(
            bucket_name=event.storage_location.bucket_name,
            object_key=event.storage_location.object_key,
        )

        log.debug(
            "File downloaded",
            size_bytes=file_content.content_length,
            encrypted=file_content.is_encrypted,
        )

        # Compute file hash (FIPS 140-3 compliant)
        file_hash = self._crypto.compute_hash(file_content.data, "SHA-256")
        log.debug("File hash computed", hash=file_hash[:16] + "...")

        # Analyze file
        analysis_result = self._analyze_file(file_content, log)

        # Update metadata with results
        metadata_record.file_hash = file_hash
        metadata_record.is_safe = analysis_result.is_safe
        metadata_record.scan_findings = analysis_result.findings
        metadata_record.status = ProcessingStatus.COMPLETED
        metadata_record.processed_at = datetime.utcnow().isoformat()

        # Create outbox event for completion
        if analysis_result.is_safe:
            outbox_event = OutboxEvent.for_file_processed(
                file_id=str(event.event_id),
                correlation_id=str(event.correlation_id),
                file_hash=file_hash,
                is_safe=True,
                bucket_name=event.storage_location.bucket_name,
                object_key=event.storage_location.object_key,
            )
        else:
            outbox_event = OutboxEvent.for_file_quarantined(
                file_id=str(event.event_id),
                correlation_id=str(event.correlation_id),
                reason="File failed security analysis",
                findings=analysis_result.findings,
            )

        # Use transactional write with Outbox Pattern if enabled
        if self._use_outbox and self._outbox:
            self._outbox.save_with_outbox(metadata_record, outbox_event)
            log.info(
                "Metadata and outbox event saved transactionally",
                event_id=outbox_event.event_id,
                event_type=outbox_event.event_type.value,
            )
        else:
            # Fallback to non-transactional (dual write - less reliable)
            self._metadata.save(metadata_record)
            log.debug("Metadata record updated with analysis results")

            # Publish completion event directly
            completion_event = event.with_new_event_type(EventType.ANALYSIS_COMPLETED)
            self._publisher.publish(completion_event)
            log.info("Completion event published (direct mode)")

        return result.with_completion(
            status=ProcessingStatus.COMPLETED,
            metadata={
                "file_hash": file_hash,
                "is_safe": analysis_result.is_safe,
                "findings_count": len(analysis_result.findings),
                "outbox_event_id": outbox_event.event_id if self._use_outbox else None,
            },
        )

    def _process_scanned_file(
        self,
        event: FileEvent,
        result: ProcessingResult,
        log: Any,
    ) -> ProcessingResult:
        """Process a file that has been scanned externally."""
        # Just update metadata for externally scanned files
        timestamp = datetime.utcnow().isoformat()
        metadata_record = self._create_metadata_record(event, timestamp)
        metadata_record.status = ProcessingStatus.COMPLETED
        metadata_record.processed_at = timestamp

        self._metadata.save(metadata_record)

        log.info("Scanned file metadata saved")

        return result.with_completion(ProcessingStatus.COMPLETED)

    def _analyze_file(
        self,
        content: FileContent,
        log: Any,
    ) -> AnalysisResult:
        """
        Analyze file content for security and metadata.

        This is a simplified implementation. In production, you would
        integrate with virus scanners, content analysis services, etc.
        """
        findings: list[str] = []

        # Basic content checks
        data = content.data

        # Check for empty file
        if len(data) == 0:
            findings.append("File is empty")

        # Check for potential executable content (simplified)
        if data[:2] == b"MZ":  # DOS/Windows executable
            findings.append("Potentially executable content (PE format)")

        if data[:4] == b"\x7fELF":  # Linux executable
            findings.append("Potentially executable content (ELF format)")

        if data[:4] == b"%PDF":  # PDF
            log.debug("PDF file detected")

        # Compute content hash
        content_hash = self._crypto.compute_hash(data, "SHA-256")

        # Determine if safe (simplified logic)
        is_safe = len([f for f in findings if "executable" in f.lower()]) == 0

        log.debug(
            "File analysis completed",
            is_safe=is_safe,
            findings_count=len(findings),
        )

        return AnalysisResult(
            file_hash_sha256=content_hash,
            is_safe=is_safe,
            scan_engine="fsamp-internal",
            findings=findings,
        )

    def _create_metadata_record(
        self,
        event: FileEvent,
        timestamp: str,
    ) -> MetadataRecord:
        """Create a metadata record from an event."""
        return MetadataRecord(
            file_id=str(event.event_id),
            timestamp=timestamp,
            correlation_id=str(event.correlation_id),
            original_filename=event.file_metadata.original_filename,
            file_size_bytes=event.file_metadata.file_size_bytes,
            mime_type=event.file_metadata.mime_type,
            bucket_name=event.storage_location.bucket_name,
            object_key=event.storage_location.object_key,
            status=ProcessingStatus.PENDING,
            is_encrypted=event.security_context.is_encrypted,
            kms_key_id=event.security_context.kms_key_id,
        )

    def _handle_failure(
        self,
        event: FileEvent,
        error_message: str,
        error_code: str,
    ) -> None:
        """Handle processing failure - update metadata and publish failure event."""
        try:
            # Create metadata record with failure status
            timestamp = datetime.utcnow().isoformat()
            record = self._create_metadata_record(event, timestamp)
            record.status = ProcessingStatus.FAILED
            record.error_message = error_message
            record.error_code = error_code

            # Create outbox event for failure
            outbox_event = OutboxEvent.for_file_failed(
                file_id=str(event.event_id),
                correlation_id=str(event.correlation_id),
                error_code=error_code,
                error_message=error_message,
            )

            # Use transactional write with Outbox Pattern if enabled
            if self._use_outbox and self._outbox:
                self._outbox.save_with_outbox(record, outbox_event)
                logger.info(
                    "Failure metadata and outbox event saved transactionally",
                    event_id=outbox_event.event_id,
                )
            else:
                # Fallback to non-transactional
                self._metadata.save(record)

                # Publish failure event directly
                failure_event = event.with_new_event_type(EventType.PROCESSING_FAILED)
                self._publisher.publish(failure_event)

        except Exception as e:
            # Log but don't raise - we're already handling an error
            logger.exception(
                "Failed to handle processing failure",
                original_error=error_message,
                handler_error=str(e),
            )
