"""Application service for bounded, idempotent file processing."""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Any

import structlog

from processor.domain.events import (
    EventType,
    FailureDetails,
    FileEvent,
    ProcessingResultDetails,
    StorageLocation,
)
from processor.domain.exceptions import (
    NonRetryableError,
    ProcessingClaimUnavailableError,
    ProcessingError,
    StorageError,
)
from processor.domain.models import (
    AnalysisResult,
    FileContent,
    MetadataRecord,
    OutboxEvent,
    ProcessingClaim,
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
    """Process one canonical event and persist one shared current-state record."""

    def __init__(
        self,
        file_storage: FileStorage,
        metadata_repo: MetadataRepository,
        event_publisher: EventPublisher,
        crypto_provider: CryptoProvider,
        outbox_repo: OutboxRepository | None = None,
        max_file_size_bytes: int = 100 * 1024 * 1024,
        use_outbox_pattern: bool = True,
        allowed_bucket_name: str | None = None,
        allowed_region: str | None = None,
        quarantine_prefix: str = "quarantine",
        processing_claim_ttl_seconds: int = 330,
    ) -> None:
        self._storage = file_storage
        self._metadata = metadata_repo
        self._publisher = event_publisher
        self._crypto = crypto_provider
        self._outbox = outbox_repo
        self._max_file_size = max_file_size_bytes
        self._use_outbox = use_outbox_pattern and outbox_repo is not None
        self._allowed_bucket = allowed_bucket_name
        self._allowed_region = allowed_region
        self._quarantine_prefix = quarantine_prefix.strip("/") or "quarantine"
        self._processing_claim_ttl_seconds = processing_claim_ttl_seconds

    def handle(self, event: FileEvent) -> ProcessingResult:
        """Handle an input event, preserving retry/DLQ semantics."""
        log = logger.bind(
            event_id=event.event_id_str,
            file_id=event.file_id_str,
            correlation_id=event.correlation_id_str,
            event_type=event.event_type.value,
            redacted_filename=event.file_metadata.redacted_filename,
        )
        started_at = datetime.now(UTC)
        result = ProcessingResult(
            event_id=event.event_id_str,
            correlation_id=event.correlation_id_str,
            status=ProcessingStatus.PROCESSING,
            started_at=started_at,
        )

        duplicate = self._idempotent_result(event, result)
        if duplicate is not None:
            log.info("Duplicate input event acknowledged idempotently")
            return duplicate

        if event.event_type not in {EventType.FILE_UPLOADED, EventType.FILE_SCANNED}:
            log.info("Ignoring terminal processor event")
            return result.with_completion(ProcessingStatus.COMPLETED)

        claim: ProcessingClaim | None = None
        try:
            initial_record = self._create_metadata_record(event, started_at.isoformat())
            claim = self._metadata.claim_processing(
                initial_record,
                event.event_id_str,
                self._processing_claim_ttl_seconds,
            )
            if claim is None:
                duplicate = self._idempotent_result(event, result)
                if duplicate is not None:
                    log.info("Concurrent completion acknowledged idempotently")
                    return duplicate
                raise ProcessingClaimUnavailableError(
                    message="Another worker owns the active processing lease",
                    event_id=event.event_id_str,
                    correlation_id=event.correlation_id_str,
                    retryable=True,
                )
            if event.event_type == EventType.FILE_UPLOADED:
                return self._process_uploaded_file(event, result, log, claim)
            if event.event_type == EventType.FILE_SCANNED:
                return self._process_scanned_file(event, result, log, claim)
            raise AssertionError("Actionable event type was not handled")
        except ProcessingClaimUnavailableError:
            raise
        except NonRetryableError as error:
            self._handle_failure(
                event,
                error.message,
                error.error_code,
                retryable=False,
                claim=claim,
            )
            raise
        except StorageError as error:
            self._handle_failure(
                event,
                error.message,
                "STORAGE_ERROR",
                retryable=True,
                claim=claim,
            )
            raise ProcessingError(
                message=error.message,
                event_id=event.event_id_str,
                correlation_id=event.correlation_id_str,
                retryable=True,
                cause=error,
            ) from error
        except ProcessingError as error:
            self._handle_failure(
                event,
                error.message,
                error.error_code,
                retryable=error.retryable,
                claim=claim,
            )
            raise
        except Exception as error:
            self._handle_failure(
                event,
                str(error),
                "UNEXPECTED_ERROR",
                retryable=True,
                claim=claim,
            )
            raise ProcessingError(
                message=f"Unexpected error: {error}",
                event_id=event.event_id_str,
                correlation_id=event.correlation_id_str,
                retryable=True,
                cause=error,
            ) from error

    def _idempotent_result(
        self,
        event: FileEvent,
        result: ProcessingResult,
    ) -> ProcessingResult | None:
        existing = self._metadata.get_by_id(event.file_id_str)
        if existing is None or existing.last_processed_event_id != event.event_id_str:
            return None
        if existing.status == ProcessingStatus.COMPLETED:
            return result.with_completion(
                ProcessingStatus.COMPLETED,
                metadata={
                    "duplicate": True,
                    "file_hash": existing.file_hash,
                    "is_safe": existing.is_safe,
                    "findings_count": len(existing.scan_findings),
                },
            )
        raise NonRetryableError(
            message=existing.error_message or "Previously rejected input event",
            error_code=existing.error_code or "PREVIOUSLY_REJECTED",
        )

    def _process_uploaded_file(
        self,
        event: FileEvent,
        result: ProcessingResult,
        log: Any,
        claim: ProcessingClaim,
    ) -> ProcessingResult:
        self._validate_event_location(event)
        if event.file_metadata.file_size_bytes > self._max_file_size:
            raise NonRetryableError(
                message=(
                    f"Declared file size {event.file_metadata.file_size_bytes} exceeds "
                    f"the configured maximum {self._max_file_size}"
                ),
                error_code="DECLARED_FILE_TOO_LARGE",
            )

        timestamp = datetime.now(UTC).isoformat()
        record = self._create_metadata_record(event, timestamp)

        content = self._storage.download(
            event.storage_location.bucket_name,
            event.storage_location.object_key,
            max_bytes=self._max_file_size,
        )
        self._validate_actual_object(event, content)

        file_hash = self._crypto.compute_hash(content.data, "SHA-256")
        if file_hash != event.file_metadata.checksum_sha256:
            raise NonRetryableError(
                message="Downloaded object SHA-256 does not match the canonical event checksum",
                error_code="CHECKSUM_MISMATCH",
            )

        analysis = self._analyze_file(content, file_hash, log)
        output_location = event.storage_location
        if not analysis.is_safe:
            output_location = self._quarantine_file(event, file_hash)

        processed_at = datetime.now(UTC)
        record.file_hash = file_hash
        record.is_safe = analysis.is_safe
        record.scan_findings = analysis.findings
        record.status = ProcessingStatus.COMPLETED
        record.processed_at = processed_at.isoformat()
        record.updated_at = processed_at.isoformat()
        record.last_processed_event_id = event.event_id_str
        record.file_size_bytes = content.content_length
        record.mime_type = content.content_type or event.file_metadata.mime_type
        record.is_encrypted = content.encryption_algorithm == "aws:kms"
        record.encryption_algorithm = event.security_context.encryption_algorithm
        record.kms_key_id = content.kms_key_id
        record.bucket_name = output_location.bucket_name
        record.object_key = output_location.object_key

        completion = event.with_new_event_type(
            EventType.ANALYSIS_COMPLETED,
            processing_result=ProcessingResultDetails(
                is_safe=analysis.is_safe,
                findings=analysis.findings,
                processed_at=processed_at,
                file_hash_sha256=file_hash,
                scan_engine=analysis.scan_engine,
            ),
            storage_location=output_location,
        )
        outbox_event = OutboxEvent.from_file_event(completion)
        self._persist_and_publish(record, completion, outbox_event, claim)
        if not analysis.is_safe:
            try:
                # The copy and its canonical location are durable now. Deleting the
                # encrypted source is cleanup only; failure must not roll back or
                # overwrite the committed COMPLETED state.
                self._storage.delete(
                    event.storage_location.bucket_name,
                    event.storage_location.object_key,
                )
            except StorageError as cleanup_error:
                log.warning(
                    "Quarantine source cleanup failed after durable commit",
                    error=str(cleanup_error),
                )

        return result.with_completion(
            ProcessingStatus.COMPLETED,
            metadata={
                "file_hash": file_hash,
                "is_safe": analysis.is_safe,
                "findings_count": len(analysis.findings),
                "outbox_event_id": outbox_event.event_id if self._use_outbox else None,
                "quarantined": not analysis.is_safe,
            },
        )

    def _process_scanned_file(
        self,
        event: FileEvent,
        result: ProcessingResult,
        log: Any,
        claim: ProcessingClaim,
    ) -> ProcessingResult:
        timestamp = datetime.now(UTC).isoformat()
        record = self._create_metadata_record(event, timestamp)
        record.status = ProcessingStatus.COMPLETED
        record.processed_at = timestamp
        record.last_processed_event_id = event.event_id_str
        self._metadata.save(record, claim=claim)
        log.info("External scan event recorded")
        return result.with_completion(ProcessingStatus.COMPLETED)

    def _validate_event_location(self, event: FileEvent) -> None:
        if self._allowed_bucket and event.storage_location.bucket_name != self._allowed_bucket:
            raise NonRetryableError(
                message="Event references a bucket outside the configured trust boundary",
                error_code="UNTRUSTED_BUCKET",
            )
        if self._allowed_region and event.storage_location.region != self._allowed_region:
            raise NonRetryableError(
                message="Event storage region does not match the processor region",
                error_code="UNTRUSTED_REGION",
            )

    def _validate_actual_object(self, event: FileEvent, content: FileContent) -> None:
        actual_bytes = len(content.data)
        if content.content_length != actual_bytes:
            raise NonRetryableError(
                message="S3 ContentLength does not match the streamed byte count",
                error_code="S3_LENGTH_MISMATCH",
            )
        if actual_bytes != event.file_metadata.file_size_bytes:
            raise NonRetryableError(
                message="Actual S3 object size does not match the canonical event",
                error_code="FILE_SIZE_MISMATCH",
            )
        if content.encryption_algorithm != "aws:kms":
            raise NonRetryableError(
                message="S3 object is not encrypted with SSE-KMS",
                error_code="INVALID_S3_ENCRYPTION",
            )
        if content.kms_key_id != event.security_context.kms_key_id:
            raise NonRetryableError(
                message="Actual S3 KMS key ARN does not match the canonical event",
                error_code="KMS_KEY_MISMATCH",
            )

    def _quarantine_file(self, event: FileEvent, file_hash: str) -> StorageLocation:
        bucket = event.storage_location.bucket_name
        source_key = event.storage_location.object_key
        destination_key = f"{self._quarantine_prefix}/{event.file_id_str}/{file_hash}"
        self._storage.copy(bucket, source_key, bucket, destination_key)
        return StorageLocation(
            bucket_name=bucket,
            object_key=destination_key,
            region=event.storage_location.region,
        )

    def _analyze_file(
        self,
        content: FileContent,
        file_hash: str,
        log: Any,
    ) -> AnalysisResult:
        """Apply a transparent header policy; this is not an antivirus scanner."""
        findings: list[str] = []
        if not content.data:
            findings.append("Empty content is not accepted")
        elif content.data.startswith(b"MZ"):
            findings.append("Executable PE header is denied by policy")
        elif content.data.startswith(b"\x7fELF"):
            findings.append("Executable ELF header is denied by policy")
        is_safe = not findings
        log.info(
            "Header policy analysis completed",
            is_safe=is_safe,
            findings_count=len(findings),
            scan_engine="fsamp-header-policy/1",
        )
        return AnalysisResult(
            file_hash_sha256=file_hash,
            is_safe=is_safe,
            scan_engine="fsamp-header-policy/1",
            findings=findings,
        )

    def _create_metadata_record(self, event: FileEvent, timestamp: str) -> MetadataRecord:
        return MetadataRecord(
            file_id=event.file_id_str,
            timestamp=timestamp,
            correlation_id=event.correlation_id_str,
            original_filename=event.file_metadata.original_filename,
            file_size_bytes=event.file_metadata.file_size_bytes,
            mime_type=event.file_metadata.mime_type,
            bucket_name=event.storage_location.bucket_name,
            object_key=event.storage_location.object_key,
            status=ProcessingStatus.PENDING,
            checksum_sha256=event.file_metadata.checksum_sha256,
            is_encrypted=event.security_context.is_encrypted,
            encryption_algorithm=event.security_context.encryption_algorithm,
            kms_key_id=event.security_context.kms_key_id,
            created_at=timestamp,
        )

    def _persist_and_publish(
        self,
        record: MetadataRecord,
        event: FileEvent,
        outbox_event: OutboxEvent,
        claim: ProcessingClaim,
    ) -> None:
        if self._use_outbox and self._outbox is not None:
            self._outbox.save_with_outbox(record, outbox_event, claim=claim)
            return
        self._metadata.save(record, claim=claim)
        self._publisher.publish(event)

    def _handle_failure(
        self,
        event: FileEvent,
        error_message: str,
        error_code: str,
        *,
        retryable: bool,
        claim: ProcessingClaim | None,
    ) -> None:
        """Persist diagnostics atomically before allowing SQS/Lambda redrive."""
        if claim is None:
            return
        try:
            failed_at = datetime.now(UTC)
            record = self._create_metadata_record(event, failed_at.isoformat())
            record.status = ProcessingStatus.FAILED
            record.error_message = error_message[:2000]
            record.error_code = error_code[:100]
            record.processed_at = failed_at.isoformat()
            if not retryable:
                record.last_processed_event_id = event.event_id_str
            failure_event = event.with_new_event_type(
                EventType.PROCESSING_FAILED,
                failure=FailureDetails(
                    code=record.error_code,
                    message=record.error_message,
                    failed_at=failed_at,
                    retryable=retryable,
                ),
                idempotency_discriminator=(
                    f"claim:{claim.version}:{record.error_code}:{str(retryable).lower()}"
                ),
            )
            outbox_event = OutboxEvent.from_file_event(failure_event)
            self._persist_and_publish(record, failure_event, outbox_event, claim)
        except Exception as handler_error:
            logger.exception(
                "Failed to persist processing diagnostics",
                original_error=error_message,
                handler_error=str(handler_error),
                file_id=event.file_id_str,
            )
