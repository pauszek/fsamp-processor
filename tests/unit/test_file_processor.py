from __future__ import annotations

from datetime import UTC, datetime
from unittest.mock import MagicMock

import pytest

from processor.application.file_processor import FileProcessorService
from processor.domain.events import EventSource, EventType, FailureDetails, FileEvent
from processor.domain.exceptions import NonRetryableError, ProcessingError, StorageError
from processor.domain.models import FileContent, MetadataRecord, ProcessingStatus


@pytest.fixture
def dependencies(sample_file_event: FileEvent) -> dict[str, MagicMock]:
    storage = MagicMock()
    metadata = MagicMock()
    metadata.get_by_id.return_value = None
    publisher = MagicMock()
    crypto = MagicMock()
    outbox = MagicMock()
    storage.download.return_value = FileContent(
        data=b"x" * sample_file_event.file_metadata.file_size_bytes,
        content_type="application/pdf",
        content_length=sample_file_event.file_metadata.file_size_bytes,
        encryption_algorithm="aws:kms",
        kms_key_id=sample_file_event.security_context.kms_key_id,
    )
    crypto.compute_hash.return_value = sample_file_event.file_metadata.checksum_sha256
    return {
        "storage": storage,
        "metadata": metadata,
        "publisher": publisher,
        "crypto": crypto,
        "outbox": outbox,
    }


def build_service(
    dependencies: dict[str, MagicMock],
    *,
    use_outbox: bool = True,
    max_bytes: int = 100 * 1024 * 1024,
) -> FileProcessorService:
    return FileProcessorService(
        file_storage=dependencies["storage"],
        metadata_repo=dependencies["metadata"],
        event_publisher=dependencies["publisher"],
        crypto_provider=dependencies["crypto"],
        outbox_repo=dependencies["outbox"] if use_outbox else None,
        use_outbox_pattern=use_outbox,
        max_file_size_bytes=max_bytes,
        allowed_bucket_name="test-bucket",
        allowed_region=None,
    )


def test_success_writes_canonical_completion_event(
    sample_file_event: FileEvent,
    dependencies: dict[str, MagicMock],
) -> None:
    result = build_service(dependencies).handle(sample_file_event)

    assert result.status == ProcessingStatus.COMPLETED
    dependencies["outbox"].save_with_outbox.assert_called_once()
    record, outbox = dependencies["outbox"].save_with_outbox.call_args.args
    assert record.status == ProcessingStatus.COMPLETED
    assert record.last_processed_event_id == sample_file_event.event_id_str
    payload = outbox.to_sns_message()
    assert payload["schemaVersion"] == "1.2.0"
    assert payload["eventType"] == "ANALYSIS_COMPLETED"
    assert payload["source"] == "fsamp-processor"
    assert payload["processingResult"]["isSafe"] is True
    assert outbox.event_id == payload["eventId"]


def test_duplicate_input_does_not_redownload_or_emit(
    sample_file_event: FileEvent,
    dependencies: dict[str, MagicMock],
) -> None:
    dependencies["metadata"].get_by_id.return_value = MetadataRecord(
        file_id=sample_file_event.file_id_str,
        timestamp=datetime.now(UTC).isoformat(),
        correlation_id=sample_file_event.correlation_id_str,
        original_filename=sample_file_event.file_metadata.original_filename,
        file_size_bytes=sample_file_event.file_metadata.file_size_bytes,
        mime_type=sample_file_event.file_metadata.mime_type,
        bucket_name=sample_file_event.storage_location.bucket_name,
        object_key=sample_file_event.storage_location.object_key,
        status=ProcessingStatus.COMPLETED,
        last_processed_event_id=sample_file_event.event_id_str,
        is_safe=True,
    )
    result = build_service(dependencies).handle(sample_file_event)
    assert result.metadata["duplicate"] is True
    dependencies["storage"].download.assert_not_called()
    dependencies["outbox"].save_with_outbox.assert_not_called()


def test_non_retryable_failure_persists_full_failure_then_raises(
    sample_file_event: FileEvent,
    dependencies: dict[str, MagicMock],
) -> None:
    dependencies["storage"].download.return_value = FileContent(
        data=b"x",
        content_length=1,
        encryption_algorithm="aws:kms",
        kms_key_id=sample_file_event.security_context.kms_key_id,
    )
    with pytest.raises(NonRetryableError, match="Actual S3 object size"):
        build_service(dependencies).handle(sample_file_event)

    record, outbox = dependencies["outbox"].save_with_outbox.call_args.args
    assert record.status == ProcessingStatus.FAILED
    assert record.last_processed_event_id == sample_file_event.event_id_str
    payload = outbox.to_sns_message()
    assert payload["eventType"] == "PROCESSING_FAILED"
    assert payload["failure"]["retryable"] is False


def test_storage_failure_is_retryable_and_persists_diagnostics(
    sample_file_event: FileEvent,
    dependencies: dict[str, MagicMock],
) -> None:
    dependencies["storage"].download.side_effect = StorageError(
        "temporary", storage_type="s3", operation="download"
    )
    with pytest.raises(ProcessingError) as caught:
        build_service(dependencies).handle(sample_file_event)
    assert caught.value.retryable is True
    record, outbox = dependencies["outbox"].save_with_outbox.call_args.args
    assert record.last_processed_event_id is None
    assert outbox.to_sns_message()["failure"]["retryable"] is True


def test_actual_kms_metadata_is_enforced(
    sample_file_event: FileEvent,
    dependencies: dict[str, MagicMock],
) -> None:
    content = dependencies["storage"].download.return_value
    dependencies["storage"].download.return_value = FileContent(
        data=content.data,
        content_type=content.content_type,
        content_length=content.content_length,
        encryption_algorithm="aws:kms",
        kms_key_id="arn:aws:kms:us-west-2:123456789012:key/other",
    )
    with pytest.raises(NonRetryableError, match="KMS key ARN"):
        build_service(dependencies).handle(sample_file_event)


def test_untrusted_bucket_is_rejected_before_download(
    sample_file_event: FileEvent,
    dependencies: dict[str, MagicMock],
) -> None:
    service = build_service(dependencies)
    service._allowed_bucket = "another-bucket"
    with pytest.raises(NonRetryableError, match="trust boundary"):
        service.handle(sample_file_event)
    dependencies["storage"].download.assert_not_called()


def test_unsafe_header_is_moved_to_encrypted_quarantine(
    sample_file_event: FileEvent,
    dependencies: dict[str, MagicMock],
) -> None:
    size = sample_file_event.file_metadata.file_size_bytes
    dependencies["storage"].download.return_value = FileContent(
        data=b"MZ" + b"\0" * (size - 2),
        content_type="application/octet-stream",
        content_length=size,
        encryption_algorithm="aws:kms",
        kms_key_id=sample_file_event.security_context.kms_key_id,
    )
    result = build_service(dependencies).handle(sample_file_event)

    assert result.metadata["is_safe"] is False
    dependencies["storage"].copy.assert_called_once()
    dependencies["storage"].delete.assert_called_once()
    _, outbox = dependencies["outbox"].save_with_outbox.call_args.args
    payload = outbox.to_sns_message()
    assert payload["storageLocation"]["objectKey"].startswith("quarantine/")
    assert "Executable PE" in payload["processingResult"]["findings"][0]


def test_quarantine_source_is_not_deleted_before_transaction_commits(
    sample_file_event: FileEvent,
    dependencies: dict[str, MagicMock],
) -> None:
    size = sample_file_event.file_metadata.file_size_bytes
    dependencies["storage"].download.return_value = FileContent(
        data=b"MZ" + b"\0" * (size - 2),
        content_length=size,
        encryption_algorithm="aws:kms",
        kms_key_id=sample_file_event.security_context.kms_key_id,
    )
    dependencies["outbox"].save_with_outbox.side_effect = StorageError(
        "transaction failed",
        storage_type="dynamodb",
        operation="transact_write",
    )

    with pytest.raises(ProcessingError):
        build_service(dependencies).handle(sample_file_event)

    dependencies["storage"].copy.assert_called_once()
    dependencies["storage"].delete.assert_not_called()


def test_direct_mode_publishes_same_canonical_event(
    sample_file_event: FileEvent,
    dependencies: dict[str, MagicMock],
) -> None:
    build_service(dependencies, use_outbox=False).handle(sample_file_event)
    event = dependencies["publisher"].publish.call_args.args[0]
    assert event.event_type == EventType.ANALYSIS_COMPLETED
    assert event.processing_result is not None
    assert event.source == EventSource.PROCESSOR


def test_scanned_event_updates_current_metadata(
    sample_file_event: FileEvent,
    dependencies: dict[str, MagicMock],
) -> None:
    scanned = sample_file_event.with_new_event_type(EventType.FILE_SCANNED)
    result = build_service(dependencies).handle(scanned)
    assert result.status == ProcessingStatus.COMPLETED
    record = dependencies["metadata"].save.call_args.args[0]
    assert record.last_processed_event_id == scanned.event_id_str


def test_output_event_id_is_deterministic(sample_file_event: FileEvent) -> None:
    failure = FailureDetails(
        code="DENIED",
        message="denied",
        failed_at=datetime.now(UTC),
        retryable=False,
    )
    first = sample_file_event.with_new_event_type(EventType.PROCESSING_FAILED, failure=failure)
    second = sample_file_event.with_new_event_type(EventType.PROCESSING_FAILED, failure=failure)
    assert first.event_id == second.event_id
    assert first.event_id.version == 5
