from datetime import datetime
from unittest.mock import MagicMock

import pytest

from processor.application.file_processor import FileProcessorService
from processor.domain.events import EventType, FileEvent
from processor.domain.exceptions import NonRetryableError, ProcessingError, StorageError
from processor.domain.models import FileContent, ProcessingStatus


class TestFileProcessorServiceInit:
    def test_init_with_all_dependencies(self) -> None:
        storage = MagicMock()
        metadata = MagicMock()
        publisher = MagicMock()
        crypto = MagicMock()
        outbox = MagicMock()

        service = FileProcessorService(
            file_storage=storage,
            metadata_repo=metadata,
            event_publisher=publisher,
            crypto_provider=crypto,
            outbox_repo=outbox,
            max_file_size_bytes=50 * 1024 * 1024,
            use_outbox_pattern=True,
        )

        assert service._storage is storage
        assert service._metadata is metadata
        assert service._publisher is publisher
        assert service._crypto is crypto
        assert service._outbox is outbox
        assert service._max_file_size == 50 * 1024 * 1024
        assert service._use_outbox is True

    def test_init_without_outbox(self) -> None:
        storage = MagicMock()
        metadata = MagicMock()
        publisher = MagicMock()
        crypto = MagicMock()

        service = FileProcessorService(
            file_storage=storage,
            metadata_repo=metadata,
            event_publisher=publisher,
            crypto_provider=crypto,
        )

        assert service._outbox is None
        assert service._use_outbox is False


class TestFileProcessorServiceHandle:
    @pytest.fixture
    def mock_dependencies(self):
        storage = MagicMock()
        metadata = MagicMock()
        publisher = MagicMock()
        crypto = MagicMock()
        outbox = MagicMock()

        storage.download.return_value = FileContent(
            data=b"%PDF-1.4 Sample content",
            content_type="application/pdf",
            content_length=100,
        )
        crypto.compute_hash.return_value = "abc123hash"

        return {
            "storage": storage,
            "metadata": metadata,
            "publisher": publisher,
            "crypto": crypto,
            "outbox": outbox,
        }

    @pytest.fixture
    def service(self, mock_dependencies) -> FileProcessorService:
        return FileProcessorService(
            file_storage=mock_dependencies["storage"],
            metadata_repo=mock_dependencies["metadata"],
            event_publisher=mock_dependencies["publisher"],
            crypto_provider=mock_dependencies["crypto"],
            outbox_repo=mock_dependencies["outbox"],
        )

    def test_handle_file_uploaded_success(
        self, service: FileProcessorService, sample_file_event: FileEvent
    ) -> None:
        result = service.handle(sample_file_event)

        assert result.status == ProcessingStatus.COMPLETED
        service._storage.download.assert_called_once()
        service._crypto.compute_hash.assert_called()

    def test_handle_file_uploaded_with_outbox(
        self, service: FileProcessorService, sample_file_event: FileEvent
    ) -> None:
        result = service.handle(sample_file_event)

        assert result.status == ProcessingStatus.COMPLETED
        service._outbox.save_with_outbox.assert_called_once()

    def test_handle_file_too_large(self, mock_dependencies, sample_file_event: FileEvent) -> None:
        from processor.domain.events import FileMetadata

        large_file_event = FileEvent(
            schema_version=sample_file_event.schema_version,
            file_id=sample_file_event.file_id,
            event_id=sample_file_event.event_id,
            correlation_id=sample_file_event.correlation_id,
            timestamp=sample_file_event.timestamp,
            source=sample_file_event.source,
            event_type=sample_file_event.event_type,
            file_metadata=FileMetadata(
                original_filename=sample_file_event.file_metadata.original_filename,
                file_size_bytes=1000,  # Large file
                mime_type=sample_file_event.file_metadata.mime_type,
                checksum_sha256=sample_file_event.file_metadata.checksum_sha256,
            ),
            storage_location=sample_file_event.storage_location,
            security_context=sample_file_event.security_context,
        )

        service = FileProcessorService(
            file_storage=mock_dependencies["storage"],
            metadata_repo=mock_dependencies["metadata"],
            event_publisher=mock_dependencies["publisher"],
            crypto_provider=mock_dependencies["crypto"],
            max_file_size_bytes=100,  # Very small
        )

        with pytest.raises(NonRetryableError) as exc_info:
            service.handle(large_file_event)

        assert "File too large" in str(exc_info.value)

    def test_handle_storage_error(
        self, service: FileProcessorService, sample_file_event: FileEvent
    ) -> None:
        service._storage.download.side_effect = StorageError(
            message="S3 error",
            storage_type="s3",
            operation="download",
            resource="test",
        )

        with pytest.raises(ProcessingError) as exc_info:
            service.handle(sample_file_event)

        assert exc_info.value.retryable is True

    def test_handle_unexpected_error(
        self, service: FileProcessorService, sample_file_event: FileEvent
    ) -> None:
        service._storage.download.side_effect = RuntimeError("Unexpected")

        with pytest.raises(ProcessingError):
            service.handle(sample_file_event)

    def test_handle_scanned_file(
        self, service: FileProcessorService, sample_file_event: FileEvent
    ) -> None:
        scanned_event = FileEvent(
            schema_version=sample_file_event.schema_version,
            file_id=sample_file_event.file_id,
            event_id=sample_file_event.event_id,
            correlation_id=sample_file_event.correlation_id,
            timestamp=sample_file_event.timestamp,
            source=sample_file_event.source,
            event_type=EventType.FILE_SCANNED,
            file_metadata=sample_file_event.file_metadata,
            storage_location=sample_file_event.storage_location,
            security_context=sample_file_event.security_context,
        )

        result = service.handle(scanned_event)

        assert result.status == ProcessingStatus.COMPLETED
        service._metadata.save.assert_called()

    def test_handle_unhandled_event_type(
        self, service: FileProcessorService, sample_file_event: FileEvent
    ) -> None:
        analysis_event = FileEvent(
            schema_version=sample_file_event.schema_version,
            file_id=sample_file_event.file_id,
            event_id=sample_file_event.event_id,
            correlation_id=sample_file_event.correlation_id,
            timestamp=sample_file_event.timestamp,
            source=sample_file_event.source,
            event_type=EventType.ANALYSIS_COMPLETED,
            file_metadata=sample_file_event.file_metadata,
            storage_location=sample_file_event.storage_location,
            security_context=sample_file_event.security_context,
        )

        result = service.handle(analysis_event)

        assert result.status == ProcessingStatus.COMPLETED


class TestFileProcessorServiceProcessUploadedFile:
    @pytest.fixture
    def mock_dependencies(self):
        storage = MagicMock()
        metadata = MagicMock()
        publisher = MagicMock()
        crypto = MagicMock()
        outbox = MagicMock()

        storage.download.return_value = FileContent(
            data=b"%PDF-1.4 Sample content",
            content_type="application/pdf",
            content_length=100,
        )
        crypto.compute_hash.return_value = "abc123hash"

        return {
            "storage": storage,
            "metadata": metadata,
            "publisher": publisher,
            "crypto": crypto,
            "outbox": outbox,
        }

    def test_process_uploaded_file_downloads_from_s3(
        self, mock_dependencies, sample_file_event: FileEvent
    ) -> None:
        service = FileProcessorService(
            file_storage=mock_dependencies["storage"],
            metadata_repo=mock_dependencies["metadata"],
            event_publisher=mock_dependencies["publisher"],
            crypto_provider=mock_dependencies["crypto"],
        )

        service.handle(sample_file_event)

        mock_dependencies["storage"].download.assert_called_once_with(
            bucket_name=sample_file_event.storage_location.bucket_name,
            object_key=sample_file_event.storage_location.object_key,
        )

    def test_process_uploaded_file_computes_hash(
        self, mock_dependencies, sample_file_event: FileEvent
    ) -> None:
        service = FileProcessorService(
            file_storage=mock_dependencies["storage"],
            metadata_repo=mock_dependencies["metadata"],
            event_publisher=mock_dependencies["publisher"],
            crypto_provider=mock_dependencies["crypto"],
        )

        service.handle(sample_file_event)

        mock_dependencies["crypto"].compute_hash.assert_called()


class TestFileProcessorServiceAnalyzeFile:
    @pytest.fixture
    def service(self) -> FileProcessorService:
        storage = MagicMock()
        metadata = MagicMock()
        publisher = MagicMock()
        crypto = MagicMock()
        crypto.compute_hash.return_value = "test-hash"

        return FileProcessorService(
            file_storage=storage,
            metadata_repo=metadata,
            event_publisher=publisher,
            crypto_provider=crypto,
        )

    def test_analyze_file_empty_file(self, service: FileProcessorService) -> None:
        content = FileContent(data=b"", content_type="text/plain", content_length=0)
        log = MagicMock()

        result = service._analyze_file(content, log)

        assert "File is empty" in result.findings

    def test_analyze_file_pe_executable(self, service: FileProcessorService) -> None:
        content = FileContent(
            data=b"MZ" + b"\x00" * 100, content_type="application/octet-stream", content_length=102
        )
        log = MagicMock()

        result = service._analyze_file(content, log)

        assert any("executable" in f.lower() for f in result.findings)
        assert not result.is_safe

    def test_analyze_file_elf_executable(self, service: FileProcessorService) -> None:
        content = FileContent(
            data=b"\x7fELF" + b"\x00" * 100,
            content_type="application/octet-stream",
            content_length=104,
        )
        log = MagicMock()

        result = service._analyze_file(content, log)

        assert any("executable" in f.lower() for f in result.findings)
        assert not result.is_safe

    def test_analyze_file_pdf(self, service: FileProcessorService) -> None:
        content = FileContent(
            data=b"%PDF-1.4" + b"\x00" * 100, content_type="application/pdf", content_length=108
        )
        log = MagicMock()

        result = service._analyze_file(content, log)

        assert result.is_safe

    def test_analyze_file_safe_file(self, service: FileProcessorService) -> None:
        content = FileContent(data=b"Hello, world!", content_type="text/plain", content_length=13)
        log = MagicMock()

        result = service._analyze_file(content, log)

        assert result.is_safe


class TestFileProcessorServiceCreateMetadataRecord:
    @pytest.fixture
    def service(self) -> FileProcessorService:
        return FileProcessorService(
            file_storage=MagicMock(),
            metadata_repo=MagicMock(),
            event_publisher=MagicMock(),
            crypto_provider=MagicMock(),
        )

    def test_create_metadata_record(
        self, service: FileProcessorService, sample_file_event: FileEvent
    ) -> None:
        timestamp = datetime.utcnow().isoformat()

        record = service._create_metadata_record(sample_file_event, timestamp)

        assert record.file_id == sample_file_event.file_id_str
        assert str(record.correlation_id) == str(sample_file_event.correlation_id)
        assert record.original_filename == sample_file_event.file_metadata.original_filename
        assert record.file_size_bytes == sample_file_event.file_metadata.file_size_bytes
        assert record.bucket_name == sample_file_event.storage_location.bucket_name
        assert record.status == ProcessingStatus.PENDING


class TestFileProcessorServiceHandleFailure:
    @pytest.fixture
    def mock_dependencies(self):
        return {
            "storage": MagicMock(),
            "metadata": MagicMock(),
            "publisher": MagicMock(),
            "crypto": MagicMock(),
            "outbox": MagicMock(),
        }

    def test_handle_failure_with_outbox(
        self, mock_dependencies, sample_file_event: FileEvent
    ) -> None:
        service = FileProcessorService(
            file_storage=mock_dependencies["storage"],
            metadata_repo=mock_dependencies["metadata"],
            event_publisher=mock_dependencies["publisher"],
            crypto_provider=mock_dependencies["crypto"],
            outbox_repo=mock_dependencies["outbox"],
        )

        service._handle_failure(sample_file_event, "Test error", "TEST_ERROR")

        mock_dependencies["outbox"].save_with_outbox.assert_called_once()

    def test_handle_failure_without_outbox(
        self, mock_dependencies, sample_file_event: FileEvent
    ) -> None:
        service = FileProcessorService(
            file_storage=mock_dependencies["storage"],
            metadata_repo=mock_dependencies["metadata"],
            event_publisher=mock_dependencies["publisher"],
            crypto_provider=mock_dependencies["crypto"],
        )

        service._handle_failure(sample_file_event, "Test error", "TEST_ERROR")

        mock_dependencies["metadata"].save.assert_called_once()
        mock_dependencies["publisher"].publish.assert_called_once()

    def test_handle_failure_exception_in_handler(
        self, mock_dependencies, sample_file_event: FileEvent
    ) -> None:
        mock_dependencies["metadata"].save.side_effect = Exception("Save failed")

        service = FileProcessorService(
            file_storage=mock_dependencies["storage"],
            metadata_repo=mock_dependencies["metadata"],
            event_publisher=mock_dependencies["publisher"],
            crypto_provider=mock_dependencies["crypto"],
        )

        service._handle_failure(sample_file_event, "Test error", "TEST_ERROR")


class TestFileProcessorServiceDirectPublishing:
    @pytest.fixture
    def mock_dependencies(self):
        storage = MagicMock()
        metadata = MagicMock()
        publisher = MagicMock()
        crypto = MagicMock()

        storage.download.return_value = FileContent(
            data=b"Test content",
            content_type="text/plain",
            content_length=12,
        )
        crypto.compute_hash.return_value = "test-hash"

        return {
            "storage": storage,
            "metadata": metadata,
            "publisher": publisher,
            "crypto": crypto,
        }

    def test_direct_publishing_mode(self, mock_dependencies, sample_file_event: FileEvent) -> None:
        service = FileProcessorService(
            file_storage=mock_dependencies["storage"],
            metadata_repo=mock_dependencies["metadata"],
            event_publisher=mock_dependencies["publisher"],
            crypto_provider=mock_dependencies["crypto"],
            use_outbox_pattern=False,
        )

        result = service.handle(sample_file_event)

        assert result.status == ProcessingStatus.COMPLETED
        mock_dependencies["publisher"].publish.assert_called_once()
