# =============================================================================
# Integration Tests for S3 Storage Adapter
# =============================================================================
"""Tests for S3FileStorage with mocked AWS."""

import pytest

from processor.adapters.outbound.s3_storage import S3FileStorage
from processor.domain.exceptions import StorageError


class TestS3FileStorage:
    """Integration tests for S3 file storage adapter."""

    @pytest.fixture
    def storage(self, s3_client, test_bucket: str) -> S3FileStorage:
        """Create S3 storage adapter."""
        return S3FileStorage(s3_client=s3_client)

    def test_upload_and_download(self, storage: S3FileStorage, test_bucket: str) -> None:
        """Test uploading and downloading a file."""
        content = b"Test file content for S3 storage"
        object_key = "test/upload-download.txt"

        # Upload
        etag = storage.upload(
            bucket_name=test_bucket,
            object_key=object_key,
            data=content,
            content_type="text/plain",
        )

        assert etag is not None

        # Download
        file_content = storage.download(
            bucket_name=test_bucket,
            object_key=object_key,
        )

        assert file_content.data == content
        assert file_content.content_type == "text/plain"

    def test_download_nonexistent_file(self, storage: S3FileStorage, test_bucket: str) -> None:
        """Test downloading a file that doesn't exist."""
        with pytest.raises(StorageError) as exc_info:
            storage.download(
                bucket_name=test_bucket,
                object_key="nonexistent/file.txt",
            )

        assert "not found" in str(exc_info.value).lower()

    def test_exists_check(self, storage: S3FileStorage, test_bucket: str) -> None:
        """Test checking if file exists."""
        object_key = "test/exists-check.txt"

        # File doesn't exist yet
        assert storage.exists(test_bucket, object_key) is False

        # Upload file
        storage.upload(test_bucket, object_key, b"content")

        # Now it exists
        assert storage.exists(test_bucket, object_key) is True

    def test_delete_file(self, storage: S3FileStorage, test_bucket: str) -> None:
        """Test deleting a file."""
        object_key = "test/to-delete.txt"

        # Upload
        storage.upload(test_bucket, object_key, b"delete me")
        assert storage.exists(test_bucket, object_key) is True

        # Delete
        storage.delete(test_bucket, object_key)
        assert storage.exists(test_bucket, object_key) is False

    def test_upload_with_metadata(self, storage: S3FileStorage, test_bucket: str) -> None:
        """Test uploading with custom metadata."""
        object_key = "test/with-metadata.txt"
        metadata = {
            "correlation-id": "test-123",
            "uploader": "test-suite",
        }

        storage.upload(
            bucket_name=test_bucket,
            object_key=object_key,
            data=b"content with metadata",
            metadata=metadata,
        )

        # Verify upload succeeded
        assert storage.exists(test_bucket, object_key) is True

    def test_presigned_url_generation(self, storage: S3FileStorage, test_bucket: str) -> None:
        """Test generating presigned URLs."""
        object_key = "test/presigned.txt"
        storage.upload(test_bucket, object_key, b"presigned content")

        url = storage.get_presigned_url(
            bucket_name=test_bucket,
            object_key=object_key,
            expiration_seconds=3600,
        )

        assert url is not None
        assert test_bucket in url
        assert object_key in url
