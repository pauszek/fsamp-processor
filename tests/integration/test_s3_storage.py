# =============================================================================
# Integration Tests for S3 Storage Adapter
# =============================================================================
"""Tests for S3FileStorage against LocalStack (real AWS-compatible service)."""

import pytest

from processor.adapters.outbound.s3_storage import S3FileStorage
from processor.domain.exceptions import StorageError

pytestmark = pytest.mark.integration


class TestS3FileStorage:
    """Integration tests for S3 file storage adapter using LocalStack."""

    @pytest.fixture
    def storage(self, localstack_s3_client, localstack_kms_key_id) -> S3FileStorage:
        """Create S3 storage adapter with LocalStack client and KMS key."""
        return S3FileStorage(
            s3_client=localstack_s3_client,
            default_kms_key_id=localstack_kms_key_id,
        )

    def test_upload_and_download(self, storage: S3FileStorage, localstack_bucket: str) -> None:
        """Test uploading and downloading a file."""
        content = b"Test file content for S3 storage"
        object_key = "test/upload-download.txt"

        # Upload
        etag = storage.upload(
            bucket_name=localstack_bucket,
            object_key=object_key,
            data=content,
            content_type="text/plain",
        )

        assert etag is not None

        # Download
        file_content = storage.download(
            bucket_name=localstack_bucket,
            object_key=object_key,
        )

        assert file_content.data == content
        assert file_content.content_type == "text/plain"

    def test_download_nonexistent_file(
        self, storage: S3FileStorage, localstack_bucket: str
    ) -> None:
        """Test downloading a file that doesn't exist."""
        with pytest.raises(StorageError) as exc_info:
            storage.download(
                bucket_name=localstack_bucket,
                object_key="nonexistent/file.txt",
            )

        assert "not found" in str(exc_info.value).lower()

    def test_exists_check(self, storage: S3FileStorage, localstack_bucket: str) -> None:
        """Test checking if file exists."""
        object_key = "test/exists-check.txt"

        # File doesn't exist yet
        assert storage.exists(localstack_bucket, object_key) is False

        # Upload file
        storage.upload(localstack_bucket, object_key, b"content")

        # Now it exists
        assert storage.exists(localstack_bucket, object_key) is True

    def test_delete_file(self, storage: S3FileStorage, localstack_bucket: str) -> None:
        """Test deleting a file."""
        object_key = "test/to-delete.txt"

        # Upload
        storage.upload(localstack_bucket, object_key, b"delete me")
        assert storage.exists(localstack_bucket, object_key) is True

        # Delete
        storage.delete(localstack_bucket, object_key)
        assert storage.exists(localstack_bucket, object_key) is False

    def test_upload_with_metadata(self, storage: S3FileStorage, localstack_bucket: str) -> None:
        """Test uploading with custom metadata."""
        object_key = "test/with-metadata.txt"
        metadata = {
            "correlation-id": "test-123",
            "uploader": "test-suite",
        }

        storage.upload(
            bucket_name=localstack_bucket,
            object_key=object_key,
            data=b"content with metadata",
            metadata=metadata,
        )

        # Verify upload succeeded
        assert storage.exists(localstack_bucket, object_key) is True

    def test_presigned_url_generation(self, storage: S3FileStorage, localstack_bucket: str) -> None:
        """Test generating presigned URLs."""
        object_key = "test/presigned.txt"
        storage.upload(localstack_bucket, object_key, b"presigned content")

        url = storage.get_presigned_url(
            bucket_name=localstack_bucket,
            object_key=object_key,
            expiration_seconds=3600,
        )

        assert url is not None
        assert localstack_bucket in url
        assert object_key in url
