from unittest.mock import MagicMock

import pytest
from botocore.exceptions import ClientError

from processor.adapters.outbound.s3_storage import S3FileStorage
from processor.domain.exceptions import StorageError
from processor.domain.models import FileContent


class TestS3FileStorageInit:
    def test_init_with_kms_key(self) -> None:
        client = MagicMock()
        storage = S3FileStorage(
            s3_client=client,
            default_kms_key_id="test-key-id",
        )

        assert storage._client is client
        assert storage._default_kms_key_id == "test-key-id"

    def test_init_without_kms_key(self) -> None:
        client = MagicMock()
        storage = S3FileStorage(s3_client=client)

        assert storage._default_kms_key_id is None


class TestS3FileStorageDownload:
    @pytest.fixture
    def storage(self) -> S3FileStorage:
        client = MagicMock()
        client.get_object.return_value = {
            "Body": MagicMock(read=MagicMock(side_effect=[b"test content", b""])),
            "ContentType": "text/plain",
            "ContentLength": 12,
            "ETag": '"abc123"',
            "ServerSideEncryption": "aws:kms",
            "SSEKMSKeyId": "test-key",
        }
        return S3FileStorage(s3_client=client)

    def test_download_success(self, storage: S3FileStorage) -> None:
        result = storage.download("test-bucket", "test-key")

        assert isinstance(result, FileContent)
        assert result.data == b"test content"
        assert result.content_type == "text/plain"
        assert result.etag == "abc123"

    def test_download_no_such_key(self) -> None:
        client = MagicMock()
        client.get_object.side_effect = ClientError(
            {"Error": {"Code": "NoSuchKey", "Message": "Not found"}},
            "GetObject",
        )
        storage = S3FileStorage(s3_client=client)

        with pytest.raises(StorageError) as exc_info:
            storage.download("test-bucket", "missing-key")

        assert "File not found" in str(exc_info.value)

    def test_download_access_denied(self) -> None:
        client = MagicMock()
        client.get_object.side_effect = ClientError(
            {"Error": {"Code": "AccessDenied", "Message": "Access denied"}},
            "GetObject",
        )
        storage = S3FileStorage(s3_client=client)

        with pytest.raises(StorageError) as exc_info:
            storage.download("test-bucket", "test-key")

        assert "Access denied" in str(exc_info.value)

    def test_download_other_error(self) -> None:
        client = MagicMock()
        client.get_object.side_effect = ClientError(
            {"Error": {"Code": "InternalError", "Message": "Internal error"}},
            "GetObject",
        )
        storage = S3FileStorage(s3_client=client)

        with pytest.raises(StorageError) as exc_info:
            storage.download("test-bucket", "test-key")

        assert "Failed to download" in str(exc_info.value)


class TestS3FileStorageUpload:
    @pytest.fixture
    def storage(self) -> S3FileStorage:
        client = MagicMock()
        client.put_object.return_value = {"ETag": '"def456"'}
        return S3FileStorage(s3_client=client, default_kms_key_id="test-key")

    def test_upload_success(self, storage: S3FileStorage) -> None:
        etag = storage.upload(
            bucket_name="test-bucket",
            object_key="test-key",
            data=b"test content",
            content_type="text/plain",
        )

        assert etag == "def456"
        storage._client.put_object.assert_called_once()

    def test_upload_with_metadata(self, storage: S3FileStorage) -> None:
        storage.upload(
            bucket_name="test-bucket",
            object_key="test-key",
            data=b"test content",
            metadata={"custom": "value"},
        )

        call_kwargs = storage._client.put_object.call_args.kwargs
        assert call_kwargs["Metadata"] == {"custom": "value"}

    def test_upload_with_kms_encryption(self, storage: S3FileStorage) -> None:
        storage.upload(
            bucket_name="test-bucket",
            object_key="test-key",
            data=b"test content",
        )

        call_kwargs = storage._client.put_object.call_args.kwargs
        assert call_kwargs["ServerSideEncryption"] == "aws:kms"
        assert call_kwargs["SSEKMSKeyId"] == "test-key"

    def test_upload_without_kms_raises_error(self) -> None:
        client = MagicMock()
        storage = S3FileStorage(s3_client=client)

        with pytest.raises(StorageError, match="KMS key is required"):
            storage.upload(
                bucket_name="test-bucket",
                object_key="test-key",
                data=b"test content",
            )

        client.put_object.assert_not_called()

    def test_upload_error(self) -> None:
        client = MagicMock()
        client.put_object.side_effect = ClientError(
            {"Error": {"Code": "InternalError"}},
            "PutObject",
        )
        storage = S3FileStorage(s3_client=client, default_kms_key_id="test-key")

        with pytest.raises(StorageError):
            storage.upload("bucket", "key", b"data")


class TestS3FileStorageExists:
    def test_exists_true(self) -> None:
        client = MagicMock()
        storage = S3FileStorage(s3_client=client)

        result = storage.exists("test-bucket", "test-key")

        assert result is True
        client.head_object.assert_called_once()

    def test_exists_false(self) -> None:
        client = MagicMock()
        client.head_object.side_effect = ClientError(
            {"Error": {"Code": "404"}},
            "HeadObject",
        )
        storage = S3FileStorage(s3_client=client)

        result = storage.exists("test-bucket", "missing-key")

        assert result is False

    def test_exists_other_error(self) -> None:
        client = MagicMock()
        client.head_object.side_effect = ClientError(
            {"Error": {"Code": "AccessDenied"}},
            "HeadObject",
        )
        storage = S3FileStorage(s3_client=client)

        with pytest.raises(StorageError):
            storage.exists("test-bucket", "test-key")


class TestS3FileStorageDelete:
    def test_delete_success(self) -> None:
        client = MagicMock()
        storage = S3FileStorage(s3_client=client)

        storage.delete("test-bucket", "test-key")

        client.delete_object.assert_called_once_with(
            Bucket="test-bucket",
            Key="test-key",
        )

    def test_delete_error(self) -> None:
        client = MagicMock()
        client.delete_object.side_effect = ClientError(
            {"Error": {"Code": "InternalError"}},
            "DeleteObject",
        )
        storage = S3FileStorage(s3_client=client)

        with pytest.raises(StorageError):
            storage.delete("test-bucket", "test-key")


class TestS3FileStorageCopy:
    @pytest.fixture
    def storage(self) -> S3FileStorage:
        client = MagicMock()
        client.copy_object.return_value = {"CopyObjectResult": {"ETag": '"copied"'}}
        return S3FileStorage(s3_client=client, default_kms_key_id="test-key")

    def test_copy_success(self, storage: S3FileStorage) -> None:
        etag = storage.copy(
            source_bucket="source-bucket",
            source_key="source-key",
            dest_bucket="dest-bucket",
            dest_key="dest-key",
        )

        assert etag == "copied"
        storage._client.copy_object.assert_called_once()

    def test_copy_with_kms(self, storage: S3FileStorage) -> None:
        storage.copy(
            source_bucket="source",
            source_key="key",
            dest_bucket="dest",
            dest_key="key",
        )

        call_kwargs = storage._client.copy_object.call_args.kwargs
        assert call_kwargs["ServerSideEncryption"] == "aws:kms"
        assert call_kwargs["SSEKMSKeyId"] == "test-key"

    def test_copy_error(self) -> None:
        client = MagicMock()
        client.copy_object.side_effect = ClientError(
            {"Error": {"Code": "InternalError"}},
            "CopyObject",
        )
        storage = S3FileStorage(s3_client=client)

        with pytest.raises(StorageError):
            storage.copy("src", "key", "dst", "key")
