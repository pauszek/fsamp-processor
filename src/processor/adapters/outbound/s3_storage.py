"""
S3 implementation of the FileStorage port.
Handles file downloads/uploads with server-side encryption.
"""

from typing import TYPE_CHECKING, Any

import structlog
from botocore.exceptions import ClientError

from processor.adapters.outbound.aws_retry import aws_retry
from processor.domain.exceptions import NonRetryableError, StorageError
from processor.domain.models import FileContent
from processor.ports.outbound import FileStorage

if TYPE_CHECKING:
    from mypy_boto3_s3 import S3Client

logger = structlog.get_logger(__name__)


class S3FileStorage(FileStorage):
    """
    S3 File Storage adapter.

    Features:
    - Server-side encryption with KMS
    - Retry logic with exponential backoff
    - Presigned URL generation
    - Content-type detection
    """

    def __init__(
        self,
        s3_client: S3Client,
        default_kms_key_id: str | None = None,
    ) -> None:
        """
        Initialize S3 File Storage.

        Args:
            s3_client: Boto3 S3 client.
            default_kms_key_id: Default KMS key for encryption.
        """
        self._client = s3_client
        self._default_kms_key_id = default_kms_key_id
        logger.info("S3 File Storage initialized")

    @aws_retry()
    def download(
        self,
        bucket_name: str,
        object_key: str,
        max_bytes: int | None = None,
    ) -> FileContent:
        """Download a file in bounded chunks and retain actual S3 metadata."""
        log = logger.bind(bucket=bucket_name, key=object_key)

        try:
            log.info("Downloading file from S3")

            response = self._client.get_object(
                Bucket=bucket_name,
                Key=object_key,
            )

            declared_length = int(response.get("ContentLength", 0))
            body = response["Body"]
            try:
                if max_bytes is not None and declared_length > max_bytes:
                    raise NonRetryableError(
                        message=(
                            f"S3 object is {declared_length} bytes, exceeding the "
                            f"configured maximum of {max_bytes} bytes"
                        ),
                        error_code="ACTUAL_FILE_TOO_LARGE",
                    )

                chunks = bytearray()
                while True:
                    chunk = body.read(1024 * 1024)
                    if not chunk:
                        break
                    chunks.extend(chunk)
                    if max_bytes is not None and len(chunks) > max_bytes:
                        raise NonRetryableError(
                            message=("S3 response exceeded the configured maximum while streaming"),
                            error_code="ACTUAL_FILE_TOO_LARGE",
                        )
                data = bytes(chunks)
            finally:
                close = getattr(body, "close", None)
                if callable(close):
                    close()

            file_content = FileContent(
                data=data,
                content_type=response.get("ContentType"),
                content_length=declared_length or len(data),
                etag=response.get("ETag", "").strip('"'),
                encryption_algorithm=response.get("ServerSideEncryption"),
                kms_key_id=response.get("SSEKMSKeyId"),
            )

            log.info(
                "File downloaded successfully",
                size_bytes=file_content.content_length,
                content_type=file_content.content_type,
                encrypted=file_content.is_encrypted,
            )

            return file_content

        except ClientError as e:
            error_code = e.response.get("Error", {}).get("Code", "Unknown")

            if error_code == "NoSuchKey":
                log.warning("File not found in S3")
                raise StorageError(
                    message=f"File not found: s3://{bucket_name}/{object_key}",
                    storage_type="s3",
                    operation="download",
                    resource=f"s3://{bucket_name}/{object_key}",
                    cause=e,
                ) from e

            if error_code == "AccessDenied":
                log.error("Access denied to S3 file")
                raise StorageError(
                    message=f"Access denied: s3://{bucket_name}/{object_key}",
                    storage_type="s3",
                    operation="download",
                    resource=f"s3://{bucket_name}/{object_key}",
                    cause=e,
                ) from e

            log.exception("S3 download failed", error_code=error_code)
            raise StorageError(
                message=f"Failed to download file: {e}",
                storage_type="s3",
                operation="download",
                resource=f"s3://{bucket_name}/{object_key}",
                cause=e,
            ) from e

    @aws_retry()
    def upload(
        self,
        bucket_name: str,
        object_key: str,
        data: bytes,
        content_type: str | None = None,
        metadata: dict[str, str] | None = None,
    ) -> str:
        """Upload a file to S3 with server-side encryption."""
        log = logger.bind(bucket=bucket_name, key=object_key, size_bytes=len(data))

        try:
            log.info("Uploading file to S3")

            put_params: dict[str, Any] = {
                "Bucket": bucket_name,
                "Key": object_key,
                "Body": data,
            }

            if content_type:
                put_params["ContentType"] = content_type

            if metadata:
                put_params["Metadata"] = metadata

            if self._default_kms_key_id:
                put_params["ServerSideEncryption"] = "aws:kms"
                put_params["SSEKMSKeyId"] = self._default_kms_key_id
            else:
                raise StorageError(
                    message="KMS key is required for S3 uploads (FedRAMP SC-13). "
                    "Set KMS_KEY_ID environment variable.",
                    storage_type="s3",
                    operation="upload",
                    resource=f"s3://{bucket_name}/{object_key}",
                )

            response = self._client.put_object(**put_params)
            etag = response.get("ETag", "").strip('"')

            log.info("File uploaded successfully", etag=etag)
            return etag

        except ClientError as e:
            log.exception("S3 upload failed")
            raise StorageError(
                message=f"Failed to upload file: {e}",
                storage_type="s3",
                operation="upload",
                resource=f"s3://{bucket_name}/{object_key}",
                cause=e,
            ) from e

    def exists(self, bucket_name: str, object_key: str) -> bool:
        """Check if a file exists in S3."""
        try:
            self._client.head_object(Bucket=bucket_name, Key=object_key)
            return True
        except ClientError as e:
            if e.response.get("Error", {}).get("Code") == "404":
                return False
            raise StorageError(
                message=f"Failed to check file existence: {e}",
                storage_type="s3",
                operation="exists",
                resource=f"s3://{bucket_name}/{object_key}",
                cause=e,
            ) from e

    @aws_retry()
    def delete(self, bucket_name: str, object_key: str) -> None:
        """Delete a file from S3."""
        log = logger.bind(bucket=bucket_name, key=object_key)

        try:
            log.info("Deleting file from S3")
            self._client.delete_object(Bucket=bucket_name, Key=object_key)
            log.info("File deleted successfully")

        except ClientError as e:
            log.exception("S3 delete failed")
            raise StorageError(
                message=f"Failed to delete file: {e}",
                storage_type="s3",
                operation="delete",
                resource=f"s3://{bucket_name}/{object_key}",
                cause=e,
            ) from e

    @aws_retry()
    def copy(
        self,
        source_bucket: str,
        source_key: str,
        dest_bucket: str,
        dest_key: str,
    ) -> str:
        """Copy a file within S3."""
        log = logger.bind(
            source=f"s3://{source_bucket}/{source_key}",
            dest=f"s3://{dest_bucket}/{dest_key}",
        )

        try:
            log.info("Copying file in S3")

            copy_params: dict[str, Any] = {
                "Bucket": dest_bucket,
                "Key": dest_key,
                "CopySource": {"Bucket": source_bucket, "Key": source_key},
            }

            if not self._default_kms_key_id:
                raise StorageError(
                    message="KMS key is required for encrypted S3 copies",
                    storage_type="s3",
                    operation="copy",
                    resource=f"s3://{source_bucket}/{source_key}",
                )
            copy_params["ServerSideEncryption"] = "aws:kms"
            copy_params["SSEKMSKeyId"] = self._default_kms_key_id

            response = self._client.copy_object(**copy_params)
            etag = response.get("CopyObjectResult", {}).get("ETag", "").strip('"')

            log.info("File copied successfully", etag=etag)
            return etag

        except ClientError as e:
            log.exception("S3 copy failed")
            raise StorageError(
                message=f"Failed to copy file: {e}",
                storage_type="s3",
                operation="copy",
                resource=f"s3://{source_bucket}/{source_key}",
                cause=e,
            ) from e
