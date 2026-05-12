# =============================================================================
# Integration Tests for KMS Encryption with LocalStack Pro
# =============================================================================
"""
KMS key management and envelope encryption integration tests.

Validates FedRAMP SC-12 (Cryptographic Key Establishment and Management)
and SC-28 (Protection of Information at Rest) controls using LocalStack Pro.

Usage:
    pytest tests/integration/test_kms_localstack.py -m integration
"""

import boto3
import pytest


@pytest.mark.integration
class TestKmsKeyManagement:
    """Tests for KMS key lifecycle operations."""

    def test_create_symmetric_key(self, localstack_kms_client: boto3.client) -> None:
        """KMS should create a symmetric AES-256 key (FIPS 140-2 L3)."""
        response = localstack_kms_client.create_key(
            Description="FSAMP integration test key",
            KeyUsage="ENCRYPT_DECRYPT",
            KeySpec="SYMMETRIC_DEFAULT",
            Tags=[
                {"TagKey": "Project", "TagValue": "fsamp"},
                {"TagKey": "Environment", "TagValue": "test"},
            ],
        )

        key_metadata = response["KeyMetadata"]
        assert key_metadata["KeyState"] == "Enabled"
        assert key_metadata["KeyUsage"] == "ENCRYPT_DECRYPT"
        assert key_metadata["KeySpec"] == "SYMMETRIC_DEFAULT"
        assert key_metadata["KeyManager"] == "CUSTOMER"

    def test_create_key_alias(self, localstack_kms_client: boto3.client) -> None:
        """KMS aliases should resolve to the correct key ARN."""
        # Create key
        key_response = localstack_kms_client.create_key(
            Description="Alias test key",
            KeyUsage="ENCRYPT_DECRYPT",
        )
        key_id = key_response["KeyMetadata"]["KeyId"]

        # Create alias
        alias_name = "alias/fsamp-processor-test"
        try:
            localstack_kms_client.delete_alias(AliasName=alias_name)
        except localstack_kms_client.exceptions.NotFoundException:
            pass

        localstack_kms_client.create_alias(
            AliasName=alias_name,
            TargetKeyId=key_id,
        )

        # Describe via alias
        described = localstack_kms_client.describe_key(KeyId=alias_name)
        assert described["KeyMetadata"]["KeyId"] == key_id

    def test_describe_key_metadata(self, localstack_kms_client: boto3.client) -> None:
        """DescribeKey should return complete key metadata."""
        key_response = localstack_kms_client.create_key(
            Description="Metadata test key",
            KeyUsage="ENCRYPT_DECRYPT",
        )
        key_id = key_response["KeyMetadata"]["KeyId"]

        described = localstack_kms_client.describe_key(KeyId=key_id)
        metadata = described["KeyMetadata"]

        assert metadata["Enabled"] is True
        assert "Arn" in metadata
        assert "CreationDate" in metadata
        assert metadata["Description"] == "Metadata test key"

    def test_disable_and_enable_key(self, localstack_kms_client: boto3.client) -> None:
        """Key disable/enable lifecycle for key rotation (SC-12)."""
        key_response = localstack_kms_client.create_key(
            Description="Lifecycle test key",
        )
        key_id = key_response["KeyMetadata"]["KeyId"]

        # Disable
        localstack_kms_client.disable_key(KeyId=key_id)
        described = localstack_kms_client.describe_key(KeyId=key_id)
        assert described["KeyMetadata"]["KeyState"] == "Disabled"

        # Re-enable
        localstack_kms_client.enable_key(KeyId=key_id)
        described = localstack_kms_client.describe_key(KeyId=key_id)
        assert described["KeyMetadata"]["KeyState"] == "Enabled"


@pytest.mark.integration
class TestKmsEnvelopeEncryption:
    """Tests for envelope encryption pattern used by FSAMP."""

    @pytest.fixture
    def kms_key_id(self, localstack_kms_client: boto3.client) -> str:
        """Create a KMS key for envelope encryption tests."""
        response = localstack_kms_client.create_key(
            Description="Envelope encryption test key",
            KeyUsage="ENCRYPT_DECRYPT",
        )
        return response["KeyMetadata"]["KeyId"]

    def test_generate_data_key(self, localstack_kms_client: boto3.client, kms_key_id: str) -> None:
        """GenerateDataKey should return plaintext and ciphertext blob."""
        response = localstack_kms_client.generate_data_key(
            KeyId=kms_key_id,
            KeySpec="AES_256",
        )

        assert "Plaintext" in response
        assert "CiphertextBlob" in response
        assert len(response["Plaintext"]) == 32  # AES-256 = 32 bytes
        assert response["KeyId"] is not None

    def test_encrypt_decrypt_roundtrip(
        self, localstack_kms_client: boto3.client, kms_key_id: str
    ) -> None:
        """Encrypt → Decrypt should preserve plaintext integrity."""
        plaintext = b"FedRAMP SC-28: sensitive data at rest"

        # Encrypt
        encrypt_response = localstack_kms_client.encrypt(
            KeyId=kms_key_id,
            Plaintext=plaintext,
            EncryptionAlgorithm="SYMMETRIC_DEFAULT",
        )
        ciphertext = encrypt_response["CiphertextBlob"]
        assert ciphertext != plaintext

        # Decrypt
        decrypt_response = localstack_kms_client.decrypt(
            CiphertextBlob=ciphertext,
            KeyId=kms_key_id,
            EncryptionAlgorithm="SYMMETRIC_DEFAULT",
        )
        assert decrypt_response["Plaintext"] == plaintext

    def test_envelope_encryption_pattern(
        self, localstack_kms_client: boto3.client, kms_key_id: str
    ) -> None:
        """Full envelope encryption: generate DEK, encrypt data, decrypt."""
        # Step 1: Generate data encryption key (DEK)
        dek_response = localstack_kms_client.generate_data_key(
            KeyId=kms_key_id,
            KeySpec="AES_256",
        )
        plaintext_dek = dek_response["Plaintext"]
        encrypted_dek = dek_response["CiphertextBlob"]

        # Step 2: Simulate encrypting data with plaintext DEK
        # (In production, use AES-GCM with the plaintext DEK)
        data = b"Sensitive event payload for FSAMP processing"
        # XOR-based simulation for test purposes
        encrypted_data = bytes(
            b ^ plaintext_dek[i % len(plaintext_dek)] for i, b in enumerate(data)
        )

        # Step 3: Discard plaintext DEK (only keep encrypted DEK)
        del plaintext_dek

        # Step 4: Decrypt the DEK using KMS
        decrypt_response = localstack_kms_client.decrypt(
            CiphertextBlob=encrypted_dek,
            KeyId=kms_key_id,
        )
        recovered_dek = decrypt_response["Plaintext"]

        # Step 5: Decrypt data with recovered DEK
        decrypted_data = bytes(
            b ^ recovered_dek[i % len(recovered_dek)] for i, b in enumerate(encrypted_data)
        )
        assert decrypted_data == data

    def test_generate_data_key_without_plaintext(
        self, localstack_kms_client: boto3.client, kms_key_id: str
    ) -> None:
        """GenerateDataKeyWithoutPlaintext for deferred decryption."""
        response = localstack_kms_client.generate_data_key_without_plaintext(
            KeyId=kms_key_id,
            KeySpec="AES_256",
        )

        assert "CiphertextBlob" in response
        assert "Plaintext" not in response

        # Should be decryptable
        decrypt_response = localstack_kms_client.decrypt(
            CiphertextBlob=response["CiphertextBlob"],
            KeyId=kms_key_id,
        )
        assert len(decrypt_response["Plaintext"]) == 32


@pytest.mark.integration
class TestKmsS3SseIntegration:
    """Tests for S3 SSE-KMS integration (FedRAMP SC-28)."""

    @pytest.fixture
    def kms_key_id(self, localstack_kms_client: boto3.client) -> str:
        """Create a KMS key for SSE-KMS tests."""
        response = localstack_kms_client.create_key(
            Description="S3 SSE-KMS test key",
            KeyUsage="ENCRYPT_DECRYPT",
        )
        return response["KeyMetadata"]["KeyId"]

    @pytest.fixture
    def encrypted_bucket(
        self,
        localstack_s3_client: boto3.client,
        kms_key_id: str,
    ) -> str:
        """Create an S3 bucket with SSE-KMS default encryption."""
        bucket_name = "test-sse-kms-bucket"

        try:
            localstack_s3_client.create_bucket(
                Bucket=bucket_name,
                CreateBucketConfiguration={"LocationConstraint": "us-west-2"},
            )
        except localstack_s3_client.exceptions.BucketAlreadyOwnedByYou:
            pass

        localstack_s3_client.put_bucket_encryption(
            Bucket=bucket_name,
            ServerSideEncryptionConfiguration={
                "Rules": [
                    {
                        "ApplyServerSideEncryptionByDefault": {
                            "SSEAlgorithm": "aws:kms",
                            "KMSMasterKeyID": kms_key_id,
                        },
                        "BucketKeyEnabled": True,
                    }
                ]
            },
        )

        return bucket_name

    def test_upload_with_sse_kms(
        self,
        localstack_s3_client: boto3.client,
        encrypted_bucket: str,
        kms_key_id: str,
    ) -> None:
        """Objects uploaded to SSE-KMS bucket should be encrypted."""
        object_key = "test/encrypted-object.txt"
        content = b"FedRAMP SC-28 encrypted content"

        localstack_s3_client.put_object(
            Bucket=encrypted_bucket,
            Key=object_key,
            Body=content,
            ServerSideEncryption="aws:kms",
            SSEKMSKeyId=kms_key_id,
        )

        # Verify encryption metadata
        head = localstack_s3_client.head_object(
            Bucket=encrypted_bucket,
            Key=object_key,
        )
        assert head["ServerSideEncryption"] == "aws:kms"

        # Verify content is readable (decrypted transparently)
        get_response = localstack_s3_client.get_object(
            Bucket=encrypted_bucket,
            Key=object_key,
        )
        assert get_response["Body"].read() == content

    def test_bucket_default_encryption(
        self,
        localstack_s3_client: boto3.client,
        encrypted_bucket: str,
        kms_key_id: str,
    ) -> None:
        """Bucket-level default encryption should apply SSE-KMS automatically."""
        encryption = localstack_s3_client.get_bucket_encryption(
            Bucket=encrypted_bucket,
        )

        rules = encryption["ServerSideEncryptionConfiguration"]["Rules"]
        assert len(rules) == 1
        default_enc = rules[0]["ApplyServerSideEncryptionByDefault"]
        assert default_enc["SSEAlgorithm"] == "aws:kms"
        assert kms_key_id in default_enc["KMSMasterKeyID"]
