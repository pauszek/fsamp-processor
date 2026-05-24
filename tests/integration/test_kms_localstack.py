import boto3
import pytest


@pytest.mark.integration
class TestKmsKeyManagement:
    def test_create_symmetric_key(self, localstack_kms_client: boto3.client) -> None:
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
        key_response = localstack_kms_client.create_key(
            Description="Alias test key",
            KeyUsage="ENCRYPT_DECRYPT",
        )
        key_id = key_response["KeyMetadata"]["KeyId"]

        alias_name = "alias/fsamp-processor-test"
        try:
            localstack_kms_client.delete_alias(AliasName=alias_name)
        except localstack_kms_client.exceptions.NotFoundException:
            pass

        localstack_kms_client.create_alias(
            AliasName=alias_name,
            TargetKeyId=key_id,
        )

        described = localstack_kms_client.describe_key(KeyId=alias_name)
        assert described["KeyMetadata"]["KeyId"] == key_id

    def test_describe_key_metadata(self, localstack_kms_client: boto3.client) -> None:
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
        key_response = localstack_kms_client.create_key(
            Description="Lifecycle test key",
        )
        key_id = key_response["KeyMetadata"]["KeyId"]

        localstack_kms_client.disable_key(KeyId=key_id)
        described = localstack_kms_client.describe_key(KeyId=key_id)
        assert described["KeyMetadata"]["KeyState"] == "Disabled"

        localstack_kms_client.enable_key(KeyId=key_id)
        described = localstack_kms_client.describe_key(KeyId=key_id)
        assert described["KeyMetadata"]["KeyState"] == "Enabled"


@pytest.mark.integration
class TestKmsEnvelopeEncryption:
    @pytest.fixture
    def kms_key_id(self, localstack_kms_client: boto3.client) -> str:
        response = localstack_kms_client.create_key(
            Description="Envelope encryption test key",
            KeyUsage="ENCRYPT_DECRYPT",
        )
        return response["KeyMetadata"]["KeyId"]

    def test_generate_data_key(self, localstack_kms_client: boto3.client, kms_key_id: str) -> None:
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
        plaintext = b"FedRAMP SC-28: sensitive data at rest"

        encrypt_response = localstack_kms_client.encrypt(
            KeyId=kms_key_id,
            Plaintext=plaintext,
            EncryptionAlgorithm="SYMMETRIC_DEFAULT",
        )
        ciphertext = encrypt_response["CiphertextBlob"]
        assert ciphertext != plaintext

        decrypt_response = localstack_kms_client.decrypt(
            CiphertextBlob=ciphertext,
            KeyId=kms_key_id,
            EncryptionAlgorithm="SYMMETRIC_DEFAULT",
        )
        assert decrypt_response["Plaintext"] == plaintext

    def test_envelope_encryption_pattern(
        self, localstack_kms_client: boto3.client, kms_key_id: str
    ) -> None:
        dek_response = localstack_kms_client.generate_data_key(
            KeyId=kms_key_id,
            KeySpec="AES_256",
        )
        plaintext_dek = dek_response["Plaintext"]
        encrypted_dek = dek_response["CiphertextBlob"]

        data = b"Sensitive event payload for FSAMP processing"
        encrypted_data = bytes(
            b ^ plaintext_dek[i % len(plaintext_dek)] for i, b in enumerate(data)
        )

        del plaintext_dek

        decrypt_response = localstack_kms_client.decrypt(
            CiphertextBlob=encrypted_dek,
            KeyId=kms_key_id,
        )
        recovered_dek = decrypt_response["Plaintext"]

        decrypted_data = bytes(
            b ^ recovered_dek[i % len(recovered_dek)] for i, b in enumerate(encrypted_data)
        )
        assert decrypted_data == data

    def test_generate_data_key_without_plaintext(
        self, localstack_kms_client: boto3.client, kms_key_id: str
    ) -> None:
        response = localstack_kms_client.generate_data_key_without_plaintext(
            KeyId=kms_key_id,
            KeySpec="AES_256",
        )

        assert "CiphertextBlob" in response
        assert "Plaintext" not in response

        decrypt_response = localstack_kms_client.decrypt(
            CiphertextBlob=response["CiphertextBlob"],
            KeyId=kms_key_id,
        )
        assert len(decrypt_response["Plaintext"]) == 32


@pytest.mark.integration
class TestKmsS3SseIntegration:
    @pytest.fixture
    def kms_key_id(self, localstack_kms_client: boto3.client) -> str:
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
        object_key = "test/encrypted-object.txt"
        content = b"FedRAMP SC-28 encrypted content"

        localstack_s3_client.put_object(
            Bucket=encrypted_bucket,
            Key=object_key,
            Body=content,
            ServerSideEncryption="aws:kms",
            SSEKMSKeyId=kms_key_id,
        )

        head = localstack_s3_client.head_object(
            Bucket=encrypted_bucket,
            Key=object_key,
        )
        assert head["ServerSideEncryption"] == "aws:kms"

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
        encryption = localstack_s3_client.get_bucket_encryption(
            Bucket=encrypted_bucket,
        )

        rules = encryption["ServerSideEncryptionConfiguration"]["Rules"]
        assert len(rules) == 1
        default_enc = rules[0]["ApplyServerSideEncryptionByDefault"]
        assert default_enc["SSEAlgorithm"] == "aws:kms"
        assert kms_key_id in default_enc["KMSMasterKeyID"]
