from unittest.mock import MagicMock

import pytest
from botocore.exceptions import ClientError

from processor.adapters.outbound.kms_crypto import (
    KMSCryptoProvider,
    LocalCryptoProvider,
)
from processor.domain.exceptions import CryptoError


class TestKMSCryptoProviderInit:
    def test_init(self) -> None:
        client = MagicMock()
        provider = KMSCryptoProvider(
            kms_client=client,
            key_id="test-key-id",
        )

        assert provider._client is client
        assert provider._key_id == "test-key-id"


class TestKMSCryptoProviderMaskKeyId:
    def test_mask_key_id_long(self) -> None:
        result = KMSCryptoProvider._mask_key_id(
            "arn:aws:kms:us-west-2:123456789012:key/12345678-1234-1234-1234-123456789012"
        )
        assert "..." in result
        assert len(result) < 50

    def test_mask_key_id_short(self) -> None:
        result = KMSCryptoProvider._mask_key_id("short-key")
        assert result == "short-key"


class TestKMSCryptoProviderGenerateDataKey:
    @pytest.fixture
    def provider(self) -> KMSCryptoProvider:
        client = MagicMock()
        client.generate_data_key.return_value = {
            "Plaintext": b"0" * 32,
            "CiphertextBlob": b"encrypted-key-blob",
        }
        return KMSCryptoProvider(kms_client=client, key_id="test-key")

    def test_generate_data_key_success(self, provider: KMSCryptoProvider) -> None:
        plaintext_key, encrypted_key = provider.generate_data_key()

        assert plaintext_key == b"0" * 32
        assert encrypted_key == b"encrypted-key-blob"
        provider._client.generate_data_key.assert_called_once()

    def test_generate_data_key_with_context(self, provider: KMSCryptoProvider) -> None:
        context = {"purpose": "file-encryption"}
        provider.generate_data_key(context=context)

        call_kwargs = provider._client.generate_data_key.call_args.kwargs
        assert call_kwargs["EncryptionContext"] == context

    def test_generate_data_key_error(self) -> None:
        client = MagicMock()
        client.generate_data_key.side_effect = ClientError(
            {"Error": {"Code": "KMSInternalException"}},
            "GenerateDataKey",
        )
        provider = KMSCryptoProvider(kms_client=client, key_id="test-key")

        with pytest.raises(CryptoError) as exc_info:
            provider.generate_data_key()

        assert "Failed to generate data key" in str(exc_info.value)


class TestKMSCryptoProviderDecryptDataKey:
    @pytest.fixture
    def provider(self) -> KMSCryptoProvider:
        client = MagicMock()
        client.decrypt.return_value = {"Plaintext": b"decrypted-key"}
        return KMSCryptoProvider(kms_client=client, key_id="test-key")

    def test_decrypt_data_key_success(self, provider: KMSCryptoProvider) -> None:
        result = provider._decrypt_data_key(b"encrypted-key")

        assert result == b"decrypted-key"

    def test_decrypt_data_key_with_context(self, provider: KMSCryptoProvider) -> None:
        context = {"purpose": "file-encryption"}
        provider._decrypt_data_key(b"encrypted-key", context=context)

        call_kwargs = provider._client.decrypt.call_args.kwargs
        assert call_kwargs["EncryptionContext"] == context

    def test_decrypt_data_key_invalid_ciphertext(self) -> None:
        client = MagicMock()
        client.decrypt.side_effect = ClientError(
            {"Error": {"Code": "InvalidCiphertextException"}},
            "Decrypt",
        )
        provider = KMSCryptoProvider(kms_client=client, key_id="test-key")

        with pytest.raises(CryptoError) as exc_info:
            provider._decrypt_data_key(b"bad-ciphertext")

        assert "tampered" in str(exc_info.value).lower()

    def test_decrypt_data_key_incorrect_key(self) -> None:
        client = MagicMock()
        client.decrypt.side_effect = ClientError(
            {"Error": {"Code": "IncorrectKeyException"}},
            "Decrypt",
        )
        provider = KMSCryptoProvider(kms_client=client, key_id="test-key")

        with pytest.raises(CryptoError) as exc_info:
            provider._decrypt_data_key(b"ciphertext")

        assert "Incorrect KMS key" in str(exc_info.value)


class TestKMSCryptoProviderEncrypt:
    @pytest.fixture
    def provider(self) -> KMSCryptoProvider:
        client = MagicMock()
        client.generate_data_key.return_value = {
            "Plaintext": b"0" * 32,
            "CiphertextBlob": b"encrypted-key",
        }
        return KMSCryptoProvider(kms_client=client, key_id="test-key")

    def test_encrypt_success(self, provider: KMSCryptoProvider) -> None:
        plaintext = b"Hello, World!"

        ciphertext = provider.encrypt(plaintext)

        assert len(ciphertext) > len(plaintext)
        assert ciphertext != plaintext

    def test_encrypt_with_context(self, provider: KMSCryptoProvider) -> None:
        context = {"file_id": "123"}
        provider.encrypt(b"data", context=context)

        call_kwargs = provider._client.generate_data_key.call_args.kwargs
        assert call_kwargs["EncryptionContext"] == context


class TestKMSCryptoProviderDecrypt:
    @pytest.fixture
    def provider(self) -> KMSCryptoProvider:
        client = MagicMock()
        client.generate_data_key.return_value = {
            "Plaintext": b"0" * 32,
            "CiphertextBlob": b"encrypted-key",
        }
        client.decrypt.return_value = {"Plaintext": b"0" * 32}
        return KMSCryptoProvider(kms_client=client, key_id="test-key")

    def test_decrypt_success(self, provider: KMSCryptoProvider) -> None:
        plaintext = b"Hello, World!"
        ciphertext = provider.encrypt(plaintext)

        decrypted = provider.decrypt(ciphertext)

        assert decrypted == plaintext

    def test_decrypt_too_short(self, provider: KMSCryptoProvider) -> None:
        with pytest.raises(CryptoError) as exc_info:
            provider.decrypt(b"ab")

        assert "too short" in str(exc_info.value).lower()

    def test_decrypt_invalid_envelope(self, provider: KMSCryptoProvider) -> None:
        invalid = (1000000).to_bytes(4, byteorder="big") + b"short"

        with pytest.raises(CryptoError) as exc_info:
            provider.decrypt(invalid)

        assert "invalid envelope" in str(exc_info.value).lower()


class TestKMSCryptoProviderComputeHash:
    @pytest.fixture
    def provider(self) -> KMSCryptoProvider:
        client = MagicMock()
        return KMSCryptoProvider(kms_client=client, key_id="test-key")

    def test_compute_hash_sha256(self, provider: KMSCryptoProvider) -> None:
        result = provider.compute_hash(b"test data", "SHA-256")
        assert len(result) == 64

    def test_compute_hash_sha384(self, provider: KMSCryptoProvider) -> None:
        result = provider.compute_hash(b"test data", "SHA-384")
        assert len(result) == 96

    def test_compute_hash_sha512(self, provider: KMSCryptoProvider) -> None:
        result = provider.compute_hash(b"test data", "SHA-512")
        assert len(result) == 128

    def test_compute_hash_unsupported(self, provider: KMSCryptoProvider) -> None:
        with pytest.raises(CryptoError) as exc_info:
            provider.compute_hash(b"data", "MD5")

        assert "Unsupported" in str(exc_info.value)
        assert "FIPS 140-3" in str(exc_info.value)


class TestKMSCryptoProviderVerifyKeyAccess:
    def test_verify_key_access_success(self) -> None:
        client = MagicMock()
        client.describe_key.return_value = {"KeyMetadata": {}}
        provider = KMSCryptoProvider(kms_client=client, key_id="test-key")

        result = provider.verify_key_access()

        assert result is True

    def test_verify_key_access_failure(self) -> None:
        client = MagicMock()
        client.describe_key.side_effect = ClientError(
            {"Error": {"Code": "AccessDeniedException"}},
            "DescribeKey",
        )
        provider = KMSCryptoProvider(kms_client=client, key_id="test-key")

        result = provider.verify_key_access()

        assert result is False


class TestKMSCryptoProviderGetKeyMetadata:
    def test_get_key_metadata_success(self) -> None:
        client = MagicMock()
        client.describe_key.return_value = {
            "KeyMetadata": {
                "KeyId": "test-key-id",
                "Arn": "arn:aws:kms:us-west-2:123456789012:key/test",
                "KeyState": "Enabled",
                "KeyUsage": "ENCRYPT_DECRYPT",
                "KeySpec": "SYMMETRIC_DEFAULT",
                "EncryptionAlgorithms": ["SYMMETRIC_DEFAULT"],
                "CreationDate": "2024-01-01T00:00:00Z",
            }
        }
        provider = KMSCryptoProvider(kms_client=client, key_id="test-key")

        metadata = provider.get_key_metadata()

        assert metadata["key_id"] == "test-key-id"
        assert metadata["key_state"] == "Enabled"
        assert metadata["key_usage"] == "ENCRYPT_DECRYPT"

    def test_get_key_metadata_error(self) -> None:
        client = MagicMock()
        client.describe_key.side_effect = ClientError(
            {"Error": {"Code": "NotFoundException"}},
            "DescribeKey",
        )
        provider = KMSCryptoProvider(kms_client=client, key_id="test-key")

        metadata = provider.get_key_metadata()

        assert metadata == {}


class TestLocalCryptoProviderInit:
    def test_init_with_static_key(self) -> None:
        static_key = b"0" * 32
        provider = LocalCryptoProvider(static_key=static_key)

        assert provider._master_key == static_key

    def test_init_without_key(self) -> None:
        provider = LocalCryptoProvider()

        assert len(provider._master_key) == 32

    def test_init_invalid_key_length(self) -> None:
        with pytest.raises(ValueError) as exc_info:
            LocalCryptoProvider(static_key=b"short")

        assert "32 bytes" in str(exc_info.value)


class TestLocalCryptoProviderGenerateDataKey:
    def test_generate_data_key(self) -> None:
        provider = LocalCryptoProvider()

        plaintext_key, encrypted_key = provider.generate_data_key()

        assert len(plaintext_key) == 32
        assert len(encrypted_key) == 32


class TestLocalCryptoProviderEncryptDecrypt:
    @pytest.fixture
    def provider(self) -> LocalCryptoProvider:
        return LocalCryptoProvider(static_key=b"0" * 32)

    def test_encrypt_decrypt_roundtrip(self, provider: LocalCryptoProvider) -> None:
        plaintext = b"Secret message"

        ciphertext = provider.encrypt(plaintext)
        decrypted = provider.decrypt(ciphertext)

        assert decrypted == plaintext

    def test_encrypt_produces_different_ciphertext(self, provider: LocalCryptoProvider) -> None:
        plaintext = b"Same message"

        ciphertext1 = provider.encrypt(plaintext)
        ciphertext2 = provider.encrypt(plaintext)

        assert ciphertext1 != ciphertext2


class TestLocalCryptoProviderComputeHash:
    @pytest.fixture
    def provider(self) -> LocalCryptoProvider:
        return LocalCryptoProvider()

    def test_compute_hash_sha256(self, provider: LocalCryptoProvider) -> None:
        result = provider.compute_hash(b"test", "SHA-256")
        assert len(result) == 64

    def test_compute_hash_unsupported(self, provider: LocalCryptoProvider) -> None:
        with pytest.raises(CryptoError):
            provider.compute_hash(b"data", "MD5")
