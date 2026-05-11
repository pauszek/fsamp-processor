# =============================================================================
# Unit Tests for Crypto Provider
# =============================================================================
"""Tests for FIPS 140-3-oriented crypto operations."""

import pytest

from processor.adapters.outbound.kms_crypto import LocalCryptoProvider
from processor.domain.exceptions import CryptoError


class TestLocalCryptoProvider:
    """Tests for LocalCryptoProvider (testing-only implementation)."""

    @pytest.fixture
    def crypto(self) -> LocalCryptoProvider:
        """Create crypto provider instance."""
        return LocalCryptoProvider()

    def test_encrypt_decrypt_roundtrip(self, crypto: LocalCryptoProvider) -> None:
        """Test that encrypt then decrypt returns original data."""
        plaintext = b"Hello, FIPS 140-3-oriented encryption!"

        ciphertext = crypto.encrypt(plaintext)
        decrypted = crypto.decrypt(ciphertext)

        assert decrypted == plaintext

    def test_encrypt_produces_different_ciphertext(self, crypto: LocalCryptoProvider) -> None:
        """Test that encrypting same plaintext produces different ciphertext."""
        plaintext = b"Same message"

        ciphertext1 = crypto.encrypt(plaintext)
        ciphertext2 = crypto.encrypt(plaintext)

        # Different nonces should produce different ciphertext
        assert ciphertext1 != ciphertext2

    def test_encrypt_large_data(self, crypto: LocalCryptoProvider) -> None:
        """Test encrypting larger data blocks."""
        # 1 MB of data
        plaintext = b"x" * (1024 * 1024)

        ciphertext = crypto.encrypt(plaintext)
        decrypted = crypto.decrypt(ciphertext)

        assert decrypted == plaintext

    def test_decrypt_invalid_ciphertext(self, crypto: LocalCryptoProvider) -> None:
        """Test that invalid ciphertext raises error."""
        with pytest.raises(Exception):  # Could be CryptoError or other
            crypto.decrypt(b"invalid ciphertext")

    def test_decrypt_tampered_ciphertext(self, crypto: LocalCryptoProvider) -> None:
        """Test that tampered ciphertext raises error."""
        plaintext = b"Original message"
        ciphertext = crypto.encrypt(plaintext)

        # Tamper with the ciphertext
        tampered = bytearray(ciphertext)
        tampered[-1] ^= 0xFF  # Flip bits in last byte

        with pytest.raises(Exception):
            crypto.decrypt(bytes(tampered))


class TestHashFunctions:
    """Tests for cryptographic hash functions."""

    @pytest.fixture
    def crypto(self) -> LocalCryptoProvider:
        """Create crypto provider instance."""
        return LocalCryptoProvider()

    def test_sha256_hash(self, crypto: LocalCryptoProvider) -> None:
        """Test SHA-256 hash computation."""
        data = b"test data for hashing"
        hash_result = crypto.compute_hash(data, "SHA-256")

        assert len(hash_result) == 64  # 256 bits = 64 hex chars
        assert hash_result.isalnum()

    def test_sha384_hash(self, crypto: LocalCryptoProvider) -> None:
        """Test SHA-384 hash computation."""
        data = b"test data"
        hash_result = crypto.compute_hash(data, "SHA-384")

        assert len(hash_result) == 96  # 384 bits = 96 hex chars

    def test_sha512_hash(self, crypto: LocalCryptoProvider) -> None:
        """Test SHA-512 hash computation."""
        data = b"test data"
        hash_result = crypto.compute_hash(data, "SHA-512")

        assert len(hash_result) == 128  # 512 bits = 128 hex chars

    def test_hash_consistency(self, crypto: LocalCryptoProvider) -> None:
        """Test that same data produces same hash."""
        data = b"consistent data"

        hash1 = crypto.compute_hash(data, "SHA-256")
        hash2 = crypto.compute_hash(data, "SHA-256")

        assert hash1 == hash2

    def test_hash_different_data(self, crypto: LocalCryptoProvider) -> None:
        """Test that different data produces different hash."""
        hash1 = crypto.compute_hash(b"data1", "SHA-256")
        hash2 = crypto.compute_hash(b"data2", "SHA-256")

        assert hash1 != hash2

    def test_unsupported_algorithm_rejected(self, crypto: LocalCryptoProvider) -> None:
        """Test that non-FIPS algorithms are rejected."""
        with pytest.raises(CryptoError) as exc_info:
            crypto.compute_hash(b"data", "MD5")

        assert "Unsupported" in str(exc_info.value)

    def test_sha1_rejected(self, crypto: LocalCryptoProvider) -> None:
        """Test that SHA-1 is rejected."""
        with pytest.raises(CryptoError) as exc_info:
            crypto.compute_hash(b"data", "SHA-1")

        assert "Unsupported" in str(exc_info.value)

    def test_algorithm_case_insensitive(self, crypto: LocalCryptoProvider) -> None:
        """Test that algorithm names are case-insensitive."""
        data = b"test"

        hash_lower = crypto.compute_hash(data, "sha-256")
        hash_upper = crypto.compute_hash(data, "SHA-256")
        hash_mixed = crypto.compute_hash(data, "Sha-256")

        assert hash_lower == hash_upper == hash_mixed
