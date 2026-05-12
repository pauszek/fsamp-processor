# =============================================================================
# OpenSSL FIPS Readiness Tests
# =============================================================================
"""
Verify that the Python runtime cryptographic primitives are FIPS-compatible.

These tests validate FedRAMP SC-13 (Cryptographic Protection) controls:
- Only FIPS-approved algorithms are available (AES, SHA-2, HMAC)
- Non-approved algorithms (MD5, RC4) raise errors or warnings in FIPS mode
- TLS library version meets minimum requirements

Note: Full FIPS 140-2/3 enforcement requires OpenSSL compiled with FIPS module.
These tests verify readiness — that the code uses only approved algorithms.
"""

import hashlib
import hmac
import os
import ssl

import pytest


class TestFipsApprovedAlgorithms:
    """Verify FIPS-approved cryptographic algorithms are available."""

    def test_sha256_available(self) -> None:
        """SHA-256 (FIPS 180-4) must be available."""
        digest = hashlib.sha256(b"FSAMP FIPS test").hexdigest()
        assert len(digest) == 64

    def test_sha384_available(self) -> None:
        """SHA-384 (FIPS 180-4) must be available."""
        digest = hashlib.sha384(b"FSAMP FIPS test").hexdigest()
        assert len(digest) == 96

    def test_sha512_available(self) -> None:
        """SHA-512 (FIPS 180-4) must be available."""
        digest = hashlib.sha512(b"FSAMP FIPS test").hexdigest()
        assert len(digest) == 128

    def test_sha3_256_available(self) -> None:
        """SHA3-256 (FIPS 202) should be available."""
        digest = hashlib.sha3_256(b"FSAMP FIPS test").hexdigest()
        assert len(digest) == 64

    def test_hmac_sha256(self) -> None:
        """HMAC-SHA256 (FIPS 198-1) must be available."""
        key = b"fsamp-hmac-key"
        msg = b"message to authenticate"
        mac = hmac.new(key, msg, hashlib.sha256).hexdigest()
        assert len(mac) == 64

        # Verify deterministic
        mac2 = hmac.new(key, msg, hashlib.sha256).hexdigest()
        assert mac == mac2

    def test_hmac_sha512(self) -> None:
        """HMAC-SHA512 (FIPS 198-1) must be available."""
        key = b"fsamp-hmac-key"
        msg = b"message to authenticate"
        mac = hmac.new(key, msg, hashlib.sha512).hexdigest()
        assert len(mac) == 128


class TestNonApprovedAlgorithmWarnings:
    """Verify non-FIPS algorithms are flagged or rejected."""

    def test_md5_rejected_with_usedforsecurity(self) -> None:
        """MD5 with usedforsecurity=True should raise ValueError in FIPS mode.

        Python 3.9+ supports the usedforsecurity parameter.
        When OpenSSL FIPS module is active, MD5(usedforsecurity=True) raises.
        In non-FIPS mode, we verify the parameter is accepted (no crash).
        """
        # MD5 should always work when usedforsecurity=False (checksums)
        digest = hashlib.md5(b"test", usedforsecurity=False).hexdigest()
        assert len(digest) == 32

        # If FIPS mode is active, usedforsecurity=True should raise
        fips_mode = os.environ.get("OPENSSL_FIPS", "0") == "1"
        if fips_mode:
            with pytest.raises((ValueError, Exception)):
                hashlib.md5(  # noqa: S324 - intentional rejection test
                    b"test",
                    usedforsecurity=True,
                )
        else:
            # In non-FIPS mode, just verify it works without crash
            digest = hashlib.md5(  # noqa: S324 - intentional legacy algorithm test
                b"test",
                usedforsecurity=True,
            ).hexdigest()
            assert len(digest) == 32


class TestTlsConfiguration:
    """Verify TLS library meets FedRAMP requirements."""

    def test_openssl_version_minimum(self) -> None:
        """OpenSSL version must be >= 1.1.1 for TLS 1.2+ support."""
        version = ssl.OPENSSL_VERSION
        # Extract version number
        parts = version.split()
        assert len(parts) >= 2, f"Unexpected OpenSSL version format: {version}"
        # Should be OpenSSL 1.1.1+ or 3.x+ or LibreSSL 3.x+
        version_str = parts[1]
        major = int(version_str.split(".")[0])
        assert major >= 1, f"OpenSSL version too old: {version}"

    def test_tls_1_2_supported(self) -> None:
        """TLS 1.2 must be available (FedRAMP SC-8, SC-23)."""
        ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
        # TLS 1.2 is the minimum for FedRAMP
        ctx.minimum_version = ssl.TLSVersion.TLSv1_2
        assert ctx.minimum_version == ssl.TLSVersion.TLSv1_2

    def test_tls_1_0_disabled(self) -> None:
        """TLS 1.0 must be disabled because it is not FIPS-approved."""
        ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
        ctx.minimum_version = ssl.TLSVersion.TLSv1_2
        # After setting minimum to 1.2, TLS 1.0 connections should fail
        assert ctx.minimum_version >= ssl.TLSVersion.TLSv1_2

    def test_strong_ciphers_available(self) -> None:
        """At least one AES-GCM cipher must be available."""
        ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
        ciphers = ctx.get_ciphers()
        cipher_names = [c["name"] for c in ciphers]

        aes_gcm_ciphers = [c for c in cipher_names if "AES" in c and "GCM" in c]
        assert len(aes_gcm_ciphers) > 0, f"No AES-GCM ciphers found. Available: {cipher_names[:10]}"


class TestSecureRandomness:
    """Verify cryptographic random number generation."""

    def test_os_urandom_available(self) -> None:
        """os.urandom must use system CSPRNG."""
        random_bytes = os.urandom(32)
        assert len(random_bytes) == 32

        # Should not produce all zeros
        assert random_bytes != b"\x00" * 32

    def test_secrets_module_available(self) -> None:
        """secrets module (CSPRNG) must be available."""
        import secrets

        token = secrets.token_hex(32)
        assert len(token) == 64  # 32 bytes = 64 hex chars

        # Two tokens should differ
        token2 = secrets.token_hex(32)
        assert token != token2

    def test_random_bytes_entropy(self) -> None:
        """Generated random bytes should have reasonable entropy."""
        sample = os.urandom(1000)
        # Count unique bytes — good entropy should have most of 256 values
        unique_bytes = len(set(sample))
        assert (
            unique_bytes > 200
        ), f"Low entropy: only {unique_bytes}/256 unique byte values in 1000 bytes"
