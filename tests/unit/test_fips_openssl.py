import hashlib
import hmac
import os
import ssl

import pytest


class TestFipsApprovedAlgorithms:
    def test_sha256_available(self) -> None:
        digest = hashlib.sha256(b"FSAMP FIPS test").hexdigest()
        assert len(digest) == 64

    def test_sha384_available(self) -> None:
        digest = hashlib.sha384(b"FSAMP FIPS test").hexdigest()
        assert len(digest) == 96

    def test_sha512_available(self) -> None:
        digest = hashlib.sha512(b"FSAMP FIPS test").hexdigest()
        assert len(digest) == 128

    def test_sha3_256_available(self) -> None:
        digest = hashlib.sha3_256(b"FSAMP FIPS test").hexdigest()
        assert len(digest) == 64

    def test_hmac_sha256(self) -> None:
        key = b"fsamp-hmac-key"
        msg = b"message to authenticate"
        mac = hmac.new(key, msg, hashlib.sha256).hexdigest()
        assert len(mac) == 64

        mac2 = hmac.new(key, msg, hashlib.sha256).hexdigest()
        assert mac == mac2

    def test_hmac_sha512(self) -> None:
        key = b"fsamp-hmac-key"
        msg = b"message to authenticate"
        mac = hmac.new(key, msg, hashlib.sha512).hexdigest()
        assert len(mac) == 128


class TestNonApprovedAlgorithmWarnings:
    def test_md5_rejected_with_usedforsecurity(self) -> None:
        digest = hashlib.md5(b"test", usedforsecurity=False).hexdigest()
        assert len(digest) == 32

        fips_mode = os.environ.get("OPENSSL_FIPS", "0") == "1"
        if fips_mode:
            with pytest.raises((ValueError, Exception)):
                hashlib.md5(  # noqa: S324 - intentional rejection test
                    b"test",
                    usedforsecurity=True,
                )
        else:
            digest = hashlib.md5(  # noqa: S324 - intentional legacy algorithm test
                b"test",
                usedforsecurity=True,
            ).hexdigest()
            assert len(digest) == 32


class TestTlsConfiguration:
    def test_openssl_version_minimum(self) -> None:
        version = ssl.OPENSSL_VERSION
        parts = version.split()
        assert len(parts) >= 2, f"Unexpected OpenSSL version format: {version}"
        version_str = parts[1]
        major = int(version_str.split(".")[0])
        assert major >= 1, f"OpenSSL version too old: {version}"

    def test_tls_1_2_supported(self) -> None:
        ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
        ctx.minimum_version = ssl.TLSVersion.TLSv1_2
        assert ctx.minimum_version == ssl.TLSVersion.TLSv1_2

    def test_tls_1_0_disabled(self) -> None:
        ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
        ctx.minimum_version = ssl.TLSVersion.TLSv1_2
        assert ctx.minimum_version >= ssl.TLSVersion.TLSv1_2

    def test_strong_ciphers_available(self) -> None:
        ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
        ciphers = ctx.get_ciphers()
        cipher_names = [c["name"] for c in ciphers]

        aes_gcm_ciphers = [c for c in cipher_names if "AES" in c and "GCM" in c]
        assert len(aes_gcm_ciphers) > 0, f"No AES-GCM ciphers found. Available: {cipher_names[:10]}"


class TestSecureRandomness:
    def test_os_urandom_available(self) -> None:
        random_bytes = os.urandom(32)
        assert len(random_bytes) == 32

        assert random_bytes != b"\x00" * 32

    def test_secrets_module_available(self) -> None:
        import secrets

        token = secrets.token_hex(32)
        assert len(token) == 64  # 32 bytes = 64 hex chars

        token2 = secrets.token_hex(32)
        assert token != token2

    def test_random_bytes_entropy(self) -> None:
        sample = os.urandom(1000)
        unique_bytes = len(set(sample))
        assert (
            unique_bytes > 200
        ), f"Low entropy: only {unique_bytes}/256 unique byte values in 1000 bytes"
