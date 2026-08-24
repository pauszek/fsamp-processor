"""
KMS implementation of the CryptoProvider port.
Provides FIPS 140-3-oriented cryptographic operations using AWS KMS
and the Python cryptography library.
"""

import hashlib
import os
from typing import TYPE_CHECKING, Any

import structlog
from botocore.exceptions import ClientError
from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives.ciphers.aead import AESGCM

from processor.adapters.outbound.aws_retry import aws_retry, is_retryable_aws_error
from processor.domain.exceptions import CryptoError
from processor.ports.outbound import CryptoProvider

if TYPE_CHECKING:
    from mypy_boto3_kms import KMSClient

logger = structlog.get_logger(__name__)

AES_KEY_SIZE = 256  # bits
GCM_NONCE_SIZE = 12  # bytes (96 bits as per NIST SP 800-38D)
GCM_TAG_SIZE = 16  # bytes (128 bits)


class KMSCryptoProvider(CryptoProvider):
    """
    AWS KMS Crypto Provider with a FIPS 140-3-oriented posture.

    Implements envelope encryption:
    1. KMS generates/encrypts data encryption keys (DEKs)
    2. Data is encrypted locally using AES-256-GCM
    3. Encrypted DEK is stored alongside ciphertext

    FIPS 140-3-oriented posture:
    - Delegates key operations to AWS KMS instead of custom key handling
    - AES-256-GCM for symmetric encryption
    - SHA-256/384/512 for hashing
    - No disallowed legacy algorithms (MD5, SHA-1, DES, etc.)
    """

    def __init__(
        self,
        kms_client: KMSClient,
        key_id: str,
    ) -> None:
        """
        Initialize KMS Crypto Provider.

        Args:
            kms_client: Boto3 KMS client.
            key_id: KMS key ID, ARN, or alias.
        """
        self._client = kms_client
        self._key_id = key_id
        self._backend = default_backend()

        logger.info(
            "KMS Crypto Provider initialized (FIPS 140-3-oriented)",
            key_id=self._mask_key_id(key_id),
        )

    @staticmethod
    def _mask_key_id(key_id: str) -> str:
        """Mask key ID for logging."""
        if len(key_id) > 20:
            return f"{key_id[:10]}...{key_id[-6:]}"
        return key_id

    @aws_retry()
    def generate_data_key(
        self,
        context: dict[str, str] | None = None,
    ) -> tuple[bytes, bytes]:
        """
        Generate a data encryption key using KMS.

        Returns:
            Tuple of (plaintext_key, encrypted_key).
        """
        try:
            params: dict[str, Any] = {
                "KeyId": self._key_id,
                "KeySpec": "AES_256",  # FIPS-approved key size
            }

            if context:
                params["EncryptionContext"] = context

            response = self._client.generate_data_key(**params)

            plaintext_key = response["Plaintext"]
            encrypted_key = response["CiphertextBlob"]

            logger.debug(
                "Data key generated",
                encrypted_key_size=len(encrypted_key),
            )

            return plaintext_key, encrypted_key

        except ClientError as e:
            logger.exception("Failed to generate data key")
            raise CryptoError(
                message=f"Failed to generate data key: {e}",
                operation="generate_data_key",
                algorithm="AES_256",
                cause=e,
            ) from e

    @aws_retry()
    def _decrypt_data_key(
        self,
        encrypted_key: bytes,
        context: dict[str, str] | None = None,
    ) -> bytes:
        """Decrypt a data encryption key using KMS."""
        try:
            params: dict[str, Any] = {
                "CiphertextBlob": encrypted_key,
                "KeyId": self._key_id,
            }

            if context:
                params["EncryptionContext"] = context

            response = self._client.decrypt(**params)
            return response["Plaintext"]

        except ClientError as e:
            error_code = e.response.get("Error", {}).get("Code", "Unknown")

            if error_code == "InvalidCiphertextException":
                raise CryptoError(
                    message="Invalid ciphertext - key may have been tampered with",
                    operation="decrypt_data_key",
                    cause=e,
                ) from e

            if error_code == "IncorrectKeyException":
                raise CryptoError(
                    message="Incorrect KMS key used for decryption",
                    operation="decrypt_data_key",
                    cause=e,
                ) from e

            logger.exception("Failed to decrypt data key")
            raise CryptoError(
                message=f"Failed to decrypt data key: {e}",
                operation="decrypt_data_key",
                cause=e,
            ) from e

    def encrypt(
        self,
        plaintext: bytes,
        context: dict[str, str] | None = None,
    ) -> bytes:
        """
        Encrypt data using envelope encryption (AES-256-GCM).

        Format of returned ciphertext:
        [encrypted_key_length (4 bytes)]
        [encrypted_key (variable)]
        [nonce (12 bytes)]
        [ciphertext + tag (variable)]
        """
        try:
            plaintext_key, encrypted_key = self.generate_data_key(context)

            nonce = os.urandom(GCM_NONCE_SIZE)

            aesgcm = AESGCM(plaintext_key)
            ciphertext = aesgcm.encrypt(nonce, plaintext, None)

            key_len_bytes = len(encrypted_key).to_bytes(4, byteorder="big")
            envelope = bytes(key_len_bytes + encrypted_key + nonce + ciphertext)

            logger.debug(
                "Data encrypted",
                plaintext_size=len(plaintext),
                envelope_size=len(envelope),
            )

            return envelope

        except Exception as e:
            if isinstance(e, CryptoError):
                raise
            logger.exception("Encryption failed")
            raise CryptoError(
                message=f"Encryption failed: {e}",
                operation="encrypt",
                algorithm="AES/GCM/NoPadding",
                cause=e,
            ) from e

    def decrypt(
        self,
        ciphertext: bytes,
        context: dict[str, str] | None = None,
    ) -> bytes:
        """
        Decrypt data using envelope encryption (AES-256-GCM).

        Parses the envelope format and decrypts using KMS for key decryption.
        """
        try:
            if len(ciphertext) < 4:
                raise CryptoError(
                    message="Ciphertext too short - invalid envelope",
                    operation="decrypt",
                )

            key_len = int.from_bytes(ciphertext[:4], byteorder="big")
            offset = 4

            if len(ciphertext) < offset + key_len + GCM_NONCE_SIZE:
                raise CryptoError(
                    message="Ciphertext too short - invalid envelope format",
                    operation="decrypt",
                )

            encrypted_key = ciphertext[offset : offset + key_len]
            offset += key_len

            nonce = ciphertext[offset : offset + GCM_NONCE_SIZE]
            offset += GCM_NONCE_SIZE

            encrypted_data = ciphertext[offset:]

            plaintext_key = self._decrypt_data_key(encrypted_key, context)

            aesgcm = AESGCM(plaintext_key)
            plaintext = bytes(aesgcm.decrypt(nonce, encrypted_data, None))

            logger.debug(
                "Data decrypted",
                ciphertext_size=len(ciphertext),
                plaintext_size=len(plaintext),
            )

            return plaintext

        except Exception as e:
            if isinstance(e, CryptoError):
                raise
            logger.exception("Decryption failed")
            raise CryptoError(
                message=f"Decryption failed: {e}",
                operation="decrypt",
                algorithm="AES/GCM/NoPadding",
                cause=e,
            ) from e

    def compute_hash(self, data: bytes, algorithm: str = "SHA-256") -> str:
        """
        Compute a cryptographic hash of data.

        FIPS 140-3 approved algorithms:
        - SHA-256 (default)
        - SHA-384
        - SHA-512

        SHA-1 and MD5 are explicitly NOT supported.
        """
        algorithm_upper = algorithm.upper().replace("-", "")

        hash_functions = {
            "SHA256": hashlib.sha256,
            "SHA384": hashlib.sha384,
            "SHA512": hashlib.sha512,
        }

        if algorithm_upper not in hash_functions:
            raise CryptoError(
                message=f"Unsupported hash algorithm: {algorithm}. "
                f"FIPS 140-3-oriented options: SHA-256, SHA-384, SHA-512",
                operation="hash",
                algorithm=algorithm,
            )

        hash_func = hash_functions[algorithm_upper]
        digest = hash_func(data).hexdigest()

        logger.debug(
            "Hash computed",
            algorithm=algorithm,
            data_size=len(data),
        )

        return digest

    @aws_retry()
    def verify_key_access(self) -> bool:
        """Verify that we have access to the KMS key."""
        try:
            self._client.describe_key(KeyId=self._key_id)
            logger.info("KMS key access verified")
            return True
        except ClientError as e:
            error_code = e.response.get("Error", {}).get("Code", "Unknown")
            logger.error(
                "KMS key access verification failed",
                error_code=error_code,
            )
            if is_retryable_aws_error(e):
                raise CryptoError(
                    message=f"Transient KMS access verification failure: {e}",
                    operation="describe_key",
                    cause=e,
                ) from e
            return False


class LocalCryptoProvider(CryptoProvider):
    """
    Local crypto provider for testing without AWS KMS.

    WARNING: This is not part of the FIPS 140-3-oriented runtime and should only be used
    for local development and testing with LocalStack.
    """

    def __init__(self, static_key: bytes | None = None) -> None:
        """
        Initialize local crypto provider.

        Args:
            static_key: Optional static key (32 bytes for AES-256).
                       If not provided, generates a random key.
        """
        if static_key:
            if len(static_key) != 32:
                raise ValueError("Static key must be 32 bytes for AES-256")
            self._master_key = static_key
        else:
            self._master_key = os.urandom(32)

        logger.warning(
            "Using LocalCryptoProvider - not part of the FIPS 140-3-oriented runtime!",
            use_case="local development only",
        )

    def generate_data_key(
        self,
        context: dict[str, str] | None = None,
    ) -> tuple[bytes, bytes]:
        """Generate a data key (local, not KMS-backed)."""
        plaintext_key = os.urandom(32)

        encrypted_key = bytes(a ^ b for a, b in zip(plaintext_key, self._master_key * 2))

        return plaintext_key, encrypted_key

    def encrypt(self, plaintext: bytes, context: dict[str, str] | None = None) -> bytes:
        """Encrypt using local key."""
        plaintext_key, encrypted_key = self.generate_data_key(context)

        nonce = os.urandom(GCM_NONCE_SIZE)
        aesgcm = AESGCM(plaintext_key)
        ciphertext = aesgcm.encrypt(nonce, plaintext, None)

        key_len_bytes = len(encrypted_key).to_bytes(4, byteorder="big")
        return bytes(key_len_bytes + encrypted_key + nonce + ciphertext)

    def decrypt(self, ciphertext: bytes, context: dict[str, str] | None = None) -> bytes:
        """Decrypt using local key."""
        key_len = int.from_bytes(ciphertext[:4], byteorder="big")
        offset = 4

        encrypted_key = ciphertext[offset : offset + key_len]
        offset += key_len

        nonce = ciphertext[offset : offset + GCM_NONCE_SIZE]
        offset += GCM_NONCE_SIZE

        encrypted_data = ciphertext[offset:]

        plaintext_key = bytes(a ^ b for a, b in zip(encrypted_key, self._master_key * 2))

        aesgcm = AESGCM(plaintext_key)
        return bytes(aesgcm.decrypt(nonce, encrypted_data, None))

    def compute_hash(self, data: bytes, algorithm: str = "SHA-256") -> str:
        """Compute hash (same as KMS provider)."""
        algorithm_upper = algorithm.upper().replace("-", "")

        hash_functions = {
            "SHA256": hashlib.sha256,
            "SHA384": hashlib.sha384,
            "SHA512": hashlib.sha512,
        }

        if algorithm_upper not in hash_functions:
            raise CryptoError(
                message=f"Unsupported hash algorithm: {algorithm}",
                operation="hash",
                algorithm=algorithm,
            )

        return hash_functions[algorithm_upper](data).hexdigest()
