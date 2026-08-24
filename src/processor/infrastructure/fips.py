"""
Utilities for validating OpenSSL FIPS mode at runtime.
"""

import ctypes
from typing import cast

import structlog
from cryptography.hazmat.bindings.openssl.binding import Binding

from processor.openssl_runtime import _initialize_openssl_config, _load_libcrypto

logger = structlog.get_logger(__name__)


def _is_fips_enabled_via_libcrypto() -> bool | None:
    """
    Check OpenSSL 3 FIPS default properties through the system libcrypto.

    Newer cryptography releases do not expose every OpenSSL symbol through
    Binding().lib, so this path verifies the same libcrypto used by Python ssl
    and the source-built cryptography extension in the Lambda image.
    """
    libcrypto = _load_libcrypto()
    if libcrypto is None:
        logger.debug("Unable to load libcrypto for FIPS check")
        return None

    is_fips_enabled = getattr(libcrypto, "EVP_default_properties_is_fips_enabled", None)
    if is_fips_enabled is None:
        return None

    is_fips_enabled.argtypes = [ctypes.c_void_p]
    is_fips_enabled.restype = ctypes.c_int

    initialization_result = _initialize_openssl_config(libcrypto)
    if initialization_result is None:
        return None
    if not initialization_result:
        return False

    return cast(int, is_fips_enabled(None)) == 1


def is_fips_enabled() -> bool:
    """
    Return True if OpenSSL FIPS mode is enabled.

    Supports both OpenSSL 3 (EVP_default_properties_is_fips_enabled)
    and OpenSSL 1.1.1 (FIPS_mode), falling back to False if unavailable.
    """
    system_result = _is_fips_enabled_via_libcrypto()
    if system_result is not None:
        return system_result

    binding = Binding()
    lib = binding.lib
    ffi = binding.ffi

    if hasattr(lib, "EVP_default_properties_is_fips_enabled"):
        return cast(int, lib.EVP_default_properties_is_fips_enabled(ffi.NULL)) == 1

    if hasattr(lib, "FIPS_mode"):
        return cast(int, lib.FIPS_mode()) == 1

    return False


def enforce_fips(required: bool) -> None:
    """
    Enforce FIPS mode when required.

    Raises RuntimeError if FIPS is required but not enabled.
    """
    if not required:
        logger.info("FIPS enforcement disabled", fips_required=False)
        return

    enabled = is_fips_enabled()
    if not enabled:
        raise RuntimeError(
            "FIPS mode is required but not enabled. "
            "Verify OPENSSL_CONF points to a FIPS-enabled configuration."
        )

    logger.info("FIPS mode enabled", fips_required=True)
