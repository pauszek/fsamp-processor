# =============================================================================
# FIPS Enforcement Utilities
# =============================================================================
"""
Utilities for validating OpenSSL FIPS mode at runtime.
"""

from typing import cast

import structlog
from cryptography.hazmat.bindings.openssl.binding import Binding

logger = structlog.get_logger(__name__)


def is_fips_enabled() -> bool:
    """
    Return True if OpenSSL FIPS mode is enabled.

    Supports both OpenSSL 3 (EVP_default_properties_is_fips_enabled)
    and OpenSSL 1.1.1 (FIPS_mode), falling back to False if unavailable.
    """
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
