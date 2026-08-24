"""Early OpenSSL configuration bootstrap using only the Python standard library."""

import ctypes
import ctypes.util
import sys
from typing import cast

OPENSSL_INIT_LOAD_CONFIG = 0x00000040


def _load_libcrypto() -> ctypes.CDLL | None:
    if sys.platform != "linux":
        return None

    library_name = ctypes.util.find_library("crypto") or "libcrypto.so.3"
    try:
        return ctypes.CDLL(library_name)
    except OSError:
        return None


def _initialize_openssl_config(libcrypto: object) -> bool | None:
    init_crypto = getattr(libcrypto, "OPENSSL_init_crypto", None)
    if init_crypto is None:
        return None

    init_crypto.argtypes = [ctypes.c_uint64, ctypes.c_void_p]
    init_crypto.restype = ctypes.c_int
    return cast(int, init_crypto(OPENSSL_INIT_LOAD_CONFIG, None)) == 1


def initialize_openssl_config() -> bool | None:
    """Load the process OpenSSL configuration before crypto-capable dependencies."""
    libcrypto = _load_libcrypto()
    if libcrypto is None:
        return None
    return _initialize_openssl_config(libcrypto)
