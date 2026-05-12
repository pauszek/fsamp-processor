# =============================================================================
# Unit Tests for FIPS Runtime Enforcement
# =============================================================================
"""Fail-closed checks for FIPS runtime enforcement."""

import pytest

from processor.config import Settings
from processor.infrastructure import fips


class FakeOpenSslFunction:
    """Callable that behaves enough like a ctypes OpenSSL function for tests."""

    def __init__(self, return_value: int) -> None:
        self.return_value = return_value
        self.argtypes = None
        self.restype = None

    def __call__(self, *_args: object) -> int:
        return self.return_value


def test_non_local_environment_requires_fips_by_default() -> None:
    settings = Settings(environment="dev", aws_endpoint_url=None)

    assert settings.should_require_fips is True


def test_localstack_environment_does_not_require_fips() -> None:
    settings = Settings(environment="local", aws_endpoint_url="http://localhost:4566")

    assert settings.should_require_fips is False


def test_enforce_fips_raises_when_required_and_not_enabled(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(fips, "is_fips_enabled", lambda: False)

    with pytest.raises(RuntimeError, match="FIPS mode is required"):
        fips.enforce_fips(required=True)


def test_enforce_fips_allows_local_disabled_mode(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(fips, "is_fips_enabled", lambda: False)

    fips.enforce_fips(required=False)


def test_enforce_fips_allows_required_enabled_mode(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(fips, "is_fips_enabled", lambda: True)

    fips.enforce_fips(required=True)


def test_libcrypto_check_returns_none_when_library_cannot_load(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(fips.ctypes.util, "find_library", lambda _name: "missing-libcrypto")

    def raise_os_error(_name: str) -> object:
        raise OSError("not found")

    monkeypatch.setattr(fips.ctypes, "CDLL", raise_os_error)

    assert fips._is_fips_enabled_via_libcrypto() is None


def test_libcrypto_check_returns_none_when_symbols_are_missing(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(fips.ctypes.util, "find_library", lambda _name: "libcrypto.so.3")
    monkeypatch.setattr(fips.ctypes, "CDLL", lambda _name: object())

    assert fips._is_fips_enabled_via_libcrypto() is None


def test_libcrypto_check_returns_false_when_openssl_init_fails(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    fake_libcrypto = type(
        "FakeLibCrypto",
        (),
        {
            "OPENSSL_init_crypto": FakeOpenSslFunction(0),
            "EVP_default_properties_is_fips_enabled": FakeOpenSslFunction(1),
        },
    )()
    monkeypatch.setattr(fips.ctypes, "CDLL", lambda _name: fake_libcrypto)

    assert fips._is_fips_enabled_via_libcrypto() is False


def test_libcrypto_check_returns_default_property_status(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    fake_libcrypto = type(
        "FakeLibCrypto",
        (),
        {
            "OPENSSL_init_crypto": FakeOpenSslFunction(1),
            "EVP_default_properties_is_fips_enabled": FakeOpenSslFunction(1),
        },
    )()
    monkeypatch.setattr(fips.ctypes, "CDLL", lambda _name: fake_libcrypto)

    assert fips._is_fips_enabled_via_libcrypto() is True


def test_is_fips_enabled_uses_binding_evp_fallback(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(fips, "_is_fips_enabled_via_libcrypto", lambda: None)

    fake_binding = type(
        "FakeBinding",
        (),
        {
            "lib": type(
                "FakeLib",
                (),
                {"EVP_default_properties_is_fips_enabled": staticmethod(lambda _null: 1)},
            )(),
            "ffi": type("Ffi", (), {"NULL": None})(),
        },
    )
    monkeypatch.setattr(fips, "Binding", lambda: fake_binding)

    assert fips.is_fips_enabled() is True


def test_is_fips_enabled_returns_system_libcrypto_result(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(fips, "_is_fips_enabled_via_libcrypto", lambda: True)

    assert fips.is_fips_enabled() is True


def test_is_fips_enabled_uses_legacy_fips_mode_fallback(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(fips, "_is_fips_enabled_via_libcrypto", lambda: None)

    fake_binding = type(
        "FakeBinding",
        (),
        {"lib": type("FakeLib", (), {"FIPS_mode": staticmethod(lambda: 1)})(), "ffi": object()},
    )
    monkeypatch.setattr(fips, "Binding", lambda: fake_binding)

    assert fips.is_fips_enabled() is True


def test_is_fips_enabled_returns_false_when_no_supported_symbol(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(fips, "_is_fips_enabled_via_libcrypto", lambda: None)
    fake_binding = type("FakeBinding", (), {"lib": object(), "ffi": object()})
    monkeypatch.setattr(fips, "Binding", lambda: fake_binding)

    assert fips.is_fips_enabled() is False
