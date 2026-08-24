import ctypes

import pytest

from processor import openssl_runtime


class FakeOpenSslFunction:
    def __init__(self, return_value: int) -> None:
        self.return_value = return_value
        self.argtypes: list[object] | None = None
        self.restype: object | None = None
        self.calls: list[tuple[object, object]] = []

    def __call__(self, flags: object, settings: object) -> int:
        self.calls.append((flags, settings))
        return self.return_value


def test_load_libcrypto_uses_discovered_library(monkeypatch: pytest.MonkeyPatch) -> None:
    fake_libcrypto = object()
    loaded_names: list[str] = []
    monkeypatch.setattr(openssl_runtime.sys, "platform", "linux")
    monkeypatch.setattr(openssl_runtime.ctypes.util, "find_library", lambda _name: "crypto-test")
    monkeypatch.setattr(
        openssl_runtime.ctypes,
        "CDLL",
        lambda name: loaded_names.append(name) or fake_libcrypto,
    )

    assert openssl_runtime._load_libcrypto() is fake_libcrypto
    assert loaded_names == ["crypto-test"]


def test_load_libcrypto_uses_openssl_3_fallback(monkeypatch: pytest.MonkeyPatch) -> None:
    loaded_names: list[str] = []
    monkeypatch.setattr(openssl_runtime.sys, "platform", "linux")
    monkeypatch.setattr(openssl_runtime.ctypes.util, "find_library", lambda _name: None)
    monkeypatch.setattr(
        openssl_runtime.ctypes,
        "CDLL",
        lambda name: loaded_names.append(name) or object(),
    )

    assert openssl_runtime._load_libcrypto() is not None
    assert loaded_names == ["libcrypto.so.3"]


def test_load_libcrypto_returns_none_when_library_cannot_load(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(openssl_runtime.sys, "platform", "linux")
    monkeypatch.setattr(openssl_runtime.ctypes.util, "find_library", lambda _name: "missing")

    def raise_os_error(_name: str) -> object:
        raise OSError("not found")

    monkeypatch.setattr(openssl_runtime.ctypes, "CDLL", raise_os_error)

    assert openssl_runtime._load_libcrypto() is None


def test_load_libcrypto_skips_non_linux_platform(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(openssl_runtime.sys, "platform", "darwin")

    def fail_if_called(_name: str) -> object:
        pytest.fail("libcrypto must not be loaded outside Linux")

    monkeypatch.setattr(openssl_runtime.ctypes, "CDLL", fail_if_called)

    assert openssl_runtime._load_libcrypto() is None


def test_initialize_returns_none_when_libcrypto_is_unavailable(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(openssl_runtime, "_load_libcrypto", lambda: None)

    assert openssl_runtime.initialize_openssl_config() is None


def test_initialize_returns_none_when_init_symbol_is_unavailable(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(openssl_runtime, "_load_libcrypto", lambda: object())

    assert openssl_runtime.initialize_openssl_config() is None


@pytest.mark.parametrize(("return_value", "expected"), [(0, False), (1, True)])
def test_initialize_loads_openssl_configuration(
    monkeypatch: pytest.MonkeyPatch,
    return_value: int,
    expected: bool,
) -> None:
    init_crypto = FakeOpenSslFunction(return_value)
    fake_libcrypto = type("FakeLibCrypto", (), {"OPENSSL_init_crypto": init_crypto})()
    monkeypatch.setattr(openssl_runtime, "_load_libcrypto", lambda: fake_libcrypto)

    assert openssl_runtime.initialize_openssl_config() is expected
    assert init_crypto.argtypes == [ctypes.c_uint64, ctypes.c_void_p]
    assert init_crypto.restype is ctypes.c_int
    assert init_crypto.calls == [(openssl_runtime.OPENSSL_INIT_LOAD_CONFIG, None)]
