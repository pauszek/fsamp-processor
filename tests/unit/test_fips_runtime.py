# =============================================================================
# Unit Tests for FIPS Runtime Enforcement
# =============================================================================
"""Fail-closed checks for FIPS runtime enforcement."""

import pytest

from processor.config import Settings
from processor.infrastructure import fips


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
