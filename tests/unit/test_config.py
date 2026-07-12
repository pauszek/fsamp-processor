import pytest

from processor.config import Settings, get_settings


def test_environment_and_log_level_are_normalized() -> None:
    settings = Settings(environment="PROD", log_level="debug")

    assert settings.environment == "prod"
    assert settings.log_level == "DEBUG"
    assert settings.is_production is True


def test_non_string_validator_inputs_are_returned_unchanged() -> None:
    assert Settings.lowercase_environment(123) == 123
    assert Settings.uppercase_log_level(123) == 123


def test_should_use_fips_requires_supported_region_and_no_custom_endpoint() -> None:
    assert Settings(environment="prod", aws_region="us-west-2").should_use_fips is True
    with pytest.raises(ValueError, match="Custom AWS endpoints"):
        Settings(
            environment="prod",
            aws_region="us-west-2",
            aws_endpoint_url="http://localhost:4566",
        )
    with pytest.raises(ValueError, match="must enable AWS FIPS"):
        Settings(
            environment="prod",
            aws_region="us-west-2",
            use_fips_endpoint=False,
        )


@pytest.mark.parametrize("region", ["us-east-1", "eu-west-1"])
def test_should_use_fips_fails_closed_for_unsupported_deployment_region(region: str) -> None:
    with pytest.raises(ValueError, match="require region us-west-2"):
        _ = Settings(environment="prod", aws_region=region).should_use_fips


@pytest.mark.parametrize("region", ["us-east-1", "eu-west-1"])
def test_should_use_fips_fails_closed_for_unsupported_region_when_fips_disabled(
    region: str,
) -> None:
    with pytest.raises(ValueError, match="require region us-west-2"):
        _ = Settings(
            environment="prod",
            aws_region=region,
            use_fips_endpoint=False,
        ).should_use_fips


def test_fips_required_override_takes_precedence() -> None:
    with pytest.raises(ValueError, match="cannot disable FIPS"):
        Settings(environment="prod", fips_required=False)
    assert Settings(environment="local", fips_required=True).should_require_fips is True


def test_processor_runtime_configuration_fails_fast() -> None:
    settings = Settings(
        environment="local",
        s3_bucket_name="",
        dynamodb_table_name="",
        kms_key_id="",
        sqs_queue_url="",
        sns_topic_arn="",
    )
    with pytest.raises(ValueError, match="S3_BUCKET_NAME"):
        settings.validate_processor_runtime()


def test_lambda_runtime_name_auto_detects_lambda() -> None:
    settings = Settings(AWS_LAMBDA_FUNCTION_NAME="fsamp-local-processor")

    assert settings.aws_lambda_function_name == "fsamp-local-processor"
    assert settings.is_lambda is True


def test_json_logging_flag() -> None:
    assert Settings(log_format="json").use_json_logging is True
    assert Settings(log_format="console").use_json_logging is False


def test_get_settings_returns_cached_instance() -> None:
    get_settings.cache_clear()

    first = get_settings()
    second = get_settings()

    assert first is second
    get_settings.cache_clear()
