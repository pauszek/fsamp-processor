import os
from collections.abc import Generator

import boto3
import pytest
from testcontainers.localstack import LocalStackContainer

_localstack_container: LocalStackContainer | None = None


def get_localstack_container() -> LocalStackContainer:
    global _localstack_container

    if _localstack_container is None:
        _localstack_container = LocalStackContainer(image="localstack/localstack-pro:4.14.0")
        _localstack_container.with_services(
            "s3", "sqs", "sns", "dynamodb", "kms", "iam", "sts", "cloudwatch", "logs"
        )

        auth_token = os.environ.get("LOCALSTACK_AUTH_TOKEN", "")
        if auth_token:
            _localstack_container.with_env("LOCALSTACK_AUTH_TOKEN", auth_token)

        _localstack_container.with_env("ENFORCE_IAM", "1")
        _localstack_container.with_env("IAM_SOFT_MODE", "0")

        _localstack_container.start()

    return _localstack_container


@pytest.fixture(scope="session")
def localstack() -> Generator[LocalStackContainer]:
    container = get_localstack_container()
    yield container


@pytest.fixture(scope="session")
def localstack_endpoint(localstack: LocalStackContainer) -> str:
    return localstack.get_url()


@pytest.fixture
def localstack_s3_client(localstack_endpoint: str) -> Generator[boto3.client]:
    client = boto3.client(
        "s3",
        endpoint_url=localstack_endpoint,
        aws_access_key_id="test",
        aws_secret_access_key="test",
        region_name="us-west-2",
    )
    yield client


@pytest.fixture
def localstack_sqs_client(localstack_endpoint: str) -> Generator[boto3.client]:
    client = boto3.client(
        "sqs",
        endpoint_url=localstack_endpoint,
        aws_access_key_id="test",
        aws_secret_access_key="test",
        region_name="us-west-2",
    )
    yield client


@pytest.fixture
def localstack_sns_client(localstack_endpoint: str) -> Generator[boto3.client]:
    client = boto3.client(
        "sns",
        endpoint_url=localstack_endpoint,
        aws_access_key_id="test",
        aws_secret_access_key="test",
        region_name="us-west-2",
    )
    yield client


@pytest.fixture
def localstack_dynamodb_client(localstack_endpoint: str) -> Generator[boto3.client]:
    client = boto3.client(
        "dynamodb",
        endpoint_url=localstack_endpoint,
        aws_access_key_id="test",
        aws_secret_access_key="test",
        region_name="us-west-2",
    )
    yield client


@pytest.fixture
def localstack_kms_client(localstack_endpoint: str) -> Generator[boto3.client]:
    client = boto3.client(
        "kms",
        endpoint_url=localstack_endpoint,
        aws_access_key_id="test",
        aws_secret_access_key="test",
        region_name="us-west-2",
    )
    yield client


@pytest.fixture
def localstack_bucket(localstack_s3_client: boto3.client) -> str:
    bucket_name = "test-integration-bucket"

    try:
        localstack_s3_client.create_bucket(
            Bucket=bucket_name,
            CreateBucketConfiguration={"LocationConstraint": "us-west-2"},
        )
    except localstack_s3_client.exceptions.BucketAlreadyOwnedByYou:
        pass  # Bucket exists from previous test

    return bucket_name


@pytest.fixture
def localstack_queue_url(localstack_sqs_client: boto3.client) -> str:
    import uuid

    queue_name = f"test-queue-{uuid.uuid4().hex[:8]}"

    response = localstack_sqs_client.create_queue(
        QueueName=queue_name,
        Attributes={
            "VisibilityTimeout": "30",
            "MessageRetentionPeriod": "86400",
        },
    )
    return response["QueueUrl"]


@pytest.fixture
def localstack_topic_arn(localstack_sns_client: boto3.client) -> str:
    import uuid

    topic_name = f"test-topic-{uuid.uuid4().hex[:8]}"

    response = localstack_sns_client.create_topic(Name=topic_name)
    return response["TopicArn"]


@pytest.fixture
def localstack_table_name(localstack_dynamodb_client: boto3.client) -> str:
    import uuid

    table_name = f"test-table-{uuid.uuid4().hex[:8]}"

    try:
        localstack_dynamodb_client.create_table(
            TableName=table_name,
            KeySchema=[
                {"AttributeName": "PK", "KeyType": "HASH"},
                {"AttributeName": "SK", "KeyType": "RANGE"},
            ],
            AttributeDefinitions=[
                {"AttributeName": "PK", "AttributeType": "S"},
                {"AttributeName": "SK", "AttributeType": "S"},
                {"AttributeName": "GSI1PK", "AttributeType": "S"},
                {"AttributeName": "GSI1SK", "AttributeType": "S"},
            ],
            GlobalSecondaryIndexes=[
                {
                    "IndexName": "GSI1",
                    "KeySchema": [
                        {"AttributeName": "GSI1PK", "KeyType": "HASH"},
                        {"AttributeName": "GSI1SK", "KeyType": "RANGE"},
                    ],
                    "Projection": {"ProjectionType": "ALL"},
                },
            ],
            BillingMode="PAY_PER_REQUEST",
        )

        waiter = localstack_dynamodb_client.get_waiter("table_exists")
        waiter.wait(TableName=table_name)

    except localstack_dynamodb_client.exceptions.ResourceInUseException:
        pass  # Table exists

    return table_name


@pytest.fixture
def localstack_kms_key_id(localstack_kms_client: boto3.client) -> str:
    response = localstack_kms_client.create_key(
        Description="Test key for integration tests",
        KeyUsage="ENCRYPT_DECRYPT",
    )
    return response["KeyMetadata"]["KeyId"]


@pytest.fixture(autouse=True)
def localstack_env(localstack_endpoint: str) -> Generator[None]:
    original_env = os.environ.copy()

    os.environ["AWS_ENDPOINT_URL"] = localstack_endpoint
    os.environ["AWS_ACCESS_KEY_ID"] = "test"
    os.environ["AWS_SECRET_ACCESS_KEY"] = "test"
    os.environ["AWS_DEFAULT_REGION"] = "us-west-2"

    yield

    os.environ.clear()
    os.environ.update(original_env)
