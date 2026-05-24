# FSAMP Processor

[![Python](https://img.shields.io/badge/Python-3.14+-3776AB?logo=python)](https://www.python.org/)
[![AWS Lambda](https://img.shields.io/badge/AWS-Lambda-FF9900?logo=awslambda)](https://aws.amazon.com/lambda/)
[![FIPS 140-3](https://img.shields.io/badge/FIPS-140--3-oriented-green)](https://csrc.nist.gov/publications/detail/fips/140/3/final)

Event-driven file processor for the FSAMP platform. The primary runtime is AWS Lambda with an SQS trigger; the same core service can run as a long-polling ECS/Fargate container for larger files or operational experiments.

## Responsibilities

| Area | Implementation |
|---|---|
| Inbound events | SQS messages containing FSAMP file events |
| Processing | Metadata persistence, checksum validation, safe/unsafe classification |
| Storage | S3 read/write through outbound ports |
| Messaging | SNS publishing through direct or transactional outbox mode |
| Crypto posture | AWS KMS and OpenSSL FIPS provider where supported |
| Observability | Structured logs, Powertools metrics/tracing in Lambda |

## Structure

```text
src/processor/
├── lambda_handler.py      # Lambda entry point
├── main.py                # ECS/container entry point
├── outbox_publisher.py    # Outbox publisher Lambda
├── application/           # Processing orchestration
├── domain/                # Events, models, exceptions
├── ports/                 # Inbound and outbound interfaces
├── adapters/              # SQS, S3, DynamoDB, SNS, KMS adapters
└── infrastructure/        # Config, logging, AWS clients, FIPS checks
```

AWS resources and deployments are owned by `fsamp-infra`. This repo builds and tests the processor code and container images.

## Local Development

```bash
python3.14 -m venv .venv
source .venv/bin/activate
pip install -e ".[dev,test]"
```

Start LocalStack from the infra repository, then apply local Terraform:

```bash
cd ../fsamp-infra
make up
make apply-local
```

Run the processor locally:

```bash
cd ../fsamp-processor
python -m processor.main
```

## Docker

```bash
docker build -f Dockerfile.lambda -t fsamp-processor-lambda:latest .
docker build -t fsamp-processor:latest .
```

Use the infra compose stack for LocalStack instead of service-local compose files.

## Configuration

| Variable | Description | Default |
|---|---|---|
| `ENVIRONMENT` | `local`, `dev`, `staging`, `prod` | `local` |
| `AWS_REGION` | AWS region | `us-west-2` |
| `AWS_ENDPOINT_URL` | LocalStack endpoint | unset |
| `USE_FIPS_ENDPOINT` | Use AWS FIPS endpoints where supported | `true` |
| `FIPS_REQUIRED` | Fail closed when FIPS runtime is unavailable | non-local only |
| `SQS_QUEUE_URL` | Input queue URL for ECS mode | required |
| `SNS_TOPIC_ARN` | File event topic ARN | required |
| `S3_BUCKET_NAME` | File storage bucket | required |
| `DYNAMODB_TABLE_NAME` | Metadata table | required |
| `OUTBOX_TABLE_NAME` | Outbox table | optional |
| `KMS_KEY_ID` | Customer-managed KMS key | required |

## Tests

```bash
pytest
pytest --cov=processor --cov-report=html
pytest -m unit
pytest -m integration
```

Integration tests expect LocalStack credentials and services to be available.

## Deployment

Deployment is handled by `fsamp-infra/.github/workflows/deploy.yml`.

- Merge to `main` in this repo runs CI and dispatches a dev deployment.
- Manual promotion in `fsamp-infra` moves the same image tag through `dev -> staging -> prod`.
- Rollback in `fsamp-infra` redeploys a previous immutable image tag without rebuilding.

## Related Repositories

| Repository | Description |
|---|---|
| `fsamp-gateway` | Spring Boot API gateway for upload/auth/resilience |
| `fsamp-infra` | Terraform, deployment, LocalStack, e2e and load tests |
| `fsamp-event-schema` | Canonical event schema |
| `fsamp-code-ci` | Reusable CI workflows and composite actions |
