# FSAMP Processor

[![Python](https://img.shields.io/badge/Python-3.14+-3776AB?logo=python)](https://www.python.org/)
[![AWS Lambda](https://img.shields.io/badge/AWS-Lambda-FF9900?logo=awslambda)](https://aws.amazon.com/lambda/)
[![FIPS 140-3](https://img.shields.io/badge/FIPS-140--3-green)](https://csrc.nist.gov/publications/detail/fips/140/3/final)

> Event-driven file processor for FSAMP platform with a FIPS 140-3-oriented security posture.
> Core runtime: **AWS Lambda** with SQS trigger. ECS Fargate remains an optional demonstration profile.

## 🏗️ Architecture

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                         DEPLOYMENT OPTIONS                                   │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                              │
│   ┌────────────────────────────────────────────────────────────────────┐    │
│   │  PRODUCTION: AWS Lambda                                             │    │
│   │  ┌──────────┐     ┌────────────────┐     ┌──────────────────┐     │    │
│   │  │   SQS    │────▶│ Lambda Handler │────▶│ FileProcessor    │     │    │
│   │  │  Queue   │     │ (Powertools)   │     │ Service          │     │    │
│   │  └──────────┘     └────────────────┘     └──────────────────┘     │    │
│   │  • Event Source Mapping          • Batch processing                │    │
│   │  • Auto-scaling 0→1000           • Partial batch response          │    │
│   │  • Pay-per-invocation            • X-Ray tracing                   │    │
│   └────────────────────────────────────────────────────────────────────┘    │
│                                                                              │
│   ┌────────────────────────────────────────────────────────────────────┐    │
│   │  PRODUCTION: ECS Fargate (long-running / large files)              │    │
│   │  ┌──────────┐     ┌────────────────┐     ┌──────────────────┐     │    │
│   │  │   SQS    │────▶│ SQS Consumer   │────▶│ FileProcessor    │     │    │
│   │  │  Queue   │     │ (Long-poll)    │     │ Service          │     │    │
│   │  └──────────┘     └────────────────┘     └──────────────────┘     │    │
│   │  • Long-polling consumer         • Same core processing logic      │    │
│   │  • Unlimited timeout             • Container-based                 │    │
│   └────────────────────────────────────────────────────────────────────┘    │
└─────────────────────────────────────────────────────────────────────────────┘
```

### Hexagonal Architecture (Ports & Adapters)

```
┌─────────────────────────────────────────────────────────────────────────────┐
│   ┌──────────────────────────────────────────────────────────────────────┐  │
│   │                         ADAPTERS (IN)                                 │  │
│   │   ┌─────────────────┐    ┌─────────────────┐                         │  │
│   │   │ Lambda Handler  │    │  SQS Consumer   │                         │  │
│   │   │ (Powertools)    │    │  (ECS mode)     │                         │  │
│   │   └────────┬────────┘    └────────┬────────┘                         │  │
│   └────────────┼──────────────────────┼──────────────────────────────────┘  │
│                │                      │                                      │
│   ┌────────────▼──────────────────────▼──────────────────────────────────┐  │
│   │                         DOMAIN (CORE)                                 │  │
│   │   ┌──────────────┐    ┌──────────────┐    ┌──────────────────────┐  │  │
│   │   │ FileEvent    │    │ FileProcessor│    │   Ports (Interfaces) │  │  │
│   │   │ (Pydantic)   │    │  Service     │    │   - FileStorage      │  │  │
│   │   └──────────────┘    └──────────────┘    │   - MetadataRepo     │  │  │
│   │                                            │   - EventPublisher   │  │  │
│   │                                            │   - CryptoProvider   │  │  │
│   └────────────┬─────────────────────────────────────────────────────────┘  │
│                │                                                             │
│   ┌────────────▼─────────────────────────────────────────────────────────┐  │
│   │                         ADAPTERS (OUT)                                │  │
│   │   ┌────────────┐  ┌──────────────┐  ┌───────────┐  ┌──────────────┐ │  │
│   │   │ S3 Storage │  │ DynamoDB Repo│  │ SNS Pub   │  │ KMS Crypto   │ │  │
│   │   └────────────┘  └──────────────┘  └───────────┘  └──────────────┘ │  │
│   └──────────────────────────────────────────────────────────────────────┘  │
└─────────────────────────────────────────────────────────────────────────────┘
```

## 📁 Project Structure

```
fsamp-processor/
├── src/
│   └── processor/
│       ├── __init__.py
│       ├── main.py                 # ECS/Container entry point
│       ├── lambda_handler.py       # AWS Lambda entry point ⚡
│       ├── config.py               # Pydantic settings
│       ├── domain/
│       │   ├── events.py           # Event models (Pydantic)
│       │   ├── models.py           # Domain models
│       │   └── exceptions.py       # Domain exceptions
│       ├── ports/
│       │   ├── inbound.py          # Input ports (interfaces)
│       │   └── outbound.py         # Output ports (interfaces)
│       ├── adapters/
│       │   ├── inbound/
│       │   │   └── sqs_consumer.py # SQS consumer (ECS mode)
│       │   └── outbound/
│       │       ├── s3_storage.py   # S3 file storage
│       │       ├── dynamodb_repo.py # DynamoDB repository
│       │       ├── sns_publisher.py # SNS publisher
│       │       └── kms_crypto.py   # KMS crypto (FIPS)
│       ├── application/
│       │   └── file_processor.py   # Core processing service
│       └── infrastructure/
│           ├── logging.py          # Structured logging
│           └── aws_clients.py      # AWS client factory
├── tests/
│   ├── test_lambda_handler.py      # Lambda-specific tests
│   └── ...
├── events/                          # Sample Lambda events
│   ├── sample-sqs-event.json
│   └── sample-sns-wrapped-event.json
├── template.yaml                    # SAM template for local dev
├── Dockerfile                       # ECS container image
├── Dockerfile.lambda                # Lambda container image
├── deploy-lambda.sh                 # Lambda deployment script
├── env.json                         # SAM local env vars
├── pyproject.toml
└── README.md
```

## 🚀 Quick Start

### Prerequisites

- Python 3.14+
- Docker & Docker Compose
- AWS SAM CLI (for local Lambda testing)
- LocalStack (for local development)

### Lambda Deployment (Recommended)

```bash
# 1. Build and package Lambda
./deploy-lambda.sh build

# 2. Test locally with SAM
sam build && sam local invoke ProcessorFunction -e events/sample-sqs-event.json

# 3. Deploy to AWS (after Terraform has created the function)
./deploy-lambda.sh deploy dev
```

### Local Development with SAM

```bash
# 1. Start LocalStack (from fsamp-infra)
cd ../fsamp-infra && make up && make apply-local

# 2. Build with SAM
cd ../fsamp-processor
sam build

# 3. Invoke Lambda locally
sam local invoke ProcessorFunction \
    -e events/sample-sqs-event.json \
    --env-vars env.json
```

### ECS/Container Mode

```bash
# 1. Create virtual environment
python -m venv .venv
source .venv/bin/activate

# 2. Install dependencies
pip install -e ".[dev]"

# 3. Start LocalStack
cd ../fsamp-infra && make up && make apply-local

# 4. Run processor (long-polling mode)
cd ../fsamp-processor
python -m processor.main
```

### Docker

```bash
# Build Lambda container image
docker build -f Dockerfile.lambda -t fsamp-processor-lambda:latest .

# Build ECS container image  
docker build -t fsamp-processor:latest .

# Run with LocalStack
docker-compose up
```

## 🔒 FIPS 140-3-Oriented Security

This processor is designed for a FIPS 140-3-oriented runtime:

- **Encryption**: AES-256-GCM via AWS KMS
- **Key Management**: AWS KMS with automatic key rotation
- **Library**: Python `cryptography` package (OpenSSL FIPS provider)
- **Enforcement**: FIPS mode is required in non-local environments (override via `FIPS_REQUIRED=false` if needed)

## ✅ Enterprise Controls (FedRAMP-aligned)

- **FedRAMP Moderate aligned** baseline (not FedRAMP authorized) with NIST SP 800-53 Rev. 5 mappings at the platform level.
- **FIPS 140-3-oriented crypto** with OpenSSL FIPS provider + AWS KMS; optional AWS FIPS endpoints via `USE_FIPS_ENDPOINT` in US regions.
- **Audit & monitoring** using structured JSON logs and CloudWatch logs/metrics for both Lambda and ECS modes.
- **Resilience** with SQS DLQ handling and at-least-once delivery semantics.
- **Least-privilege IAM** roles for SQS/SNS/S3/DynamoDB/KMS access.

## ⚙️ Configuration

Environment variables:

| Variable | Description | Default | Lambda | ECS |
|----------|-------------|---------|--------|-----|
| `ENVIRONMENT` | Deployment environment | `local` | ✅ | ✅ |
| `AWS_REGION` | AWS region | `us-west-2` | ✅ | ✅ |
| `AWS_ENDPOINT_URL` | LocalStack endpoint | `None` | ✅ | ✅ |
| `USE_FIPS_ENDPOINT` | Use AWS FIPS endpoints (us-*) | `true` | ✅ | ✅ |
| `FIPS_REQUIRED` | Enforce OpenSSL FIPS mode | `true` (non-local) | ✅ | ✅ |
| `SQS_QUEUE_URL` | Processing queue URL | Required | ❌ | ✅ |
| `SNS_TOPIC_ARN` | Event topic ARN | Required | ✅ | ✅ |
| `S3_BUCKET_NAME` | File storage bucket | Required | ✅ | ✅ |
| `DYNAMODB_TABLE_NAME` | Metadata table | Required | ✅ | ✅ |
| `KMS_KEY_ID` | Encryption key ID | Required | ✅ | ✅ |
| `LOG_LEVEL` | Logging level | `INFO` | ✅ | ✅ |
| `POWERTOOLS_SERVICE_NAME` | Service name for observability | `fsamp-processor` | ✅ | ❌ |
| `POWERTOOLS_METRICS_NAMESPACE` | CloudWatch namespace | `FSAMP/Processor` | ✅ | ❌ |

## 🧪 Testing

```bash
# Run all tests
pytest

# Run with coverage
pytest --cov=processor --cov-report=html

# Run only unit tests
pytest -m unit

# Run Lambda handler tests
pytest tests/test_lambda_handler.py -v

# Run integration tests (requires LocalStack)
pytest -m integration
```

## 📊 Observability

### AWS Lambda Powertools

The Lambda handler uses [AWS Lambda Powertools](https://docs.powertools.aws.dev/lambda/python/):

- **Logger**: Structured JSON logging with correlation IDs
- **Tracer**: X-Ray distributed tracing
- **Metrics**: CloudWatch EMF metrics
- **Batch**: Partial batch response for SQS

### Custom Metrics

| Metric | Description | Unit |
|--------|-------------|------|
| `ProcessedFiles` | Successfully processed files | Count |
| `ProcessingDuration` | Time to process file | Milliseconds |
| `RetryableErrors` | Errors that trigger retry | Count |
| `NonRetryableErrors` | Errors that skip message | Count |
| `BatchSize` | Messages per Lambda invocation | Count |

---

## 🔗 Related Repositories

| Repository | Description |
|---|---|
| **fsamp-gateway** | Spring Boot API gateway — file upload, download, auth, resilience |
| **fsamp-infra** | Terraform IaC, Docker Compose, e2e tests, load tests |
| **fsamp-event-schema** | Canonical JSON Schema for domain events |
| **fsamp-code-ci** | Reusable GitHub Actions workflows & composite actions |

### Central Compliance Documentation (fsamp-infra)

| Document | Path |
|---|---|
| FedRAMP-aligned SSP | `docs/compliance/FEDRAMP_SSP.md` |
| NIST 800-53 Control Matrix | `docs/compliance/NIST_800_53_CONTROLS.md` |
| Security Audit Report | `docs/compliance/SECURITY_AUDIT_REPORT.md` |
| TLS Architecture | `docs/TLS_ARCHITECTURE.md` |
| Architecture Decision Records | `docs/adr/` |

## 📄 License

MIT
