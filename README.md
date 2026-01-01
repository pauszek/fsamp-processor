# FSAMP Processor

[![Python](https://img.shields.io/badge/Python-3.11+-3776AB?logo=python)](https://www.python.org/)
[![AWS](https://img.shields.io/badge/AWS-Cloud-FF9900?logo=amazonaws)](https://aws.amazon.com/)
[![FIPS 140-3](https://img.shields.io/badge/FIPS-140--3-green)](https://csrc.nist.gov/publications/detail/fips/140/3/final)

> Event-driven file processor for FSAMP platform with FIPS 140-3 compliance.

## 🏗️ Architecture

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                    Hexagonal Architecture (Ports & Adapters)                │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                              │
│   ┌──────────────────────────────────────────────────────────────────────┐  │
│   │                         ADAPTERS (IN)                                 │  │
│   │   ┌─────────────────┐                                                │  │
│   │   │  SQS Consumer   │  ← Receives events from SQS queue              │  │
│   │   └────────┬────────┘                                                │  │
│   └────────────┼─────────────────────────────────────────────────────────┘  │
│                │                                                             │
│   ┌────────────▼─────────────────────────────────────────────────────────┐  │
│   │                         DOMAIN (CORE)                                 │  │
│   │                                                                       │  │
│   │   ┌──────────────┐    ┌──────────────┐    ┌──────────────────────┐  │  │
│   │   │    Events    │    │   Services   │    │   Ports (Interfaces) │  │  │
│   │   │  (Models)    │    │  (Use Cases) │    │   - FileStorage      │  │  │
│   │   └──────────────┘    └──────────────┘    │   - MetadataRepo     │  │  │
│   │                                            │   - EventPublisher   │  │  │
│   │                                            │   - CryptoProvider   │  │  │
│   │                                            └──────────────────────┘  │  │
│   └────────────┬─────────────────────────────────────────────────────────┘  │
│                │                                                             │
│   ┌────────────▼─────────────────────────────────────────────────────────┐  │
│   │                         ADAPTERS (OUT)                                │  │
│   │   ┌────────────┐  ┌──────────────┐  ┌───────────┐  ┌──────────────┐ │  │
│   │   │ S3 Client  │  │ DynamoDB Repo│  │ SNS Client│  │ KMS Crypto   │ │  │
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
│       ├── main.py                 # Application entry point
│       ├── config.py               # Pydantic settings
│       ├── domain/
│       │   ├── __init__.py
│       │   ├── events.py           # Event models (Pydantic)
│       │   ├── models.py           # Domain models
│       │   └── exceptions.py       # Domain exceptions
│       ├── ports/
│       │   ├── __init__.py
│       │   ├── inbound.py          # Input ports (interfaces)
│       │   └── outbound.py         # Output ports (interfaces)
│       ├── adapters/
│       │   ├── __init__.py
│       │   ├── inbound/
│       │   │   ├── __init__.py
│       │   │   └── sqs_consumer.py # SQS message consumer
│       │   └── outbound/
│       │       ├── __init__.py
│       │       ├── s3_storage.py   # S3 file storage adapter
│       │       ├── dynamodb_repo.py # DynamoDB metadata repository
│       │       ├── sns_publisher.py # SNS event publisher
│       │       └── kms_crypto.py   # KMS crypto provider
│       ├── application/
│       │   ├── __init__.py
│       │   └── file_processor.py   # Application service
│       └── infrastructure/
│           ├── __init__.py
│           ├── logging.py          # Structured logging config
│           └── aws_clients.py      # AWS client factory
├── tests/
│   ├── __init__.py
│   ├── conftest.py                 # Pytest fixtures
│   ├── unit/
│   │   └── ...
│   └── integration/
│       └── ...
├── Dockerfile
├── docker-compose.yml
├── pyproject.toml
├── requirements.txt
└── README.md
```

## 🚀 Quick Start

### Prerequisites

- Python 3.11+
- Docker & Docker Compose
- LocalStack (for local development)

### Local Development

```bash
# 1. Create virtual environment
python -m venv .venv
source .venv/bin/activate

# 2. Install dependencies
pip install -e ".[dev]"

# 3. Start LocalStack (from fsamp-infra)
cd ../fsamp-infra && make up && make apply-local

# 4. Run processor
cd ../fsamp-processor
python -m processor.main
```

### Docker

```bash
# Build image
docker build -t fsamp-processor:latest .

# Run with LocalStack
docker-compose up
```

## 🔒 FIPS 140-3 Compliance

This processor implements FIPS 140-3 compliant cryptography:

- **Encryption**: AES-256-GCM via AWS KMS
- **Key Management**: AWS KMS with automatic key rotation
- **Library**: Python `cryptography` package (OpenSSL FIPS provider)

## ⚙️ Configuration

Environment variables:

| Variable | Description | Default |
|----------|-------------|---------|
| `AWS_REGION` | AWS region | `us-west-2` |
| `AWS_ENDPOINT_URL` | LocalStack endpoint | `None` |
| `SQS_QUEUE_URL` | Processing queue URL | Required |
| `SNS_TOPIC_ARN` | Event topic ARN | Required |
| `S3_BUCKET_NAME` | File storage bucket | Required |
| `DYNAMODB_TABLE_NAME` | Metadata table | Required |
| `KMS_KEY_ID` | Encryption key ID | Required |
| `LOG_LEVEL` | Logging level | `INFO` |

## 🧪 Testing

```bash
# Run all tests
pytest

# Run with coverage
pytest --cov=processor --cov-report=html

# Run only unit tests
pytest -m unit

# Run integration tests (requires LocalStack)
pytest -m integration
```

## 📊 Observability

- **Structured Logging**: JSON logs with correlation IDs
- **Metrics**: CloudWatch metrics for processing stats
- **Tracing**: X-Ray integration (optional)

## 📄 License

MIT
