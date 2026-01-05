# 📚 FSAMP Processor Documentation

## Table of Contents

1. [Introduction](#introduction)
2. [Architecture](#architecture)
3. [Project Structure](#project-structure)
4. [Data Flow](#data-flow)
5. [System Components](#system-components)
6. [Data Models](#data-models)
7. [Deployment Modes](#deployment-modes)
8. [Configuration](#configuration)
9. [Security (FIPS 140-3)](#security-fips-140-3)
10. [Outbox Pattern](#outbox-pattern)
11. [Monitoring & Metrics](#monitoring--metrics)
12. [Running & Deployment](#running--deployment)
13. [Testing](#testing)

---

## Introduction

**FSAMP Processor** is an event-driven file processor for the FSAMP (File Secure Access Management Platform). The system handles secure file processing with full **FIPS 140-3** compliance (US Federal cryptographic security standard).

### Key Features:
- 🔒 **FIPS 140-3 Compliance** - AES-256-GCM encryption, keys managed by AWS KMS
- ⚡ **Dual Deployment Modes** - AWS Lambda (primary) or ECS Fargate (for large files >10GB)
- 📦 **Hexagonal Architecture** - Clean ports and adapters pattern
- 🔄 **Outbox Pattern** - Guaranteed event delivery
- 📊 **Observability** - AWS Lambda Powertools, X-Ray tracing, CloudWatch metrics

---

## Architecture

### Hexagonal Architecture (Ports & Adapters)

The project implements the **Hexagonal Architecture** (Ports & Adapters) pattern, which provides:
- Isolation of business logic from infrastructure
- Easy component replacement (e.g., S3 → GCS)
- Testability through port mocking

```
┌─────────────────────────────────────────────────────────────────────┐
│                         INBOUND ADAPTERS                            │
│   ┌─────────────────┐    ┌─────────────────┐                        │
│   │ Lambda Handler  │    │  SQS Consumer   │                        │
│   │ (AWS Powertools)│    │  (ECS mode)     │                        │
│   └────────┬────────┘    └────────┬────────┘                        │
└────────────┼──────────────────────┼─────────────────────────────────┘
             │                      │
┌────────────▼──────────────────────▼─────────────────────────────────┐
│                         DOMAIN (CORE)                               │
│   ┌──────────────┐    ┌──────────────┐    ┌──────────────────────┐ │
│   │ FileEvent    │    │FileProcessor │    │   Ports (Interfaces) │ │
│   │ (Pydantic)   │    │  Service     │    │   - FileStorage      │ │
│   └──────────────┘    └──────────────┘    │   - MetadataRepo     │ │
│                                           │   - EventPublisher   │ │
│                                           │   - CryptoProvider   │ │
│                                           └──────────────────────┘ │
└────────────┬────────────────────────────────────────────────────────┘
             │
┌────────────▼────────────────────────────────────────────────────────┐
│                         OUTBOUND ADAPTERS                           │
│   ┌────────────┐  ┌──────────────┐  ┌───────────┐  ┌──────────────┐│
│   │ S3 Storage │  │ DynamoDB Repo│  │ SNS Pub   │  │ KMS Crypto   ││
│   └────────────┘  └──────────────┘  └───────────┘  └──────────────┘│
└─────────────────────────────────────────────────────────────────────┘
```

### High-Level Flow

```
SQS Queue ──▶ Lambda/ECS Consumer ──▶ FileProcessorService
                                               │
                    ┌──────────────────────────┼──────────────────────────┐
                    │                          │                          │
                    ▼                          ▼                          ▼
              📥 S3 Download             🔐 KMS Decrypt            📊 DynamoDB Save
                    │                          │                          │
                    └──────────────────────────┴──────────────────────────┘
                                               │
                                               ▼
                                    📤 SNS Publish (via Outbox)
```

---

## Project Structure

```
fsamp-processor/
├── src/
│   └── processor/
│       ├── __init__.py
│       ├── main.py                    # ECS/Container entry point
│       ├── lambda_handler.py          # AWS Lambda entry point ⚡
│       ├── outbox_publisher.py        # Outbox Publisher Lambda
│       ├── config.py                  # Configuration (Pydantic Settings)
│       │
│       ├── domain/                    # 🎯 DOMAIN LAYER
│       │   ├── events.py              # Event models (FileEvent)
│       │   ├── models.py              # Domain models (MetadataRecord, etc.)
│       │   └── exceptions.py          # Domain exceptions
│       │
│       ├── ports/                     # 🔌 INTERFACES (PORTS)
│       │   ├── inbound.py             # Inbound ports (EventHandler, MessageConsumer)
│       │   └── outbound.py            # Outbound ports (FileStorage, MetadataRepo, etc.)
│       │
│       ├── adapters/                  # 🔧 IMPLEMENTATIONS (ADAPTERS)
│       │   ├── inbound/
│       │   │   └── sqs_consumer.py    # SQS Consumer (ECS mode)
│       │   └── outbound/
│       │       ├── s3_storage.py      # S3 file storage
│       │       ├── dynamodb_repo.py   # Metadata repository
│       │       ├── sns_publisher.py   # SNS event publisher
│       │       ├── kms_crypto.py      # FIPS encryption (KMS + AES-GCM)
│       │       └── outbox_repo.py     # Outbox repository
│       │
│       ├── application/               # 📋 APPLICATION LAYER
│       │   └── file_processor.py      # Main processing service
│       │
│       └── infrastructure/            # 🏗️ INFRASTRUCTURE
│           ├── logging.py             # Logging configuration (structlog)
│           └── aws_clients.py         # AWS client factory
│
├── tests/                             # 🧪 TESTS
│   ├── unit/                          # Unit tests
│   ├── integration/                   # Integration tests (LocalStack)
│   └── contract/                      # Contract tests (event schema)
│
├── docs/                              # 📖 DOCUMENTATION
├── schema/                            # 📋 JSON SCHEMAS
│   └── event.schema.json              # FSAMP events schema v1.0.0
├── events/                            # 📧 SAMPLE EVENTS
│   ├── sample-sqs-event.json
│   └── sample-sns-wrapped-event.json
│
├── template.yaml                      # SAM Template (local dev)
├── Dockerfile                         # ECS image
├── Dockerfile.lambda                  # Lambda image
├── docker-compose.yml                 # Docker Compose (with LocalStack)
└── pyproject.toml                     # Python project configuration
```

---

## Data Flow

### 1. Event Reception

A `FILE_UPLOADED` event arrives via SQS Queue (Lambda trigger or ECS long-polling).

```json
{
  "schemaVersion": "1.0.0",
  "eventId": "123e4567-e89b-12d3-a456-426614174000",
  "correlationId": "abc12345-e89b-12d3-a456-426614174000",
  "timestamp": "2024-01-05T10:30:00Z",
  "source": "fsamp-gateway",
  "eventType": "FILE_UPLOADED",
  "fileMetadata": {
    "originalFilename": "document.pdf",
    "fileSizeBytes": 1048576,
    "mimeType": "application/pdf",
    "checksumSHA256": "a948904f2f0f4779..."
  },
  "storageLocation": {
    "bucketName": "fsamp-files-dev",
    "objectKey": "uploads/2024/01/05/123e4567.pdf"
  },
  "encryptionDetails": {
    "algorithm": "AES_256_GCM",
    "keyId": "arn:aws:kms:us-west-2:123456789:key/..."
  }
}
```

### 2. Processing by FileProcessorService

```python
# Simplified flow in file_processor.py
def _process_uploaded_file(self, event: FileEvent):
    # 1. Download file from S3
    file_content = self._storage.download(bucket, key)

    # 2. Verify integrity (SHA-256)
    self._verify_checksum(file_content, event.file_metadata.checksum_sha256)

    # 3. Optional: decrypt if needed
    decrypted = self._crypto.decrypt(file_content.data)

    # 4. Analyze file (magic bytes, antivirus, etc.)
    analysis = self._analyze_file(decrypted)

    # 5. Save metadata + outbox event (transactionally)
    self._save_with_outbox(metadata_record, outbox_event)

    # 6. Outbox Publisher Lambda will publish event to SNS
    return ProcessingResult(status=COMPLETED)
```

### 3. Event Types

| Event Type | Description | Source |
|------------|-------------|--------|
| `FILE_UPLOADED` | New file uploaded via gateway | fsamp-gateway |
| `FILE_SCANNED` | File passed antivirus scan | fsamp-processor |
| `ANALYSIS_COMPLETED` | File analysis completed | fsamp-processor |
| `PROCESSING_FAILED` | Processing failed | fsamp-processor |

---

## System Components

### 1. Lambda Handler (`lambda_handler.py`)

Main entry point for AWS Lambda. Uses **AWS Lambda Powertools**:

```python
@logger.inject_lambda_context(log_event=True)
@tracer.capture_lambda_handler
@metrics.log_metrics(capture_cold_start_metric=True)
@batch_processor(record_handler=record_handler, processor=processor)
def lambda_handler(event: dict, context: LambdaContext):
    """
    Processes SQS message batch with partial batch response.
    Failed messages automatically return to queue.
    """
```

**Features:**
- Batch processing with SQS (up to 10 messages)
- Partial Batch Response (only failed messages return to queue)
- Cold/warm start optimization (singleton pattern)
- X-Ray tracing
- CloudWatch EMF metrics

### 2. FileProcessorService (`application/file_processor.py`)

Main application service (use case):

```python
class FileProcessorService:
    """
    Orchestrates the workflow:
    1. Event validation
    2. File download from S3
    3. Integrity verification
    4. Content analysis
    5. Save metadata + outbox event
    """

    def handle(self, event: FileEvent) -> ProcessingResult:
        if event.event_type == EventType.FILE_UPLOADED:
            return self._process_uploaded_file(event)
        elif event.event_type == EventType.FILE_SCANNED:
            return self._process_scanned_file(event)
```

### 3. SQS Consumer (`adapters/inbound/sqs_consumer.py`)

Consumer for ECS mode with long-polling:

```python
class SQSConsumer(MessageConsumer):
    """
    - Long-polling (20s wait time)
    - Graceful shutdown (SIGTERM/SIGINT)
    - Automatic acknowledge/reject
    - Retry with exponential backoff
    """
```

### 4. Outbound Adapters

| Adapter | Port | Description |
|---------|------|-------------|
| `S3FileStorage` | `FileStorage` | File download/upload with SSE-KMS |
| `DynamoDBMetadataRepository` | `MetadataRepository` | Single-table design for metadata |
| `SNSEventPublisher` | `EventPublisher` | Event publishing to SNS |
| `KMSCryptoProvider` | `CryptoProvider` | Envelope encryption (KMS + AES-GCM) |
| `DynamoDBOutboxRepository` | `OutboxRepository` | Transactional outbox writes |

---

## Data Models

### FileEvent (Input Event)

```python
class FileEvent(BaseModel):
    schema_version: str = "1.0.0"
    event_id: UUID
    correlation_id: UUID
    timestamp: datetime
    source: EventSource  # fsamp-gateway | fsamp-processor
    event_type: EventType  # FILE_UPLOADED | FILE_SCANNED | ...
    file_metadata: FileMetadata
    storage_location: StorageLocation
    encryption_details: EncryptionDetails
```

### MetadataRecord (DynamoDB Record)

```python
@dataclass
class MetadataRecord:
    file_id: str           # PK: FILE#<uuid>
    timestamp: str         # SK: TS#<iso-timestamp>
    original_filename: str
    file_size_bytes: int
    checksum_sha256: str
    status: ProcessingStatus  # PENDING | COMPLETED | FAILED
    analysis_result: AnalysisResult | None
```

### OutboxEvent (Outbox Event)

```python
@dataclass
class OutboxEvent:
    event_id: str
    event_type: OutboxEventType  # FILE_PROCESSED | FILE_FAILED
    aggregate_id: str            # file_id
    payload: dict                # Event data for SNS
    status: OutboxStatus         # PENDING | PUBLISHED | FAILED
    created_at: str
    ttl: int                     # DynamoDB TTL (auto-cleanup)
```

---

## Deployment Modes

### 1. AWS Lambda (Recommended)

```bash
# Build with SAM
sam build

# Local testing
sam local invoke ProcessorFunction -e events/sample-sqs-event.json --env-vars env.json

# Deploy
./deploy-lambda.sh deploy dev
```

**Advantages:**
- Pay-per-invocation
- Auto-scaling 0 → 1000
- Batch processing with SQS
- Integrated with AWS X-Ray

**Limitations:**
- Max 15 min timeout
- Max 10GB memory
- Not suitable for very large files

### 2. ECS Fargate (For Large Files)

```bash
# Run locally
python -m processor.main

# Or with Docker
docker-compose up
```

**Advantages:**
- Unlimited timeout
- Full resource control
- For files > 10GB

---

## Configuration

### Environment Variables

```bash
# Environment
ENVIRONMENT=dev              # local | dev | staging | prod

# AWS
AWS_REGION=us-west-2
AWS_ENDPOINT_URL=http://localhost:4566  # LocalStack

# AWS Resources
SQS_QUEUE_URL=https://sqs.us-west-2.amazonaws.com/123/fsamp-queue
SNS_TOPIC_ARN=arn:aws:sns:us-west-2:123:fsamp-events
S3_BUCKET_NAME=fsamp-files-dev
DYNAMODB_TABLE_NAME=fsamp-metadata
OUTBOX_TABLE_NAME=fsamp-outbox
KMS_KEY_ID=alias/fsamp-key

# FIPS
USE_FIPS_ENDPOINT=true       # Enforce FIPS endpoints (us-* regions only)

# Processing
MAX_FILE_SIZE_BYTES=104857600  # 100MB
PROCESSING_MAX_RETRIES=3

# Logging
LOG_LEVEL=INFO
LOG_FORMAT=json
```

### Pydantic Settings

```python
class Settings(BaseSettings):
    """
    Auto-loading from .env and environment variables.
    Type validation, default values, computed properties.
    """

    @property
    def should_use_fips(self) -> bool:
        """FIPS only for us-* regions and not for LocalStack."""
        if self.is_local or self.aws_endpoint_url:
            return False
        return self.use_fips_endpoint and self.aws_region.startswith("us-")
```

---

## Security (FIPS 140-3)

### FIPS 140-3 Requirements

FSAMP Processor meets the federal cryptographic security standard requirements:

| Component | Standard | Implementation |
|-----------|----------|----------------|
| Symmetric Encryption | AES-256-GCM | KMS + Python cryptography |
| Key Management | FIPS 140-2 Level 3 | AWS KMS |
| Hashes | SHA-256/384/512 | FIPS 180-4 |
| AWS Endpoints | FIPS endpoints | `use_fips_endpoint=True` |

### Envelope Encryption

```python
class KMSCryptoProvider:
    """
    1. KMS generates Data Encryption Key (DEK)
    2. Data encrypted locally with AES-256-GCM
    3. Encrypted DEK stored alongside ciphertext
    """

    def encrypt(self, plaintext: bytes) -> EncryptedData:
        # Generate DEK via KMS
        plaintext_key, encrypted_key = self.generate_data_key()

        # Encrypt locally with AES-256-GCM
        nonce = os.urandom(12)
        ciphertext = AESGCM(plaintext_key).encrypt(nonce, plaintext, None)

        return EncryptedData(
            ciphertext=ciphertext,
            encrypted_key=encrypted_key,
            nonce=nonce
        )
```

### Prohibited Algorithms

- ❌ MD5, SHA-1
- ❌ DES, 3DES
- ❌ RC4
- ❌ RSA < 2048 bits

---

## Outbox Pattern

### The "Dual Write" Problem

In distributed systems, database writes and message broker publishing can get out of sync:
- Database saved, but SNS publish failed → **Lost event**
- SNS published, but database failed → **Orphaned event**

### Solution

```
┌────────────────────────────────────────────────────────────────┐
│                  FileProcessor Lambda                          │
│                                                                │
│   DynamoDB TransactWriteItems (atomic operation)              │
│   ┌─────────────────┐    ┌─────────────────────────────┐      │
│   │ Metadata Table  │    │       Outbox Table         │      │
│   │ PK: FILE#xxx    │    │ PK: OUTBOX#FileProcessing  │      │
│   │ status: DONE    │    │ status: PENDING            │      │
│   └─────────────────┘    └─────────────────────────────┘      │
└────────────────────────────────────────────────────────────────┘
                                      │
                             DynamoDB Streams
                                      │
                                      ▼
┌────────────────────────────────────────────────────────────────┐
│                   Outbox Publisher Lambda                       │
│                                                                 │
│   1. Receive stream event (INSERT)                             │
│   2. Parse OutboxEvent                                         │
│   3. Publish to SNS                                            │
│   4. Mark as PUBLISHED                                         │
└────────────────────────────────────────────────────────────────┘
                                      │
                                      ▼
                              SNS Topic → Downstream
```

### Guarantees

- ✅ **At-least-once delivery** - events will always arrive
- ✅ **Atomicity** - metadata and outbox saved together
- ✅ **Event replay** - ability to resend from outbox
- ✅ **Audit trail** - history of all published events

---

## Monitoring & Metrics

### AWS Lambda Powertools

| Component | Function |
|-----------|----------|
| **Logger** | Structured JSON logging, correlation IDs |
| **Tracer** | X-Ray distributed tracing |
| **Metrics** | CloudWatch EMF embedded metrics |
| **Batch** | Partial batch response for SQS |

### Custom Metrics (namespace: FSAMP/Processor)

| Metric | Unit | Description |
|--------|------|-------------|
| `FilesProcessed` | Count | All processed files |
| `FilesProcessedSuccess` | Count | Successful processing |
| `FilesProcessedFailed` | Count | Failed processing |
| `ProcessingDuration` | Milliseconds | Processing time |
| `SafeFiles` | Count | Files marked safe |
| `UnsafeFiles` | Count | Files marked unsafe |
| `RetryableErrors` | Count | Retryable errors |
| `NonRetryableErrors` | Count | Non-retryable errors |
| `BatchSize` | Count | SQS batch size |

### CloudWatch Dashboard

Dashboard includes:
1. **Lambda Processor** - invocations, duration, errors, concurrency
2. **Outbox & SQS** - queue depth, DLQ messages
3. **DynamoDB** - read/write capacity, throttles
4. **Custom Metrics** - files processed, duration percentiles
5. **S3 & SNS** - objects, published events
6. **Error Logs** - recent errors from all services
7. **KPIs** - success rate, total files, avg duration

---

## Running & Deployment

### Local with LocalStack

```bash
# 1. Start LocalStack (from fsamp-infra project)
cd ../fsamp-infra && make up && make apply-local

# 2. Build with SAM
cd ../fsamp-processor
sam build

# 3. Invoke locally
sam local invoke ProcessorFunction \
    -e events/sample-sqs-event.json \
    --env-vars env.json
```

### Docker Compose

```bash
# Standalone with LocalStack
docker-compose -f docker-compose.standalone.yml up

# Processor only (requires external LocalStack)
docker-compose up
```

### Lambda Deployment (AWS)

```bash
# Build
./deploy-lambda.sh build

# Deploy to DEV
./deploy-lambda.sh deploy dev

# Deploy to PROD
./deploy-lambda.sh deploy prod
```

---

## Testing

### Test Structure

```
tests/
├── unit/                    # Unit tests (mocked dependencies)
│   ├── test_file_processor.py
│   ├── test_lambda_handler.py
│   ├── test_s3_storage.py
│   ├── test_dynamodb_repo.py
│   ├── test_kms_crypto.py
│   └── ...
├── integration/             # Integration tests (LocalStack)
│   ├── test_dynamodb_repo.py
│   ├── test_s3_storage.py
│   └── test_sqs_consumer_localstack.py
└── contract/                # Contract tests (JSON schema)
    └── test_event_schema_contract.py
```

### Running Tests

```bash
# All tests
pytest

# With coverage
pytest --cov=processor --cov-report=html

# Unit only
pytest -m unit

# Integration only (requires LocalStack)
pytest -m integration

# Specific file
pytest tests/unit/test_file_processor.py -v
```

### Pytest Fixtures (conftest.py)

```python
@pytest.fixture
def mock_file_storage():
    """Mock S3FileStorage for unit tests."""
    storage = MagicMock(spec=FileStorage)
    storage.download.return_value = FileContent(data=b"test", ...)
    return storage

@pytest.fixture
def file_processor_service(mock_file_storage, mock_metadata_repo, ...):
    """Service with mocked dependencies."""
    return FileProcessorService(
        file_storage=mock_file_storage,
        metadata_repo=mock_metadata_repo,
        ...
    )
```

---

## Summary

FSAMP Processor is a production-ready, event-driven file processing system:

- **Architecture**: Hexagonal (Ports & Adapters) with clean layer separation
- **Runtime**: AWS Lambda (primary) + ECS Fargate (backup for large files)
- **Security**: FIPS 140-3 with KMS + AES-256-GCM
- **Reliability**: Outbox Pattern for at-least-once delivery
- **Observability**: Lambda Powertools + CloudWatch + X-Ray
- **Quality**: Complete unit, integration, and contract tests

The project is ready for production deployment on AWS with full automation via SAM/CloudFormation.
