# FSAMP Processor Notes

This document keeps processor-specific details that do not belong in the platform-level infra docs.

## Runtime Modes

| Mode | Entry point | Use |
|---|---|---|
| Lambda | `processor.lambda_handler.lambda_handler` | Primary AWS runtime, SQS event source mapping |
| ECS/container | `python -m processor.main` | Long-running SQS consumer for larger files or experiments |
| Outbox publisher | `processor.outbox_publisher.lambda_handler` | Publishes pending outbox events to SNS |

Deployment is owned by `fsamp-infra`. This repo no longer carries SAM or manual Lambda deployment scripts.

## Event Flow

1. Gateway publishes `FILE_UPLOADED` to SNS.
2. SQS delivers the message to the processor.
3. Processor reads metadata and object pointers from the event.
4. Processor downloads the object from S3 and validates content/checksum.
5. Metadata is written to DynamoDB.
6. Follow-up events are published directly or through the transactional outbox.

## Ports And Adapters

| Layer | Main files |
|---|---|
| Inbound | `lambda_handler.py`, `adapters/inbound/sqs_consumer.py` |
| Application | `application/file_processor.py` |
| Domain | `domain/events.py`, `domain/models.py`, `domain/exceptions.py` |
| Outbound | `adapters/outbound/s3_storage.py`, `dynamodb_repo.py`, `outbox_repo.py`, `sns_publisher.py`, `kms_crypto.py` |
| Infrastructure | `config.py`, `infrastructure/aws_clients.py`, `infrastructure/fips.py`, `infrastructure/logging.py` |

## Security Posture

- Customer-managed AWS KMS keys are required for production encryption paths.
- AES-GCM and SHA-256/384/512 are the accepted application-level algorithms.
- `USE_FIPS_ENDPOINT=true` enables AWS FIPS endpoints where the selected region supports them.
- `FIPS_REQUIRED=true` fails startup when the expected FIPS runtime checks cannot pass.
- LocalStack runs with FIPS checks relaxed because it does not expose equivalent endpoints.

## Local Commands

```bash
python3.14 -m venv .venv
source .venv/bin/activate
pip install -e ".[dev,test]"
pytest
python -m processor.main
```

Use `fsamp-infra` for LocalStack:

```bash
cd ../fsamp-infra
make up
make apply-local
```

## Operational Notes

- Lambda uses partial batch response so failed SQS records can be retried independently.
- ECS mode uses long polling and the same application service as Lambda.
- Outbox records keep publish state in DynamoDB so metadata writes and event publication can be reconciled.
- Rollback is handled at the platform deploy layer by redeploying a previous immutable image tag.
