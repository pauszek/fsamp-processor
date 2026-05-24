"""Inbound adapters for receiving external requests."""

from processor.adapters.inbound.sqs_consumer import SQSConsumer

__all__ = ["SQSConsumer"]
