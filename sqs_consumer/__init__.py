"""Build Amazon SQS-based applications without the boilerplate.

An intentional exception to the AICE namespace-package rule: the public API is
re-exported here so users can import it from the package itself. Internal code
still imports modules.
"""

from sqs_consumer.config import Backoff, Heartbeat, Receive, ReleaseOnError
from sqs_consumer.consumer import BatchConsumer, Consumer, ConsumerOptions
from sqs_consumer.errors import (
    BatchFailureError,
    ConfigurationError,
    ConsumerError,
    DeserializationError,
    HandlerTimeoutError,
    MissingPermissionError,
    PartialFailureError,
    RetryAfterError,
)
from sqs_consumer.hooks import Hooks
from sqs_consumer.message import Message
from sqs_consumer.sns import SnsNotification

__all__ = [
    "Backoff",
    "BatchConsumer",
    "BatchFailureError",
    "ConfigurationError",
    "Consumer",
    "ConsumerError",
    "ConsumerOptions",
    "DeserializationError",
    "HandlerTimeoutError",
    "Heartbeat",
    "Hooks",
    "Message",
    "MissingPermissionError",
    "PartialFailureError",
    "Receive",
    "ReleaseOnError",
    "RetryAfterError",
    "SnsNotification",
]
