"""Exceptions raised or reported by the consumer."""

from collections.abc import Mapping, Sequence
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from sqs_consumer import message


class ConsumerError(Exception):
    """Base class for every error this library raises or reports."""


class ConfigurationError(ConsumerError, ValueError):
    """The consumer was configured with an invalid or inconsistent value."""


class MissingPermissionError(ConsumerError):
    """An SQS call was denied because the credentials lack an IAM action.

    Attributes:
        action: The denied IAM action, e.g. "sqs:DeleteMessage".
        feature: The consumer feature that needed it.
    """

    def __init__(self, action: str, feature: str, remedy: str) -> None:
        self.action = action
        self.feature = feature
        super().__init__(
            f"AccessDenied for {action} (needed by {feature}); "
            f"grant the action or {remedy}"
        )


class PartialFailureError(ConsumerError):
    """A batch call succeeded overall but some entries came back as Failed.

    Attributes:
        action: The SQS batch action, e.g. "DeleteMessageBatch".
        failed: The Failed entries from the responses.
    """

    def __init__(self, action: str, failed: Sequence[Mapping[str, Any]]) -> None:
        self.action = action
        self.failed = list(failed)
        codes = ", ".join(sorted({str(f.get("Code")) for f in self.failed}))
        super().__init__(f"{action} failed for {len(self.failed)} entries ({codes})")


class HandlerTimeoutError(ConsumerError, TimeoutError):
    """The handler did not finish within handler_timeout."""


class DeserializationError(ConsumerError):
    """The deserializer could not turn a message's payload into a value.

    Attributes:
        message_id: The SQS ID of the message.
    """

    def __init__(self, message_id: str) -> None:
        self.message_id = message_id
        super().__init__(f"could not deserialize the payload of message {message_id}")


class BatchFailureError(ConsumerError):
    """Raised by a batch handler to fail some messages and acknowledge the rest.

    Messages in the batch but not in `failed` are acknowledged.

    Args:
        failed: Each failed message mapped to the exception that failed it.

    Attributes:
        failed: A copy of the `failed` argument.
    """

    def __init__(self, failed: Mapping["message.Message[Any]", Exception]) -> None:
        self.failed = dict(failed)
        super().__init__(f"{len(self.failed)} messages in the batch failed")


class RetryAfterError(ConsumerError):
    """Raised by a handler to fail the message and retry it in `delay` seconds.

    Sets the message's visibility timeout to `delay`, with or without
    release_on_error, so it needs sqs:ChangeMessageVisibility. Raised by a batch
    handler it applies to the whole batch; in a BatchFailureError, to the message
    it is mapped to. Chain the cause with `raise RetryAfterError(...) from exc`.

    Args:
        delay: Seconds until the message is retried, 0..43200.

    Attributes:
        delay: The `delay` argument.

    Raises:
        ValueError: `delay` is out of range.
    """

    def __init__(self, delay: int) -> None:
        # SQS's visibility cap, config.MAX_VISIBILITY_TIMEOUT (config imports this
        # module, so it can't be imported here).
        if not 0 <= delay <= 43200:
            raise ValueError(f"RetryAfterError delay must be 0..43200, got {delay}")

        self.delay = delay
        super().__init__(f"retry in {delay}s")
