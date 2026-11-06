"""Small config objects. Each opt-in feature is one of these; None disables it."""

import dataclasses
import random
from collections.abc import Sequence

from sqs_consumer import errors

MAX_VISIBILITY_TIMEOUT = 43200


@dataclasses.dataclass(frozen=True, slots=True)
class Receive:
    """How each ReceiveMessage long poll is made.

    Attributes:
        wait_time_seconds: How long each long poll waits for messages, 0..20.
        max_messages: The most messages one poll returns, 1..10.
        visibility_timeout: Overrides the queue's default visibility timeout for
            received messages.
        attribute_names: System attributes to request, e.g. "AWSTraceHeader".
        message_attribute_names: Message attributes to request, or "All".

    Raises:
        errors.ConfigurationError: A value is out of range.
    """

    wait_time_seconds: int = 20
    max_messages: int = 10
    visibility_timeout: int | None = None
    attribute_names: Sequence[str] = ()
    message_attribute_names: Sequence[str] = ()

    def __post_init__(self) -> None:
        if not 0 <= self.wait_time_seconds <= 20:
            raise errors.ConfigurationError("Receive.wait_time_seconds must be 0..20")

        if not 1 <= self.max_messages <= 10:
            raise errors.ConfigurationError("Receive.max_messages must be 1..10")

        if self.visibility_timeout is not None and not (
            0 <= self.visibility_timeout <= MAX_VISIBILITY_TIMEOUT
        ):
            raise errors.ConfigurationError(
                f"Receive.visibility_timeout must be 0..{MAX_VISIBILITY_TIMEOUT}"
            )


@dataclasses.dataclass(frozen=True, slots=True)
class Heartbeat:
    """Extend visibility while a batch runs. Needs sqs:ChangeMessageVisibility.

    Attributes:
        interval: Seconds between extensions; less than visibility_timeout.
        visibility_timeout: The visibility timeout each extension sets.

    Raises:
        errors.ConfigurationError: A value is out of range.
    """

    interval: float
    visibility_timeout: int

    def __post_init__(self) -> None:
        if not 0 < self.interval < self.visibility_timeout:
            raise errors.ConfigurationError(
                "Heartbeat.interval must be greater than 0 and less than "
                f"visibility_timeout (got {self.interval} and "
                f"{self.visibility_timeout})"
            )

        if self.visibility_timeout > MAX_VISIBILITY_TIMEOUT:
            raise errors.ConfigurationError(
                f"Heartbeat.visibility_timeout must be <= {MAX_VISIBILITY_TIMEOUT}"
            )


@dataclasses.dataclass(frozen=True, slots=True)
class ReleaseOnError:
    """Reset visibility of failed messages. Needs sqs:ChangeMessageVisibility.

    Attributes:
        visibility_timeout: The visibility timeout a failed message gets; 0 makes
            it visible again straight away.

    Raises:
        errors.ConfigurationError: A value is out of range.
    """

    visibility_timeout: int = 0

    def __post_init__(self) -> None:
        if not 0 <= self.visibility_timeout <= MAX_VISIBILITY_TIMEOUT:
            raise errors.ConfigurationError(
                f"ReleaseOnError.visibility_timeout must be 0..{MAX_VISIBILITY_TIMEOUT}"
            )


@dataclasses.dataclass(frozen=True, slots=True)
class Backoff:
    """Delay between polls after consecutive ReceiveMessage failures.

    Attributes:
        initial: Seconds to wait after the first failure.
        maximum: The longest wait, however many failures.
        factor: What each further failure multiplies the wait by.
        jitter: Wait a random time between 0 and the computed delay.

    Raises:
        errors.ConfigurationError: A value is out of range.
    """

    initial: float = 1.0
    maximum: float = 60.0
    factor: float = 2.0
    jitter: bool = True

    def __post_init__(self) -> None:
        if self.initial <= 0 or self.maximum < self.initial or self.factor < 1:
            raise errors.ConfigurationError(
                "Backoff needs initial > 0, maximum >= initial and factor >= 1"
            )

    def delay(self, failures: int) -> float:
        """Return the seconds to wait after consecutive failures.

        Args:
            failures: How many polls in a row have failed, at least 1.

        Returns:
            The delay in seconds, randomised if jitter is on.
        """
        ceiling = min(self.maximum, self.initial * self.factor ** (failures - 1))
        return random.uniform(0, ceiling) if self.jitter else ceiling
