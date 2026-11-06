"""Optional async callbacks, e.g. for metrics. Errors go to logging, not a hook."""

import dataclasses
from collections.abc import Awaitable, Callable, Sequence
from typing import Any

from sqs_consumer import message

type MessageHook = Callable[[message.Message[Any]], Awaitable[None]]


@dataclasses.dataclass(frozen=True, slots=True)
class Hooks:
    """A hook that raises is logged and ignored; it never stops the consumer.

    Attributes:
        on_message_received: Called with each message before it is handled.
        on_message_processed: Called with each message that was acknowledged.
        on_processing_error: Called with each failure and every message it failed.
        on_empty: Called after a poll that returned no messages.
        on_stopped: Called once when the consumer stops polling.
    """

    on_message_received: MessageHook | None = None
    on_message_processed: MessageHook | None = None
    on_processing_error: (
        Callable[[Exception, Sequence[message.Message[Any]]], Awaitable[None]] | None
    ) = None
    on_empty: Callable[[], Awaitable[None]] | None = None
    on_stopped: Callable[[], Awaitable[None]] | None = None
