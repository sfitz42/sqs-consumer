"""Collects hook calls and logged errors so tests can assert what was reported."""

import logging
from collections.abc import Sequence

from sqs_consumer import hooks, message

LOGGER = logging.getLogger("sqs_consumer")


class _ErrorCollector(logging.Handler):
    """Keeps the exceptions the consumer reports (logged as warnings)."""

    def __init__(self, errors: list[Exception]) -> None:
        super().__init__(level=logging.WARNING)
        self._errors = errors

    def emit(self, record: logging.LogRecord) -> None:
        if record.levelno == logging.WARNING and record.exc_info:
            error = record.exc_info[1]
            if isinstance(error, Exception):
                self._errors.append(error)


class Events:
    def __init__(self) -> None:
        self.errors: list[Exception] = []
        self.received: list[message.Message] = []
        self.processed: list[message.Message] = []
        self.failures: list[tuple[Exception, Sequence[message.Message]]] = []
        self.empty = 0
        self.stopped = 0
        LOGGER.addHandler(_ErrorCollector(self.errors))

    def hooks(self) -> hooks.Hooks:
        return hooks.Hooks(
            on_message_received=self._received,
            on_message_processed=self._processed,
            on_processing_error=self._failed,
            on_empty=self._empty,
            on_stopped=self._stopped,
        )

    async def _received(self, item: message.Message) -> None:
        self.received.append(item)

    async def _processed(self, item: message.Message) -> None:
        self.processed.append(item)

    async def _failed(self, exc: Exception, items: Sequence[message.Message]) -> None:
        self.failures.append((exc, items))

    async def _empty(self) -> None:
        self.empty += 1

    async def _stopped(self) -> None:
        self.stopped += 1

    def bodies(self, messages: Sequence[message.Message]) -> list[str]:
        return sorted(m.body for m in messages)


def detach_collectors() -> None:
    for handler in list(LOGGER.handlers):
        if isinstance(handler, _ErrorCollector):
            LOGGER.removeHandler(handler)
