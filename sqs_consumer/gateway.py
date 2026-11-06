"""Bridge between the event loop and the (blocking) boto3 SQS client.

Every call runs on the consumer's own thread pool, in a copy of the caller's
contextvars, so logging and tracing context survive the thread hop and a 20s
long poll never occupies a worker of the event loop's default executor.
"""

import asyncio
import concurrent.futures
import contextlib
import contextvars
import functools
from collections.abc import Callable, Mapping, Sequence
from typing import TYPE_CHECKING, Any

import botocore.exceptions

from sqs_consumer import config, errors, message

if TYPE_CHECKING:
    from mypy_boto3_sqs import client as sqs_client

BATCH_SIZE = 10
ACCESS_DENIED_CODES = frozenset({"AccessDenied", "AccessDeniedException", "403"})


def is_access_denied(exc: BaseException) -> bool:
    """Return whether `exc` is an SQS AccessDenied error."""
    if not isinstance(exc, botocore.exceptions.ClientError):
        return False

    error = exc.response.get("Error", {})
    # Moto answers in XML, which boto3's JSON SQS client reports as code "403"
    # with the body in the message, so the message is checked as well.
    return error.get("Code") in ACCESS_DENIED_CODES or "AccessDenied" in str(
        error.get("Message", "")
    )


def _chunks[T](items: Sequence[T]) -> list[Sequence[T]]:
    return [items[i : i + BATCH_SIZE] for i in range(0, len(items), BATCH_SIZE)]


class Gateway:
    """The SQS calls the consumer makes for one queue, run on its thread pool."""

    def __init__(
        self,
        client: "sqs_client.SQSClient",
        queue_url: str,
        pool: concurrent.futures.ThreadPoolExecutor,
    ) -> None:
        self._client = client
        self._queue_url = queue_url
        self._pool = pool

    async def _run[T](self, fn: Callable[[], T]) -> T:
        future = self._pool.submit(contextvars.copy_context().run, fn)
        return await asyncio.wrap_future(future)

    async def receive(
        self, settings: config.Receive, *, orphan_release_timeout: int | None
    ) -> list[Mapping[str, Any]]:
        """Long-poll the queue, returning the raw ReceiveMessage entries.

        If the awaiting task is cancelled the thread cannot be interrupted. When
        orphan_release_timeout is set, whatever that poll eventually returns is
        released back to the queue instead of waiting out its visibility timeout.
        """
        params: dict[str, Any] = {
            "QueueUrl": self._queue_url,
            "MaxNumberOfMessages": settings.max_messages,
            "WaitTimeSeconds": settings.wait_time_seconds,
        }

        if settings.visibility_timeout is not None:
            params["VisibilityTimeout"] = settings.visibility_timeout

        if settings.attribute_names:
            params["AttributeNames"] = list(settings.attribute_names)

        if settings.message_attribute_names:
            params["MessageAttributeNames"] = list(settings.message_attribute_names)

        future = self._pool.submit(
            contextvars.copy_context().run,
            functools.partial(self._client.receive_message, **params),
        )

        try:
            response = await asyncio.wrap_future(future)
        except asyncio.CancelledError:
            if orphan_release_timeout is not None:
                future.add_done_callback(
                    functools.partial(self._release_orphans, orphan_release_timeout)
                )

            raise

        return list(response.get("Messages", []))

    def _release_orphans(
        self, timeout: int, future: concurrent.futures.Future[Any]
    ) -> None:
        # Runs on the worker thread that finished the poll; best effort only.
        with contextlib.suppress(Exception):
            raws = future.result().get("Messages", [])

            for chunk in _chunks(raws):
                self._client.change_message_visibility_batch(
                    QueueUrl=self._queue_url,
                    Entries=[
                        {
                            "Id": str(i),
                            "ReceiptHandle": raw["ReceiptHandle"],
                            "VisibilityTimeout": timeout,
                        }
                        for i, raw in enumerate(chunk)
                    ],
                )

    async def delete(self, messages: Sequence[message.Message[Any]]) -> None:
        """DeleteMessageBatch in chunks of 10; raises PartialFailureError at the end."""
        failed: list[Mapping[str, Any]] = []

        for chunk in _chunks(messages):
            response = await self._run(
                functools.partial(
                    self._client.delete_message_batch,
                    QueueUrl=self._queue_url,
                    Entries=[
                        {"Id": str(i), "ReceiptHandle": m.receipt_handle}
                        for i, m in enumerate(chunk)
                    ],
                )
            )
            failed.extend(response.get("Failed", []))

        if failed:
            raise errors.PartialFailureError("DeleteMessageBatch", failed)

    async def change_visibility(
        self, messages: Sequence[message.Message[Any]], timeout: int
    ) -> None:
        """Give every message the same timeout; see change_visibility_each."""
        await self.change_visibility_each([(m, timeout) for m in messages])

    async def change_visibility_each(
        self, entries: Sequence[tuple[message.Message[Any], int]]
    ) -> None:
        """ChangeMessageVisibilityBatch in chunks of 10, each with its own timeout.

        Same failure contract as delete.
        """
        failed: list[Mapping[str, Any]] = []

        for chunk in _chunks(entries):
            response = await self._run(
                functools.partial(
                    self._client.change_message_visibility_batch,
                    QueueUrl=self._queue_url,
                    Entries=[
                        {
                            "Id": str(i),
                            "ReceiptHandle": m.receipt_handle,
                            "VisibilityTimeout": timeout,
                        }
                        for i, (m, timeout) in enumerate(chunk)
                    ],
                )
            )
            failed.extend(response.get("Failed", []))

        if failed:
            raise errors.PartialFailureError("ChangeMessageVisibilityBatch", failed)

    async def queue_visibility_timeout(self) -> int:
        """The queue's default visibility timeout. Needs sqs:GetQueueAttributes."""
        response = await self._run(
            functools.partial(
                self._client.get_queue_attributes,
                QueueUrl=self._queue_url,
                AttributeNames=["VisibilityTimeout"],
            )
        )
        return int(response["Attributes"]["VisibilityTimeout"])
