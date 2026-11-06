"""Long-polling SQS consumer."""

import abc
import asyncio
import concurrent.futures
import contextlib
import dataclasses
import functools
import json
import logging
from collections.abc import Awaitable, Callable, Sequence
from typing import TYPE_CHECKING, Any, Self, TypedDict, Unpack

import boto3
import botocore.config
import botocore.exceptions

from sqs_consumer import config, errors, gateway, heartbeat, message
from sqs_consumer import hooks as consumer_hooks

if TYPE_CHECKING:
    from mypy_boto3_sqs import client as sqs_client

type Logger = logging.Logger | logging.LoggerAdapter[Any]
type Handler[T] = Callable[[message.Message[T]], Awaitable[None]]
type BatchHandler[T] = Callable[[Sequence[message.Message[T]]], Awaitable[None]]

RECEIVE = "sqs:ReceiveMessage"
DELETE = "sqs:DeleteMessage"
CHANGE_VISIBILITY = "sqs:ChangeMessageVisibility"
GET_QUEUE_ATTRIBUTES = "sqs:GetQueueAttributes"

_NO_HANDLER = "no handler registered; pass one or use @consumer.handler"


class ConsumerOptions[T](TypedDict, total=False):
    """Keyword options shared by Consumer and BatchConsumer. Defaults: _Settings."""

    deserializer: Callable[[str], T]
    client: "sqs_client.SQSClient | None"
    receive: config.Receive
    handler_timeout: float | None
    shutdown_timeout: float | None
    delete_on_success: bool
    heartbeat: config.Heartbeat | None
    release_on_error: config.ReleaseOnError | None
    check_queue: bool
    unwrap_sns: bool
    backoff: config.Backoff
    hooks: consumer_hooks.Hooks
    logger: Logger
    io_threads: int


@dataclasses.dataclass(frozen=True, slots=True, kw_only=True)
class _Settings[T]:
    """ConsumerOptions with their defaults, checked once at construction."""

    deserializer: Callable[[str], T] = json.loads
    client: "sqs_client.SQSClient | None" = None
    receive: config.Receive = dataclasses.field(default_factory=config.Receive)
    handler_timeout: float | None = None
    shutdown_timeout: float | None = None
    delete_on_success: bool = True
    heartbeat: config.Heartbeat | None = None
    release_on_error: config.ReleaseOnError | None = None
    check_queue: bool = False
    unwrap_sns: bool = False
    backoff: config.Backoff = dataclasses.field(default_factory=config.Backoff)
    hooks: consumer_hooks.Hooks = dataclasses.field(
        default_factory=consumer_hooks.Hooks
    )
    # A LoggerAdapter here adds the app's own context and fields to every record.
    logger: Logger = dataclasses.field(
        default_factory=functools.partial(logging.getLogger, __name__)
    )
    io_threads: int = 4

    def __post_init__(self) -> None:
        if self.io_threads < 1:
            raise errors.ConfigurationError("io_threads must be at least 1")

        if self.handler_timeout is not None and self.handler_timeout <= 0:
            raise errors.ConfigurationError("handler_timeout must be greater than 0")

        if self.shutdown_timeout is not None and self.shutdown_timeout < 0:
            raise errors.ConfigurationError("shutdown_timeout must not be negative")

        visibility_timeout = self.receive.visibility_timeout
        if (
            self.heartbeat is not None
            and visibility_timeout is not None
            and self.heartbeat.interval >= visibility_timeout
        ):
            raise errors.ConfigurationError(
                "Heartbeat.interval must be less than the receive visibility_timeout, "
                "otherwise the message becomes visible before the first extension"
            )


@dataclasses.dataclass(slots=True)
class _Run:
    """What exists between one start() and its stop()."""

    pool: concurrent.futures.ThreadPoolExecutor
    queue: gateway.Gateway
    task: asyncio.Task[None] = dataclasses.field(init=False)
    stop_event: asyncio.Event = dataclasses.field(default_factory=asyncio.Event)
    # Cleared when ChangeMessageVisibility is denied, like release_enabled.
    heartbeat_enabled: bool = True
    release_enabled: bool = True


class _BaseConsumer[T](abc.ABC):
    """Poll one queue, hand messages to a handler, delete what succeeded.

    Messages are received in batches and processed one batch at a time. Run more
    consumers for more throughput.

    `message.payload` is the body (or, with unwrap_sns=True, the inner SNS message)
    run through `deserializer`, json.loads by default, on first access.

    Errors are logged to `logger` (the `sqs_consumer` logger by default) and never
    stop the consumer.

    Stopping cannot interrupt a ReceiveMessage that is already in flight (it runs
    in a thread), so it can take up to receive.wait_time_seconds. Messages returned
    by that poll are still processed.

    A client passed in as `client` is shared by the consumer's io_threads threads;
    its connection pool should be at least that large (boto3 defaults to 10).
    """

    def __init__(self, queue_url: str, **options: Unpack[ConsumerOptions[T]]) -> None:
        self._queue_url = queue_url
        self._fifo = queue_url.endswith(".fifo")
        self._settings: _Settings[T] = _Settings(**options)
        self._receive = (
            _with_group_id(self._settings.receive)
            if self._fifo
            else self._settings.receive
        )
        self._active: _Run | None = None

    @property
    def is_running(self) -> bool:
        """Whether the consumer has started and not yet stopped."""
        return self._active is not None and not self._active.task.done()

    def required_iam_actions(self) -> set[str]:
        """Return the IAM actions this configuration uses.

        Handlers that raise errors.RetryAfterError also need
        sqs:ChangeMessageVisibility, which the configuration can't show.

        Returns:
            The actions, e.g. {"sqs:ReceiveMessage", "sqs:DeleteMessage"}.
        """
        settings = self._settings
        actions = {RECEIVE}

        if settings.delete_on_success:
            actions.add(DELETE)

        if settings.heartbeat is not None or settings.release_on_error is not None:
            actions.add(CHANGE_VISIBILITY)

        if settings.check_queue:
            actions.add(GET_QUEUE_ATTRIBUTES)

        return actions

    async def run(self) -> None:
        """Consume until cancelled or stop() is called.

        Cancelling drains in-flight work for up to shutdown_timeout, then re-raises
        the cancellation, so asyncio.run(consumer.run()) shuts down cleanly on Ctrl-C.

        Raises:
            Anything start() or stop() raises.
        """
        await self.start()

        try:
            await self.wait()
        except asyncio.CancelledError:
            await self.stop()
            raise

        await self.stop()

    async def start(self) -> None:
        """Start consuming in a background task. Prefer run() or `async with`.

        Raises:
            RuntimeError: The consumer is already running.
            errors.ConfigurationError: No handler is registered, or check_queue
                found the heartbeat interval too long for the queue.
            errors.MissingPermissionError: check_queue was denied
                sqs:GetQueueAttributes.
        """
        if self.is_running:
            raise RuntimeError("Consumer is already running")

        self._require_handler()
        active = self._open()

        try:
            if self._settings.check_queue:
                await self._check_queue(active.queue)
        except BaseException:
            active.pool.shutdown(wait=False)
            raise

        active.task = asyncio.create_task(self._poll(active))
        active.task.add_done_callback(self._log_crash)
        self._active = active

    async def stop(self, timeout: float | None = None) -> None:
        """Stop polling and let in-flight work finish, for up to `timeout` seconds.

        On timeout the handlers are cancelled and nothing in the batch is deleted.

        Args:
            timeout: Seconds to wait; defaults to shutdown_timeout, and None
                there waits for as long as it takes.

        Raises:
            Whatever crashed the polling task, if it crashed.
        """
        active = self._active
        if active is None:
            return

        if timeout is None:
            timeout = self._settings.shutdown_timeout

        task = active.task
        active.stop_event.set()
        done, _ = await asyncio.wait({task}, timeout=timeout)
        graceful = bool(done)
        if not graceful:
            task.cancel()
            await asyncio.wait({task})

        self._active = None
        active.pool.shutdown(wait=graceful, cancel_futures=not graceful)

        if graceful and not task.cancelled():
            task.result()

    async def wait(self) -> None:
        """Block until the consumer stops."""
        if self._active is not None:
            await asyncio.wait({self._active.task})

    async def __aenter__(self) -> Self:
        await self.start()
        return self

    async def __aexit__(self, *exc_info: object) -> None:
        await self.stop()

    @abc.abstractmethod
    def _require_handler(self) -> object:
        """Return the registered handler.

        Raises:
            errors.ConfigurationError: No handler is registered.
        """

    @abc.abstractmethod
    async def _handle(self, messages: list[message.Message[T]]) -> "_Outcome[T]": ...

    def _open(self) -> _Run:
        io_threads = self._settings.io_threads
        pool = concurrent.futures.ThreadPoolExecutor(
            max_workers=io_threads, thread_name_prefix="sqs-consumer"
        )
        client = self._settings.client or boto3.client(
            "sqs", config=botocore.config.Config(max_pool_connections=io_threads)
        )
        return _Run(pool, gateway.Gateway(client, self._queue_url, pool))

    def _log_crash(self, task: asyncio.Task[None]) -> None:
        if not task.cancelled() and task.exception() is not None:
            self._settings.logger.error(
                "sqs consumer for %s stopped unexpectedly",
                self._queue_url,
                exc_info=task.exception(),
            )

    async def _check_queue(self, queue: gateway.Gateway) -> None:
        try:
            queue_visibility = await queue.queue_visibility_timeout()
        except botocore.exceptions.ClientError as exc:
            if gateway.is_access_denied(exc):
                raise errors.MissingPermissionError(
                    GET_QUEUE_ATTRIBUTES, "check_queue", "set check_queue=False"
                ) from exc

            raise

        beat = self._settings.heartbeat
        if (
            beat is not None
            and self._receive.visibility_timeout is None
            and beat.interval >= queue_visibility
        ):
            raise errors.ConfigurationError(
                f"Heartbeat.interval ({beat.interval}) must be less "
                f"than the queue's default visibility timeout ({queue_visibility})"
            )

    async def _poll(self, active: _Run) -> None:
        settings = self._settings
        failures = 0

        try:
            while not active.stop_event.is_set():
                try:
                    raws = await active.queue.receive(
                        self._receive,
                        orphan_release_timeout=self._orphan_release_timeout(active),
                    )
                    messages = [
                        message.Message.from_sqs(
                            raw,
                            deserializer=settings.deserializer,
                            unwrap_sns=settings.unwrap_sns,
                        )
                        for raw in raws
                    ]
                except Exception as exc:
                    failures += 1
                    await self._report(exc)
                    await self._back_off(active, failures)
                    continue

                failures = 0

                if not messages:
                    await self._emit(settings.hooks.on_empty)
                    continue

                try:
                    await self._process(active, messages)
                except Exception as exc:
                    # A bug here must not stop the consumer; the batch is simply
                    # not acknowledged and SQS redelivers it.
                    await self._report(exc)
        finally:
            await self._emit(settings.hooks.on_stopped)

    async def _back_off(self, active: _Run, failures: int) -> None:
        with contextlib.suppress(TimeoutError):
            async with asyncio.timeout(self._settings.backoff.delay(failures)):
                await active.stop_event.wait()

    def _orphan_release_timeout(self, active: _Run) -> int | None:
        release = self._settings.release_on_error
        if release is None or not active.release_enabled:
            return None

        return release.visibility_timeout

    async def _process(self, active: _Run, messages: list[message.Message[T]]) -> None:
        settings = self._settings

        for received in messages:
            await self._emit(settings.hooks.on_message_received, received)

        beat: asyncio.Task[None] | None = None

        if settings.heartbeat is not None and active.heartbeat_enabled:
            beat = asyncio.create_task(
                heartbeat.run(
                    active.queue,
                    messages,
                    settings.heartbeat,
                    report=self._report,
                    denied=functools.partial(self._heartbeat_denied, active),
                )
            )

        try:
            outcome = await self._handle(messages)
        finally:
            if beat is not None:
                beat.cancel()
                await asyncio.wait({beat})

        acked, failures = outcome
        if settings.delete_on_success and acked:
            await self._delete(active, acked)

        releases = self._releases(active, failures)
        if releases:
            await self._release(active, releases)

        for done in acked:
            await self._emit(settings.hooks.on_message_processed, done)

        for exc, group in failures:
            if isinstance(exc, errors.RetryAfterError):
                self._settings.logger.info(
                    "sqs handler asked to retry %s in %ss",
                    ", ".join(m.message_id for m in group),
                    exc.delay,
                )

            await self._emit(settings.hooks.on_processing_error, exc, group)

    def _timeout_error(self) -> errors.HandlerTimeoutError:
        return errors.HandlerTimeoutError(
            f"handler did not finish within {self._settings.handler_timeout}s"
        )

    async def _delete(self, active: _Run, acked: list[message.Message[T]]) -> None:
        try:
            await active.queue.delete(acked)
        except botocore.exceptions.ClientError as exc:
            if gateway.is_access_denied(exc):
                denied = errors.MissingPermissionError(
                    DELETE, "delete_on_success", "set delete_on_success=False"
                )
                denied.__cause__ = exc
                await self._report(denied)
            else:
                await self._report(exc)
        except (errors.PartialFailureError, botocore.exceptions.BotoCoreError) as exc:
            await self._report(exc)

    def _releases(
        self,
        active: _Run,
        failures: list[tuple[Exception, list[message.Message[T]]]],
    ) -> list[tuple[message.Message[T], int]]:
        """Each unacknowledged message to release, with its visibility timeout.

        A skipped FIFO message is in the same failure as the message that skipped
        it, so the rest of a group comes back with its failed message.
        """
        if not active.release_enabled:
            return []

        releases = []

        for exc, group in failures:
            delay = self._release_delay(exc)
            if delay is not None:
                releases += [(item, delay) for item in group]

        return releases

    def _release_delay(self, exc: Exception) -> int | None:
        """The visibility timeout a failed message is released with; None leaves it.

        The only place this is decided, so a per-message policy (e.g. backoff on
        ApproximateReceiveCount) needs the message passed in and nothing else.
        """
        if isinstance(exc, errors.RetryAfterError):
            return exc.delay

        release = self._settings.release_on_error
        return None if release is None else release.visibility_timeout

    async def _release(
        self, active: _Run, releases: list[tuple[message.Message[T], int]]
    ) -> None:
        try:
            await active.queue.change_visibility_each(releases)
        except botocore.exceptions.ClientError as exc:
            if gateway.is_access_denied(exc):
                active.release_enabled = False

                if self._settings.release_on_error is not None:
                    feature, remedy = "release_on_error", "set release_on_error=None"
                else:
                    feature, remedy = (
                        "RetryAfterError",
                        "stop raising errors.RetryAfterError",
                    )

                denied = errors.MissingPermissionError(
                    CHANGE_VISIBILITY, feature, remedy
                )
                denied.__cause__ = exc
                await self._report(denied)
            else:
                await self._report(exc)
        except (errors.PartialFailureError, botocore.exceptions.BotoCoreError) as exc:
            await self._report(exc)

    async def _heartbeat_denied(self, active: _Run, exc: Exception) -> None:
        active.heartbeat_enabled = False
        denied = errors.MissingPermissionError(
            CHANGE_VISIBILITY, "heartbeat", "set heartbeat=None"
        )
        denied.__cause__ = exc
        await self._report(denied)

    async def _report(self, exc: Exception) -> None:
        self._settings.logger.warning("sqs consumer error: %s", exc, exc_info=exc)

    async def _emit(
        self, hook: Callable[..., Awaitable[None]] | None, *args: object
    ) -> None:
        if hook is None:
            return

        try:
            await hook(*args)
        except Exception:
            self._settings.logger.exception("sqs consumer hook %s raised", hook)


class Consumer[T](_BaseConsumer[T]):
    """Call `handler` once per message, concurrently within a batch.

    Returning deletes the message; raising leaves it on the queue. Register the
    handler here or with the @consumer.handler decorator.

    On a FIFO queue (URL ending .fifo) message groups run concurrently but the
    messages of one group in order. After a failure the rest of that group in the
    batch is skipped, so nothing later in the group is deleted before the failed
    message is retried. Skipped messages are reported to on_processing_error with
    the failed one.
    """

    def __init__(
        self,
        queue_url: str,
        handler: Handler[T] | None = None,
        **options: Unpack[ConsumerOptions[T]],
    ) -> None:
        super().__init__(queue_url, **options)
        self._handler = handler

    def handler(self, fn: Handler[T]) -> Handler[T]:
        """Register `fn` as the handler, for use as a decorator.

        Args:
            fn: The handler.

        Returns:
            `fn`, unchanged.

        Raises:
            errors.ConfigurationError: A handler is already registered.
        """
        if self._handler is not None:
            raise errors.ConfigurationError("a handler is already registered")

        self._handler = fn
        return fn

    def _require_handler(self) -> Handler[T]:
        if self._handler is None:
            raise errors.ConfigurationError(_NO_HANDLER)

        return self._handler

    async def _handle(self, messages: list[message.Message[T]]) -> "_Outcome[T]":
        handler = self._require_handler()
        outcomes: dict[str, Exception | None] = {}
        skipped: dict[str, list[message.Message[T]]] = {}

        async def one(item: message.Message[T]) -> bool:
            try:
                async with asyncio.timeout(self._settings.handler_timeout) as scope:
                    await handler(item)
            except Exception as exc:
                outcomes[item.receipt_handle] = (
                    self._timeout_error() if scope.expired() else exc
                )
                return False

            outcomes[item.receipt_handle] = None
            return True

        async def in_order(group: list[message.Message[T]]) -> None:
            for i, item in enumerate(group):
                if not await one(item):
                    skipped[item.receipt_handle] = group[i + 1 :]
                    return

        async with asyncio.TaskGroup() as tasks:
            if self._fifo:
                for group in _by_group(messages):
                    tasks.create_task(in_order(group))
            else:
                for item in messages:
                    tasks.create_task(one(item))

        acked = [
            m
            for m in messages
            if m.receipt_handle in outcomes and outcomes[m.receipt_handle] is None
        ]
        failures: list[tuple[Exception, list[message.Message[T]]]] = []

        for item in messages:
            failure = outcomes.get(item.receipt_handle)
            if failure is not None:
                # The rest of a FIFO group fails with the message that skipped it.
                rest = skipped.get(item.receipt_handle, [])
                failures.append((failure, [item, *rest]))

        return acked, failures


class BatchConsumer[T](_BaseConsumer[T]):
    """Call `batch_handler` once per received batch, in SQS order.

    Returning acknowledges every message. Raising errors.BatchFailureError fails the
    messages it names and acknowledges the rest; any other exception fails the
    whole batch. On a FIFO queue the handler owns ordering: acknowledging a later
    message of a group while an earlier one fails breaks FIFO order.
    """

    def __init__(
        self,
        queue_url: str,
        batch_handler: BatchHandler[T] | None = None,
        **options: Unpack[ConsumerOptions[T]],
    ) -> None:
        super().__init__(queue_url, **options)
        self._batch_handler = batch_handler

    def handler(self, fn: BatchHandler[T]) -> BatchHandler[T]:
        """Register `fn` as the batch handler, for use as a decorator.

        Args:
            fn: The batch handler.

        Returns:
            `fn`, unchanged.

        Raises:
            errors.ConfigurationError: A handler is already registered.
        """
        if self._batch_handler is not None:
            raise errors.ConfigurationError("a handler is already registered")

        self._batch_handler = fn
        return fn

    def _require_handler(self) -> BatchHandler[T]:
        if self._batch_handler is None:
            raise errors.ConfigurationError(_NO_HANDLER)

        return self._batch_handler

    async def _handle(self, messages: list[message.Message[T]]) -> "_Outcome[T]":
        batch_handler = self._require_handler()

        try:
            async with asyncio.timeout(self._settings.handler_timeout) as scope:
                # Typed as object: handlers written for the old "return what to
                # delete" convention must fail the batch, not ack all of it.
                pending: Awaitable[object] = batch_handler(messages)
                result = await pending

            if result is not None:
                raise TypeError(
                    "a batch handler must return None; raise errors.BatchFailureError "
                    "to fail some of the messages"
                )
        except errors.BatchFailureError as failure:
            return (
                [m for m in messages if m not in failure.failed],
                [(failure.failed[m], [m]) for m in messages if m in failure.failed],
            )
        except Exception as exc:
            error = self._timeout_error() if scope.expired() else exc
            return [], [(error, messages)]

        return messages, []


# (acked, failures) for one batch. Each failure lists every message it failed.
type _Outcome[T] = tuple[
    list[message.Message[T]],
    list[tuple[Exception, list[message.Message[T]]]],
]


def _with_group_id(receive: config.Receive) -> config.Receive:
    names = receive.attribute_names
    if "All" in names or "MessageGroupId" in names:
        return receive

    return dataclasses.replace(receive, attribute_names=[*names, "MessageGroupId"])


def _by_group[M: message.Message[Any]](messages: list[M]) -> list[list[M]]:
    """Split a FIFO batch by message group, keeping SQS's order within each."""
    groups: dict[str | None, list[M]] = {}

    for item in messages:
        groups.setdefault(item.message_group_id, []).append(item)

    return list(groups.values())
