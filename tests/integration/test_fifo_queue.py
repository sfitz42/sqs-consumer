import asyncio
from collections.abc import Sequence

from mypy_boto3_sqs import client as sqs_client

from sqs_consumer import config, errors, message
from tests.support import consumers, sqs
from tests.support import events as hook_events


def send_group(
    client: sqs_client.SQSClient, queue_url: str, group: str, *bodies: str
) -> None:
    for body in bodies:
        client.send_message(QueueUrl=queue_url, MessageBody=body, MessageGroupId=group)


def in_order(messages: Sequence[message.Message]) -> list[str]:
    """Bodies in the order the consumer reported them, unlike Events.bodies."""
    return [m.body for m in messages]


class TestFifoOrdering:
    async def test_a_group_is_handled_in_order_even_when_later_messages_are_faster(
        self, client: sqs_client.SQSClient
    ):
        queue_url = sqs.create_queue(client, fifo=True)
        send_group(client, queue_url, "orders", "0", "1", "2", "3", "4")
        completed: list[str] = []
        groups: set[str | None] = set()

        async def handle(item: message.Message) -> None:
            # Earlier messages take longer, so concurrent handling would finish
            # them in reverse.
            await asyncio.sleep((5 - int(item.body)) * 0.05)
            completed.append(item.body)
            groups.add(item.message_group_id)

        async with consumers.build(queue_url, client=client, handler=handle):
            await sqs.wait_for(lambda: sqs.depth(client, queue_url) == (0, 0))

        assert completed == ["0", "1", "2", "3", "4"]
        assert groups == {"orders"}

    async def test_a_failure_skips_the_rest_of_its_group_and_deletes_none_of_it(
        self, client: sqs_client.SQSClient
    ):
        """Covers the first attempt only: the group stays in flight, untouched.
        Redelivery order after the visibility timeout is covered by
        test_a_group_is_retried_in_order_after_its_visibility_timeout.
        """
        queue_url = sqs.create_queue(client, fifo=True, VisibilityTimeout="60")
        send_group(client, queue_url, "orders", "0", "1", "2")
        attempts: list[str] = []
        recorder = hook_events.Events()

        async def fail_first(item: message.Message) -> None:
            attempts.append(item.body)
            if item.body == "0":
                raise RuntimeError("first message fails")

        async with consumers.build(
            queue_url, client=client, handler=fail_first, hooks=recorder.hooks()
        ):
            await sqs.wait_for(lambda: recorder.failures)

        assert attempts == ["0"]
        assert recorder.processed == []
        [(_, failed)] = recorder.failures
        assert in_order(failed) == ["0", "1", "2"]
        assert sqs.depth(client, queue_url) == (0, 3)

    async def test_a_handler_timeout_skips_the_rest_of_its_group(
        self, client: sqs_client.SQSClient
    ):
        queue_url = sqs.create_queue(client, fifo=True, VisibilityTimeout="60")
        send_group(client, queue_url, "orders", "0", "1", "2")
        attempts: list[str] = []
        recorder = hook_events.Events()

        async def hang_first(item: message.Message) -> None:
            attempts.append(item.body)
            if item.body == "0":
                await asyncio.sleep(10)

        async with consumers.build(
            queue_url,
            client=client,
            handler=hang_first,
            handler_timeout=0.2,
            hooks=recorder.hooks(),
        ):
            await sqs.wait_for(lambda: recorder.failures)

        assert attempts == ["0"]
        [(error, failed)] = recorder.failures
        assert isinstance(error, errors.HandlerTimeoutError)
        assert in_order(failed) == ["0", "1", "2"]
        assert sqs.depth(client, queue_url) == (0, 3)

    async def test_a_shutdown_timeout_mid_group_deletes_none_of_it(
        self, client: sqs_client.SQSClient
    ):
        """Deleting the finished 0 and 1 would be safe for order, but nothing in an
        abandoned batch is deleted, so the whole group comes back with no gap."""
        queue_url = sqs.create_queue(client, fifo=True, VisibilityTimeout="60")
        send_group(client, queue_url, "orders", "0", "1", "2")
        attempts: list[str] = []
        recorder = hook_events.Events()

        async def hang_last(item: message.Message) -> None:
            attempts.append(item.body)
            if item.body == "2":
                await asyncio.Event().wait()

        subject = consumers.build(
            queue_url, client=client, handler=hang_last, hooks=recorder.hooks()
        )
        await subject.start()
        await sqs.wait_for(lambda: "2" in attempts)
        await subject.stop(timeout=0.2)

        assert attempts == ["0", "1", "2"]
        assert recorder.processed == []
        assert sqs.depth(client, queue_url) == (0, 3)

    async def test_release_on_error_returns_the_failed_and_skipped_messages_at_once(
        self, client: sqs_client.SQSClient
    ):
        queue_url = sqs.create_queue(client, fifo=True, VisibilityTimeout="60")
        send_group(client, queue_url, "orders", "0", "1", "2")
        attempts: list[str] = []

        async def fail_first_once(item: message.Message) -> None:
            attempts.append(item.body)
            if attempts == ["0"]:
                raise RuntimeError("first attempt fails")

        async with consumers.build(
            queue_url,
            client=client,
            handler=fail_first_once,
            release_on_error=config.ReleaseOnError(),
        ):
            # Far below the 60s visibility timeout: only a release gets here.
            await sqs.wait_for(
                lambda: sqs.depth(client, queue_url) == (0, 0), timeout=10
            )

        assert attempts == ["0", "0", "1", "2"]

    async def test_a_group_is_retried_in_order_after_its_visibility_timeout(
        self, client: sqs_client.SQSClient
    ):
        queue_url = sqs.create_queue(client, fifo=True, VisibilityTimeout="1")
        send_group(client, queue_url, "orders", "0", "1", "2")
        attempts: list[str] = []

        async def fail_first_once(item: message.Message) -> None:
            attempts.append(item.body)
            if attempts == ["0"]:
                raise RuntimeError("first attempt fails")

        async with consumers.build(queue_url, client=client, handler=fail_first_once):
            await sqs.wait_for(
                lambda: sqs.depth(client, queue_url) == (0, 0), timeout=15
            )

        assert attempts == ["0", "0", "1", "2"]


class TestFifoHeartbeat:
    async def test_a_slow_group_stays_hidden_until_it_is_finished(
        self, client: sqs_client.SQSClient
    ):
        queue_url = sqs.create_queue(client, fifo=True, VisibilityTimeout="2")
        send_group(client, queue_url, "orders", "0", "1", "2")
        completed: list[str] = []
        peeked: list[bool] = []

        async def slow(item: message.Message) -> None:
            # The group takes 3.6s in all, well past the 2s visibility timeout.
            await asyncio.sleep(1.2)
            if item.body == "2":
                peeked.append("Messages" in client.receive_message(QueueUrl=queue_url))
            completed.append(item.body)

        async with consumers.build(
            queue_url,
            client=client,
            handler=slow,
            heartbeat=config.Heartbeat(interval=0.5, visibility_timeout=3),
        ):
            await sqs.wait_for(
                lambda: sqs.depth(client, queue_url) == (0, 0), timeout=15
            )

        assert completed == ["0", "1", "2"]
        assert peeked == [False]

    async def test_without_a_heartbeat_a_slow_group_is_redelivered_mid_way(
        self, client: sqs_client.SQSClient
    ):
        """Why FIFO consumers with slow handlers need a heartbeat: another
        consumer could receive the group while this one is still handling it."""
        queue_url = sqs.create_queue(client, fifo=True, VisibilityTimeout="2")
        send_group(client, queue_url, "orders", "0", "1", "2")
        peeked: list[bool] = []

        async def slow(item: message.Message) -> None:
            await asyncio.sleep(1.2)
            if item.body == "2":
                peeked.append("Messages" in client.receive_message(QueueUrl=queue_url))

        async with consumers.build(queue_url, client=client, handler=slow):
            await sqs.wait_for(lambda: peeked, timeout=15)

        assert peeked == [True]


class TestFifoGroups:
    async def test_different_groups_are_handled_concurrently(
        self, client: sqs_client.SQSClient
    ):
        queue_url = sqs.create_queue(client, fifo=True)
        send_group(client, queue_url, "a", "from-a")
        send_group(client, queue_url, "b", "from-b")
        b_started = asyncio.Event()
        recorder = hook_events.Events()

        async def handle(item: message.Message) -> None:
            if item.message_group_id == "b":
                b_started.set()
                return
            # Only finishes if group b is being handled at the same time.
            await asyncio.wait_for(b_started.wait(), timeout=2)

        async with consumers.build(
            queue_url, client=client, handler=handle, hooks=recorder.hooks()
        ):
            await sqs.wait_for(lambda: len(recorder.processed) == 2)

        assert recorder.failures == []
        assert recorder.bodies(recorder.processed) == ["from-a", "from-b"]

    async def test_a_failure_in_one_group_does_not_hold_back_another(
        self, client: sqs_client.SQSClient
    ):
        queue_url = sqs.create_queue(client, fifo=True, VisibilityTimeout="60")
        send_group(client, queue_url, "broken", "broken-0", "broken-1")
        send_group(client, queue_url, "healthy", "healthy-0", "healthy-1")
        recorder = hook_events.Events()

        async def handle(item: message.Message) -> None:
            if item.message_group_id == "broken":
                raise RuntimeError("group is broken")

        async with consumers.build(
            queue_url, client=client, handler=handle, hooks=recorder.hooks()
        ):
            await sqs.wait_for(lambda: len(recorder.processed) == 2)

        assert recorder.bodies(recorder.processed) == ["healthy-0", "healthy-1"]
        [(_, failed)] = recorder.failures
        assert in_order(failed) == ["broken-0", "broken-1"]
        assert sqs.depth(client, queue_url) == (0, 2)

    async def test_interleaved_groups_each_keep_their_own_order(
        self, client: sqs_client.SQSClient
    ):
        queue_url = sqs.create_queue(client, fifo=True, VisibilityTimeout="60")
        for i in range(3):
            send_group(client, queue_url, "a", f"a{i}")
            send_group(client, queue_url, "b", f"b{i}")
        handled: dict[str, list[str]] = {"a": [], "b": []}
        recorder = hook_events.Events()

        async def fail_a1(item: message.Message) -> None:
            assert item.message_group_id is not None
            handled[item.message_group_id].append(item.body)
            if item.body == "a1":
                raise RuntimeError("a1 fails")

        async with consumers.build(
            queue_url, client=client, handler=fail_a1, hooks=recorder.hooks()
        ):
            await sqs.wait_for(lambda: len(recorder.processed) == 4)

        assert handled == {"a": ["a0", "a1"], "b": ["b0", "b1", "b2"]}
        assert recorder.bodies(recorder.processed) == ["a0", "b0", "b1", "b2"]
        [(_, failed)] = recorder.failures
        assert in_order(failed) == ["a1", "a2"]
        assert sqs.depth(client, queue_url) == (0, 2)


class TestFifoBatchHandler:
    async def test_the_batch_arrives_in_group_order(self, client: sqs_client.SQSClient):
        queue_url = sqs.create_queue(client, fifo=True)
        send_group(client, queue_url, "orders", "0", "1", "2")
        batches: list[list[str]] = []

        async def handle(items):
            batches.append([item.body for item in items])

        subject = consumers.build_batch(queue_url, handle, client=client)
        async with subject:
            await sqs.wait_for(lambda: sqs.depth(client, queue_url) == (0, 0))

        assert [body for batch in batches for body in batch] == ["0", "1", "2"]
