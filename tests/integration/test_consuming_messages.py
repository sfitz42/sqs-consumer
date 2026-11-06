from mypy_boto3_sqs import client as sqs_client

from sqs_consumer import config, message
from tests.support import consumers, sqs
from tests.support import events as hook_events


class TestSuccessfulHandling:
    async def test_a_handled_message_is_deleted_from_the_queue(
        self, client: sqs_client.SQSClient, queue_url: str
    ):
        consumers.send(client, queue_url, "hello")
        seen: list[message.Message] = []

        async def handle(item: message.Message) -> None:
            seen.append(item)

        async with consumers.build(queue_url, client=client, handler=handle):
            await sqs.wait_for(lambda: sqs.depth(client, queue_url) == (0, 0))

        assert [m.body for m in seen] == ["hello"]
        assert seen[0].message_id
        assert seen[0].receipt_handle

    async def test_requested_message_attributes_reach_the_handler(
        self, client: sqs_client.SQSClient, queue_url: str
    ):
        client.send_message(
            QueueUrl=queue_url,
            MessageBody="with-attributes",
            MessageAttributes={"kind": {"DataType": "String", "StringValue": "demo"}},
        )
        seen: list[message.Message] = []

        async def handle(item: message.Message) -> None:
            seen.append(item)

        async with consumers.build(
            queue_url,
            client=client,
            handler=handle,
            receive=config.Receive(
                wait_time_seconds=1,
                message_attribute_names=["All"],
                attribute_names=["All"],
            ),
        ):
            await sqs.wait_for(lambda: seen)

        assert seen[0].message_attributes["kind"]["StringValue"] == "demo"
        assert "SentTimestamp" in seen[0].attributes


class TestFailedHandling:
    async def test_a_failed_message_is_delivered_again_after_its_visibility_timeout(
        self, client: sqs_client.SQSClient
    ):
        queue_url = sqs.create_queue(client, VisibilityTimeout="1")
        consumers.send(client, queue_url, "flaky")
        deliveries: list[str] = []
        events = hook_events.Events()

        async def fail_once(item: message.Message) -> None:
            deliveries.append(item.message_id)
            if len(deliveries) == 1:
                raise RuntimeError("first attempt fails")

        async with consumers.build(
            queue_url, client=client, handler=fail_once, hooks=events.hooks()
        ):
            await sqs.wait_for(lambda: sqs.depth(client, queue_url) == (0, 0))

        assert len(deliveries) == 2
        assert deliveries[0] == deliveries[1]
        assert len(events.failures) == 1
        assert len(events.processed) == 1
