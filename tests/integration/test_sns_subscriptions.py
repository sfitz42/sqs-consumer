import dataclasses
import json
from typing import Any

from mypy_boto3_sqs import client as sqs_client

from sqs_consumer import config, consumer, errors, message
from tests.support import consumers, sns, sqs
from tests.support import events as hook_events

KIND = {"kind": {"DataType": "String", "StringValue": "created"}}


@dataclasses.dataclass(frozen=True)
class Order:
    order_id: int


def order_from_json(text: str) -> Order:
    return Order(**json.loads(text))


class TestEnvelopeDelivery:
    async def test_the_handler_gets_the_published_payload_and_the_sns_metadata(
        self, client: sqs_client.SQSClient, sns_client: Any, queue_url: str
    ):
        topic_arn = sns.subscribe_new_topic(
            sns_client, client, queue_url, raw_delivery=False
        )
        sns_client.publish(
            TopicArn=topic_arn,
            Message='{"order_id": 42}',
            Subject="Order created",
            MessageAttributes=KIND,
        )
        seen: list[message.Message] = []

        async def handle(item: message.Message) -> None:
            seen.append(item)

        async with consumers.build(
            queue_url, client=client, handler=handle, unwrap_sns=True
        ):
            await sqs.wait_for(lambda: sqs.depth(client, queue_url) == (0, 0))

        [item] = seen
        assert item.payload == {"order_id": 42}
        assert item.sns is not None
        assert item.sns.topic_arn == topic_arn
        assert item.sns.subject == "Order created"
        assert item.sns.message_attributes["kind"]["Value"] == "created"

    async def test_without_unwrapping_the_payload_is_the_envelope(
        self, client: sqs_client.SQSClient, sns_client: Any, queue_url: str
    ):
        topic_arn = sns.subscribe_new_topic(
            sns_client, client, queue_url, raw_delivery=False
        )
        sns_client.publish(TopicArn=topic_arn, Message='{"order_id": 42}')
        seen: list[message.Message] = []

        async def handle(item: message.Message) -> None:
            seen.append(item)

        async with consumers.build(queue_url, client=client, handler=handle):
            await sqs.wait_for(lambda: seen)

        assert seen[0].sns is None
        assert seen[0].payload["Message"] == '{"order_id": 42}'


class TestRawDelivery:
    async def test_the_handler_gets_the_same_payload_with_no_envelope(
        self, client: sqs_client.SQSClient, sns_client: Any, queue_url: str
    ):
        topic_arn = sns.subscribe_new_topic(
            sns_client, client, queue_url, raw_delivery=True
        )
        sns_client.publish(
            TopicArn=topic_arn, Message='{"order_id": 42}', MessageAttributes=KIND
        )
        seen: list[message.Message] = []

        async def handle(item: message.Message) -> None:
            seen.append(item)

        async with consumers.build(
            queue_url,
            client=client,
            handler=handle,
            unwrap_sns=True,
            receive=config.Receive(
                wait_time_seconds=1, message_attribute_names=["All"]
            ),
        ):
            await sqs.wait_for(lambda: seen)

        [item] = seen
        assert item.payload == {"order_id": 42}
        assert item.sns is None
        assert item.message_attributes["kind"]["StringValue"] == "created"


class TestMixedSubscriptions:
    async def test_one_consumer_handles_raw_and_enveloped_topics_alike(
        self, client: sqs_client.SQSClient, sns_client: Any, queue_url: str
    ):
        """The team running the consumer may not control RawMessageDelivery, and it
        can differ per subscription, so both must decode to the same payload."""
        raw_topic = sns.subscribe_new_topic(
            sns_client, client, queue_url, raw_delivery=True
        )
        wrapped_topic = sns.subscribe_new_topic(
            sns_client, client, queue_url, raw_delivery=False
        )
        sns_client.publish(TopicArn=raw_topic, Message='{"order_id": 1}')
        sns_client.publish(TopicArn=wrapped_topic, Message='{"order_id": 2}')
        orders: list[Order] = []

        async def handle(item: message.Message[Order]) -> None:
            orders.append(item.payload)

        subject = consumer.Consumer(
            queue_url,
            client=client,
            handler=handle,
            deserializer=order_from_json,
            unwrap_sns=True,
            receive=consumers.QUICK_POLL,
        )
        async with subject:
            await sqs.wait_for(lambda: len(orders) == 2)

        assert sorted(orders, key=lambda o: o.order_id) == [Order(1), Order(2)]


class TestUndecodablePayloads:
    async def test_a_payload_the_deserializer_rejects_is_left_on_the_queue(
        self, client: sqs_client.SQSClient, sns_client: Any, queue_url: str
    ):
        topic_arn = sns.subscribe_new_topic(
            sns_client, client, queue_url, raw_delivery=False
        )
        sns_client.publish(TopicArn=topic_arn, Message="not json")
        recorder = hook_events.Events()

        async def handle(item: message.Message) -> None:
            _ = item.payload

        async with consumers.build(
            queue_url,
            client=client,
            handler=handle,
            hooks=recorder.hooks(),
            unwrap_sns=True,
        ):
            await sqs.wait_for(lambda: recorder.failures)

        [(error, _)] = recorder.failures
        assert isinstance(error, errors.DeserializationError)
        assert sqs.depth(client, queue_url) == (0, 1)


class TestFifoTopics:
    async def test_a_fifo_topic_keeps_its_group_through_the_envelope(
        self, client: sqs_client.SQSClient, sns_client: Any
    ):
        queue_url = sqs.create_queue(client, fifo=True)
        topic_arn = sns.subscribe_new_topic(
            sns_client, client, queue_url, raw_delivery=False, fifo=True
        )
        for number in range(3):
            sns_client.publish(
                TopicArn=topic_arn,
                Message=json.dumps({"order_id": number}),
                MessageGroupId="orders",
            )
        seen: list[tuple[str | None, Any]] = []

        async def handle(item: message.Message) -> None:
            seen.append((item.message_group_id, item.payload["order_id"]))

        async with consumers.build(
            queue_url, client=client, handler=handle, unwrap_sns=True
        ):
            await sqs.wait_for(lambda: len(seen) == 3)

        assert seen == [("orders", 0), ("orders", 1), ("orders", 2)]
