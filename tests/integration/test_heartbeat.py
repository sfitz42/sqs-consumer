import asyncio

from mypy_boto3_sqs import client as sqs_client

from sqs_consumer import config, message
from tests.support import consumers, sqs


class TestHeartbeat:
    async def test_a_slow_message_stays_invisible_past_its_original_timeout(
        self, client: sqs_client.SQSClient
    ):
        queue_url = sqs.create_queue(client, VisibilityTimeout="2")
        consumers.send(client, queue_url, "slow")
        deliveries = 0
        peeked: list[bool] = []

        async def slow(item: message.Message) -> None:
            nonlocal deliveries
            deliveries += 1
            await asyncio.sleep(3.5)
            peeked.append("Messages" in client.receive_message(QueueUrl=queue_url))

        async with consumers.build(
            queue_url,
            client=client,
            handler=slow,
            heartbeat=config.Heartbeat(interval=0.5, visibility_timeout=3),
        ):
            await sqs.wait_for(lambda: sqs.depth(client, queue_url) == (0, 0))

        assert deliveries == 1
        assert peeked == [False]

    async def test_without_a_heartbeat_the_same_slow_message_becomes_visible_again(
        self, client: sqs_client.SQSClient
    ):
        queue_url = sqs.create_queue(client, VisibilityTimeout="2")
        consumers.send(client, queue_url, "slow")
        peeked: list[bool] = []

        async def slow(item: message.Message) -> None:
            await asyncio.sleep(3.5)
            peeked.append("Messages" in client.receive_message(QueueUrl=queue_url))

        async with consumers.build(queue_url, client=client, handler=slow):
            await sqs.wait_for(lambda: peeked, timeout=15)

        assert peeked == [True]
