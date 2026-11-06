import collections

from mypy_boto3_sqs import client as sqs_client

from sqs_consumer import errors, message
from tests.support import consumers, sqs


class TestBatchHandler:
    async def test_only_the_messages_a_batch_failure_names_are_kept(
        self, client: sqs_client.SQSClient
    ):
        queue_url = sqs.create_queue(client, VisibilityTimeout="1")
        consumers.send(client, queue_url, "a", "b", "c")
        deliveries: collections.Counter[str] = collections.Counter()

        async def handle_most(items):
            for item in items:
                deliveries[item.body] += 1
            failed = {item: ValueError(item.body) for item in items if item.body == "c"}
            if failed:
                raise errors.BatchFailureError(failed)

        subject = consumers.build_batch(queue_url, handle_most, client=client)
        async with subject:
            await sqs.wait_for(lambda: deliveries["c"] >= 2)

        assert deliveries["a"] == 1
        assert deliveries["b"] == 1

    async def test_returning_nothing_acks_the_whole_batch(
        self, client: sqs_client.SQSClient, queue_url: str
    ):
        consumers.send(client, queue_url, "a", "b")
        seen: list[message.Message] = []

        async def handle_all(items):
            seen.extend(items)

        subject = consumers.build_batch(queue_url, handle_all, client=client)
        async with subject:
            await sqs.wait_for(lambda: sqs.depth(client, queue_url) == (0, 0))

        assert sorted(m.body for m in seen) == ["a", "b"]
