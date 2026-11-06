import asyncio

from mypy_boto3_sqs import client as sqs_client

from sqs_consumer import config, message
from tests.support import consumers, sqs
from tests.support import events as hook_events


class TestGracefulShutdown:
    async def test_stop_lets_an_in_flight_handler_finish_and_acks_it(
        self, client: sqs_client.SQSClient, queue_url: str
    ):
        consumers.send(client, queue_url, "in-flight")
        started = asyncio.Event()
        events = hook_events.Events()

        async def slow(item: message.Message) -> None:
            started.set()
            await asyncio.sleep(1)

        subject = consumers.build(
            queue_url, client=client, handler=slow, hooks=events.hooks()
        )
        await subject.start()
        await started.wait()

        await subject.stop()

        assert events.bodies(events.processed) == ["in-flight"]
        assert events.stopped == 1
        assert sqs.depth(client, queue_url) == (0, 0)

    async def test_a_message_received_while_stopping_is_still_processed(
        self, client: sqs_client.SQSClient, queue_url: str
    ):
        events = hook_events.Events()
        subject = consumers.build(
            queue_url,
            client=client,
            receive=config.Receive(wait_time_seconds=3),
            hooks=events.hooks(),
        )
        await subject.start()
        await asyncio.sleep(0.3)
        consumers.send(client, queue_url, "late-arrival")

        await subject.stop()

        assert events.bodies(events.processed) == ["late-arrival"]
        assert sqs.depth(client, queue_url) == (0, 0)
