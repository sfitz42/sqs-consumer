from mypy_boto3_sqs import client as sqs_client

from sqs_consumer import config, message
from tests.support import consumers, sqs
from tests.support import events as hook_events


async def fail(item: message.Message) -> None:
    raise RuntimeError("nope")


class TestReleaseOnError:
    async def test_a_failed_message_is_visible_again_immediately(
        self, client: sqs_client.SQSClient
    ):
        queue_url = sqs.create_queue(client, VisibilityTimeout="60")
        consumers.send(client, queue_url, "retry-me")
        events = hook_events.Events()

        async with consumers.build(
            queue_url,
            client=client,
            handler=fail,
            hooks=events.hooks(),
            release_on_error=config.ReleaseOnError(),
        ):
            await sqs.wait_for(lambda: len(events.failures) >= 2)

        assert len(events.failures) >= 2

    async def test_without_it_a_failed_message_stays_hidden_for_the_visibility_timeout(
        self, client: sqs_client.SQSClient
    ):
        queue_url = sqs.create_queue(client, VisibilityTimeout="60")
        consumers.send(client, queue_url, "wait-it-out")
        events = hook_events.Events()

        async with consumers.build(
            queue_url, client=client, handler=fail, hooks=events.hooks()
        ):
            await sqs.wait_for(lambda: events.failures)

        assert len(events.failures) == 1
        assert sqs.depth(client, queue_url) == (0, 1)
