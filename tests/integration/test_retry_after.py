import time

from mypy_boto3_sqs import client as sqs_client

from sqs_consumer import errors, message
from tests.support import consumers, sqs


class TestRetryAfter:
    async def test_a_message_comes_back_after_the_delay_its_handler_chose(
        self, client: sqs_client.SQSClient
    ):
        queue_url = sqs.create_queue(client, VisibilityTimeout="60")
        consumers.send(client, queue_url, "rate-limited")
        attempts: list[float] = []

        async def retry_once(item: message.Message) -> None:
            attempts.append(time.monotonic())
            if len(attempts) == 1:
                raise errors.RetryAfterError(1)

        async with consumers.build(queue_url, client=client, handler=retry_once):
            # Far below the 60s visibility timeout: only the chosen delay gets here.
            await sqs.wait_for(lambda: sqs.depth(client, queue_url) == (0, 0))

        first, second = attempts
        assert second - first >= 0.9
