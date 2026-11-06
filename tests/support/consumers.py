from typing import Any

from mypy_boto3_sqs import client as sqs_client

from sqs_consumer import config, consumer, message

QUICK_POLL = config.Receive(wait_time_seconds=1)


async def accept(item: message.Message) -> None:
    return None


def build(queue_url: str, **overrides: Any) -> consumer.Consumer:
    """A consumer with a 1s long poll, which keeps stop() quick in tests."""
    options: dict[str, Any] = {"handler": accept, "receive": QUICK_POLL}
    options.update(overrides)
    return consumer.Consumer(queue_url, **options)


def build_batch(
    queue_url: str, batch_handler: Any, **overrides: Any
) -> consumer.BatchConsumer:
    """A batch consumer with a 1s long poll, like build()."""
    options: dict[str, Any] = {"receive": QUICK_POLL}
    options.update(overrides)
    return consumer.BatchConsumer(queue_url, batch_handler, **options)


def send(client: sqs_client.SQSClient, queue_url: str, *bodies: str) -> None:
    for body in bodies:
        client.send_message(QueueUrl=queue_url, MessageBody=body)
