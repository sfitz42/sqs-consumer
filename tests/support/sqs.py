"""Helpers for driving a real SQS client against Floci."""

import asyncio
import time
import uuid
from collections.abc import Callable
from typing import Any

from mypy_boto3_sqs import client as sqs_client


def create_queue(
    client: sqs_client.SQSClient, *, fifo: bool = False, **attrs: str
) -> str:
    """Create a uniquely named queue so tests never share state."""
    name = f"test-{uuid.uuid4().hex[:12]}"
    attributes: dict[str, str] = dict(attrs)
    if fifo:
        name += ".fifo"
        attributes["FifoQueue"] = "true"
        attributes.setdefault("ContentBasedDeduplication", "true")
    return client.create_queue(QueueName=name, Attributes=attributes)["QueueUrl"]  # type: ignore[arg-type]


def depth(client: sqs_client.SQSClient, queue_url: str) -> tuple[int, int]:
    """(visible, in_flight) message counts."""
    attributes = client.get_queue_attributes(
        QueueUrl=queue_url,
        AttributeNames=[
            "ApproximateNumberOfMessages",
            "ApproximateNumberOfMessagesNotVisible",
        ],
    )["Attributes"]
    return (
        int(attributes["ApproximateNumberOfMessages"]),
        int(attributes["ApproximateNumberOfMessagesNotVisible"]),
    )


def record_operations(client: sqs_client.SQSClient) -> list[str]:
    """Names of every SQS operation the client calls, in order."""
    operations: list[str] = []

    def record(model: Any, **_: object) -> None:
        operations.append(model.name)

    client.meta.events.register("before-call.sqs", record)
    return operations


async def wait_for(predicate: Callable[[], object], *, timeout: float = 10.0) -> None:
    deadline = time.monotonic() + timeout
    while not predicate():
        if time.monotonic() > deadline:
            raise AssertionError(f"condition not met within {timeout}s")
        await asyncio.sleep(0.05)


async def receive_eventually(
    client: sqs_client.SQSClient, queue_url: str, *, timeout: float = 5.0
) -> dict[str, Any]:
    """The next message a separate reader can receive, with all its attributes."""
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        response = await asyncio.to_thread(
            client.receive_message,
            QueueUrl=queue_url,
            WaitTimeSeconds=1,
            AttributeNames=["All"],
        )
        if response.get("Messages"):
            return dict(response["Messages"][0])
    raise AssertionError(f"no message became visible within {timeout}s")
